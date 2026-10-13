package io.pubb

import java.io.Closeable
import java.io.IOException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.serialization.json.*
import okhttp3.*
import okhttp3.HttpUrl.Companion.toHttpUrl
import okio.ByteString

enum class ConnectionState { INITIALIZED, CONNECTING, CONNECTED, DISCONNECTED, FAILED }

/** Call [close] when the owner is destroyed. [disconnect] allows later reconnection. */
class Pubb internal constructor(
    key: String,
    wsHost: String,
    private val authorizer: ChannelAuthorizer?,
    private val http: OkHttpClient,
    private val ownsHttp: Boolean,
    private val factory: WebSocket.Factory,
    private val waitBeforeRetry: suspend (Long) -> Unit,
) : Closeable {
    constructor(
        key: String,
        wsHost: String = "wss://ws.pubb.io",
        authorizer: ChannelAuthorizer? = null,
        httpClient: OkHttpClient? = null,
    ) : this(key, wsHost, authorizer, httpClient ?: defaultHttp(), httpClient == null)

    private constructor(key: String, host: String, authorizer: ChannelAuthorizer?, http: OkHttpClient, owns: Boolean)
        : this(key, host, authorizer, http, owns, http, { delay(it) })

    private val request = socketRequest(key, wsHost)
    private val dispatcher = Executors.newSingleThreadExecutor { runnable -> Thread(runnable, "Pubb client").apply { isDaemon = true } }.asCoroutineDispatcher()
    private val scope = CoroutineScope(SupervisorJob() + dispatcher)
    private val mutableState = MutableStateFlow(ConnectionState.INITIALIZED)
    private val mutableSocketId = MutableStateFlow<String?>(null)
    private val mutableErrors = MutableSharedFlow<Throwable>(extraBufferCapacity = 64, onBufferOverflow = BufferOverflow.DROP_OLDEST)
    val state = mutableState.asStateFlow()
    val socketId = mutableSocketId.asStateFlow()
    val errors = mutableErrors.asSharedFlow()
    private val channels = mutableMapOf<String, Channel>()
    private val subscriptions = mutableMapOf<String, Job>()
    private var socket: WebSocket? = null
    private var generation = 0L
    private var stopped = true
    @Volatile private var closed = false
    private var attempts = 0
    private var reconnect: Job? = null

    suspend fun connect() = withContext(dispatcher) {
        check(!closed) { "Pubb client is closed" }
        if (socket != null) return@withContext
        reconnect?.cancel(); reconnect = null
        stopped = false; attempts = 0
        startConnection()
    }

    suspend fun disconnect() = withContext(dispatcher) { stop() }

    suspend fun subscribe(name: String): Channel = withContext(dispatcher) {
        check(!closed) { "Pubb client is closed" }
        require(name.isNotBlank()) { "Channel name is required" }
        channels[name] ?: Channel(name).also { channel ->
            channels[name] = channel
            if (state.value == ConnectionState.CONNECTED) subscribeChannel(channel, generation)
        }
    }

    suspend fun unsubscribe(name: String) = withContext(dispatcher) {
        val channel = channels.remove(name) ?: return@withContext
        subscriptions.remove(name)?.cancel()
        channel.setMembers(emptyMap())
        if (state.value == ConnectionState.CONNECTED) send(buildJsonObject { put("action", "unsubscribe"); put("channel", name) }, generation)
    }

    /** Nonblocking cleanup. The internal dispatcher completes socket cancellation before exiting. */
    @Synchronized override fun close() {
        if (closed) return
        closed = true
        scope.launch {
            stop()
            if (ownsHttp) { http.dispatcher.executorService.shutdown(); http.connectionPool.evictAll() }
            scope.cancel()
            dispatcher.close()
        }
    }

    private fun stop() {
        stopped = true
        reconnect?.cancel(); reconnect = null
        resetConnection()
        attempts = 0
        mutableState.value = ConnectionState.DISCONNECTED
    }

    private fun resetConnection() {
        generation++
        subscriptions.values.forEach { it.cancel() }; subscriptions.clear()
        socket?.cancel(); socket = null
        mutableSocketId.value = null
        channels.values.forEach { it.setMembers(emptyMap()) }
    }

    private fun startConnection() {
        if (stopped || closed || socket != null) return
        val connection = ++generation
        mutableState.value = ConnectionState.CONNECTING
        socket = factory.newWebSocket(request, object : WebSocketListener() {
            override fun onMessage(webSocket: WebSocket, text: String) { scope.launch { if (generation == connection) handle(text, connection) } }
            override fun onMessage(webSocket: WebSocket, bytes: ByteString) { onMessage(webSocket, bytes.utf8()) }
            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) { webSocket.close(code, reason) }
            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) { scope.launch { failed(connection) } }
            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) { scope.launch { failed(connection) } }
        })
    }

    private fun failed(connection: Long) {
        if (generation != connection || stopped || closed) return
        resetConnection()
        val id = generation
        mutableState.value = ConnectionState.DISCONNECTED
        mutableErrors.tryEmit(IOException("Pubb WebSocket connection failed"))
        if (attempts >= 5) { mutableState.value = ConnectionState.FAILED; return }
        val milliseconds = minOf(1_000L shl ++attempts, 30_000L)
        reconnect = scope.launch {
            waitBeforeRetry(milliseconds)
            if (isActive && !stopped && generation == id) { reconnect = null; startConnection() }
        }
    }

    private fun handle(raw: String, connection: Long) {
        try {
            val packet = Json.parseToJsonElement(raw).jsonObject
            val event = packet["event"]?.jsonPrimitive?.contentOrNull?.let(::canonicalEvent)
            val rawData = packet["data"] ?: JsonNull
            val data = if (rawData is JsonPrimitive && rawData.isString) runCatching { Json.parseToJsonElement(rawData.content) }.getOrDefault(rawData) else rawData
            if (event == "pubb:connection_established") {
                val id = data.jsonObject["socket_id"]?.jsonPrimitive?.contentOrNull
                require(!id.isNullOrBlank())
                mutableSocketId.value = id; attempts = 0; mutableState.value = ConnectionState.CONNECTED
                channels.values.toList().forEach { subscribeChannel(it, connection) }
                return
            }
            val name = packet["channel"]?.jsonPrimitive?.contentOrNull
            val channel = channels[name]
            if (channel == null) { if (event == "pubb:error") mutableErrors.tryEmit(IOException("Pubb server error")); return }
            when (event) {
                "pubb:subscription_succeeded" -> {
                    if (channel.name.startsWith("presence-")) channel.setMembers(data.jsonObject["presence"]?.jsonObject?.get("hash")?.jsonObject ?: emptyMap())
                    channel.emit(event, data)
                }
                "pubb:subscription_error" -> channel.emit(event, data)
                "pubb:member_added", "pubb:member_removed" -> {
                    if (!channel.name.startsWith("presence-")) return
                    val id = data.jsonObject.getValue("user_id").jsonPrimitive.content
                    if (event == "pubb:member_added") channel.addMember(id, data.jsonObject["user_info"] ?: buildJsonObject { }) else channel.removeMember(id)
                    channel.emit(event, data)
                }
                null -> channel.emit(data.jsonObject.getValue("name").jsonPrimitive.content, data.jsonObject["data"] ?: JsonNull)
            }
        } catch (_: Exception) { mutableErrors.tryEmit(IOException("Invalid Pubb WebSocket message")) }
    }

    private fun subscribeChannel(channel: Channel, connection: Long) {
        val id = socketId.value ?: return
        if (generation != connection || channels[channel.name] !== channel || subscriptions.containsKey(channel.name)) return
        subscriptions[channel.name] = scope.launch(start = CoroutineStart.LAZY) {
            try {
                val authorization = if (channel.name.startsWith("private-") || channel.name.startsWith("presence-")) {
                    val provider = authorizer ?: throw IOException("An authorizer is required for this channel")
                    provider.authorize(id, channel.name).also { require(it.auth.isNotBlank()) { "Invalid channel authorization" } }
                } else null
                if (!isActive || generation != connection || channels[channel.name] !== channel) return@launch
                send(buildJsonObject {
                    put("action", "subscribe"); put("channel", channel.name)
                    authorization?.let { put("auth", it.auth); it.channelData?.let { data -> put("channel_data", data) } }
                }, connection)
            } catch (error: CancellationException) { throw error }
            catch (error: Exception) {
                if (generation == connection && channels[channel.name] === channel) {
                    subscriptions.remove(channel.name)
                    channel.emit("pubb:subscription_error", JsonPrimitive("Channel authorization or subscription failed"))
                    mutableErrors.tryEmit(error)
                }
            }
        }.also { it.start() }
    }

    private fun send(data: JsonObject, connection: Long) {
        if (generation == connection && socket?.send(data.toString()) == false) failed(connection)
    }

    companion object {
        private fun defaultHttp() = OkHttpClient.Builder().pingInterval(20, TimeUnit.SECONDS).build()
        internal fun socketRequest(key: String, host: String): Request {
            require(key.isNotBlank()) { "Public app key is required" }
            require(host.startsWith("wss://") || host.startsWith("ws://")) { "wsHost must use ws:// or wss://" }
            val httpHost = host.replaceFirst("wss://", "https://").replaceFirst("ws://", "http://").toHttpUrl()
            require(httpHost.username.isEmpty() && httpHost.password.isEmpty()) { "wsHost cannot contain credentials" }
            val url = httpHost.newBuilder().encodedPath("/socket").query(null).fragment(null).addQueryParameter("appKey", key).addQueryParameter("protocol", "2").build()
            return Request.Builder().url(url).build()
        }
    }
}
