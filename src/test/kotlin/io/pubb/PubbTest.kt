package io.pubb

import java.io.IOException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.test.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel as Queue
import kotlinx.coroutines.flow.first
import kotlinx.serialization.json.*
import okhttp3.*
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import okio.ByteString
import org.junit.jupiter.api.Test

class PubbTest {
    private class Socket(private val request: Request, val listener: WebSocketListener) : WebSocket {
        val sent = CopyOnWriteArrayList<JsonObject>()
        @Volatile var canceled = false
        override fun request() = request
        override fun queueSize() = 0L
        override fun send(text: String): Boolean { sent += Json.parseToJsonElement(text).jsonObject; return !canceled }
        override fun send(bytes: ByteString) = send(bytes.utf8())
        override fun close(code: Int, reason: String?): Boolean { canceled = true; return true }
        override fun cancel() { canceled = true }
        fun receive(text: String) { listener.onMessage(this, text) }
        fun handshake(id: String = "1.2") { receive("""{"event":"pubb:connection_established","data":{"socket_id":"$id"}}""") }
        fun fail() { listener.onFailure(this, IOException("test disconnect"), null) }
    }
    private class Factory : WebSocket.Factory {
        val sockets = CopyOnWriteArrayList<Socket>()
        override fun newWebSocket(request: Request, listener: WebSocketListener) = Socket(request, listener).also { sockets += it }
    }
    private class Fixture(authorizer: ChannelAuthorizer? = null, wait: suspend (Long) -> Unit = { delay(it) }) : AutoCloseable {
        val factory = Factory()
        private val http = OkHttpClient()
        val pubb = Pubb("key&? /ç", "ws://localhost:38001", authorizer, http, true, factory, wait)
        override fun close() { pubb.close() }
    }
    private suspend fun until(condition: () -> Boolean) = withTimeout(3000) { while (!condition()) delay(1) }
    private fun dispose(http: OkHttpClient) { http.dispatcher.executorService.shutdown(); http.connectionPool.evictAll() }

    @Test fun `public subscriptions use encoded keys and deliver only matching messages`() = runBlocking {
        Fixture().use { fixture ->
            val pubb = fixture.pubb
            val channel = pubb.subscribe("notifications")
            val message = async(start = CoroutineStart.UNDISPATCHED) { channel.events.first { it.name == "message.sent" } }
            pubb.connect(); pubb.connect()
            assertEquals(1, fixture.factory.sockets.size)
            val socket = fixture.factory.sockets.single()
            assertEquals("key&? /ç", socket.request().url.queryParameter("appKey"))
            assertEquals("/socket", socket.request().url.encodedPath)
            socket.handshake(); until { socket.sent.size == 1 }
            assertEquals(JsonPrimitive("subscribe"), socket.sent[0]["action"])
            socket.receive("""{"channel":"other","data":{"name":"message.sent","data":"ignore"}}""")
            socket.receive("""{"channel":"notifications","data":"{\"name\":\"message.sent\",\"data\":{\"message\":\"Hello\"}}"}""")
            assertEquals(JsonPrimitive("Hello"), withTimeout(3000) { message.await() }.data.jsonObject["message"])
            pubb.disconnect()
            assertNull(pubb.socketId.value)
            assertTrue(socket.canceled)
        }
    }

    @Test fun `presence accepts legacy subscription and native member events`() = runBlocking {
        Fixture(ChannelAuthorizer { _, _ -> ChannelAuthorization("key:signature", "{\"user_id\":\"a\"}") }).use { fixture ->
            val room = fixture.pubb.subscribe("presence-room")
            fixture.pubb.connect()
            val socket = fixture.factory.sockets.single()
            socket.handshake(); until { socket.sent.isNotEmpty() }
            assertEquals(JsonPrimitive("key:signature"), socket.sent[0]["auth"])
            socket.receive("""{"event":"pusher_internal:subscription_succeeded","channel":"presence-room","data":{"presence":{"hash":{"a":{"name":"Ada"}},"ids":["a"],"count":1}}}""")
            until { room.members.value.size == 1 }
            socket.receive("""{"event":"pubb:member_added","channel":"presence-room","data":"{\"user_id\":\"b\",\"user_info\":{}}"}""")
            until { room.members.value.size == 2 }
            socket.receive("""{"event":"pusher_internal:member_removed","channel":"presence-room","data":{"user_id":"a"}}""")
            until { room.members.value.keys == setOf("b") }
            fixture.pubb.disconnect()
            assertTrue(room.members.value.isEmpty())
        }
    }

    @Test fun `missing authorizer raises subscription error instead of subscribing`() = runBlocking {
        Fixture().use { fixture ->
            val channel = fixture.pubb.subscribe("private-room")
            val error = async(start = CoroutineStart.UNDISPATCHED) { channel.events.first { it.name == "pubb:subscription_error" } }
            fixture.pubb.connect(); fixture.factory.sockets.single().handshake()
            withTimeout(3000) { error.await() }
            assertTrue(fixture.factory.sockets.single().sent.isEmpty())
            fixture.pubb.disconnect()
        }
    }

    @Test fun `unsubscribe discards delayed authorization`() = runBlocking {
        val started = CompletableDeferred<Unit>()
        val auth = CompletableDeferred<ChannelAuthorization>()
        Fixture(ChannelAuthorizer { socket, name ->
            assertEquals("1.2", socket); assertEquals("private-room", name)
            started.complete(Unit)
            withContext(NonCancellable) { auth.await() }
        }).use { fixture ->
            fixture.pubb.subscribe("private-room"); fixture.pubb.connect()
            val socket = fixture.factory.sockets.single()
            socket.handshake(); withTimeout(3000) { started.await() }
            fixture.pubb.unsubscribe("private-room")
            auth.complete(ChannelAuthorization("stale"))
            delay(50)
            assertEquals(listOf("unsubscribe"), socket.sent.map { it.getValue("action").jsonPrimitive.content })
            fixture.pubb.disconnect()
        }
    }

    @Test fun `authorization from an old socket cannot subscribe a new connection`() = runBlocking {
        val pending = CopyOnWriteArrayList<CompletableDeferred<ChannelAuthorization>>()
        Fixture(ChannelAuthorizer { _, _ ->
            val response = CompletableDeferred<ChannelAuthorization>(); pending += response
            withContext(NonCancellable) { response.await() }
        }).use { fixture ->
            fixture.pubb.subscribe("private-room"); fixture.pubb.connect()
            fixture.factory.sockets[0].handshake(); until { pending.size == 1 }
            fixture.pubb.disconnect(); fixture.pubb.connect(); fixture.factory.sockets[1].handshake("3.4")
            until { pending.size == 2 }
            pending[0].complete(ChannelAuthorization("stale")); delay(30)
            assertTrue(fixture.factory.sockets[1].sent.isEmpty())
            pending[1].complete(ChannelAuthorization("fresh"))
            until { fixture.factory.sockets[1].sent.isNotEmpty() }
            assertEquals(JsonPrimitive("fresh"), fixture.factory.sockets[1].sent[0]["auth"])
            fixture.pubb.disconnect()
        }
    }

    @Test fun `reconnect resubscribes and disconnect cancels pending retries`() = runBlocking {
        val resume = Queue<Unit>(Queue.UNLIMITED)
        val delays = CopyOnWriteArrayList<Long>()
        Fixture(wait = { milliseconds -> delays += milliseconds; resume.receive() }).use { fixture ->
            fixture.pubb.subscribe("notifications"); fixture.pubb.connect()
            fixture.factory.sockets[0].handshake(); until { fixture.factory.sockets[0].sent.isNotEmpty() }
            fixture.factory.sockets[0].fail(); until { delays.size == 1 }
            assertEquals(2000L, delays[0]); resume.send(Unit)
            until { fixture.factory.sockets.size == 2 }
            fixture.factory.sockets[1].handshake("3.4"); until { fixture.factory.sockets[1].sent.isNotEmpty() }
            fixture.factory.sockets[1].fail(); until { delays.size == 2 }
            fixture.pubb.disconnect(); resume.send(Unit); delay(40)
            assertEquals(2, fixture.factory.sockets.size)
            assertEquals(ConnectionState.DISCONNECTED, fixture.pubb.state.value)
        }
    }

    @Test fun `five failed reconnects exhaust the retry budget`() = runBlocking {
        Fixture(wait = { yield() }).use { fixture ->
            fixture.pubb.connect()
            repeat(5) { index -> fixture.factory.sockets.last().fail(); until { fixture.factory.sockets.size == index + 2 } }
            fixture.factory.sockets.last().fail()
            until { fixture.pubb.state.value == ConnectionState.FAILED }
            assertEquals(6, fixture.factory.sockets.size)
            fixture.pubb.disconnect()
        }
    }

    @Test fun `malformed frames and server errors are surfaced`() = runBlocking {
        Fixture().use { fixture ->
            val error = async(start = CoroutineStart.UNDISPATCHED) { fixture.pubb.errors.first() }
            fixture.pubb.connect(); fixture.factory.sockets[0].receive("not json")
            assertEquals("Invalid Pubb WebSocket message", withTimeout(3000) { error.await() }.message)
            fixture.pubb.disconnect()
        }
    }

    @Test fun `real websocket exchanges Pubb frames`() = runBlocking {
        val server = MockWebServer()
        server.enqueue(MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) { webSocket.send("""{"event":"pubb:connection_established","data":"{\"socket_id\":\"real.1\"}"}""") }
            override fun onMessage(webSocket: WebSocket, text: String) {
                assertEquals("subscribe", Json.parseToJsonElement(text).jsonObject.getValue("action").jsonPrimitive.content)
                webSocket.send("""{"channel":"notifications","data":"{\"name\":\"message.sent\",\"data\":{\"message\":\"over websocket\"}}"}""")
            }
        }))
        server.start()
        val http = OkHttpClient()
        val pubb = Pubb("public-key", server.url("/").toString().replaceFirst("http://", "ws://"), httpClient = http)
        try {
            val channel = pubb.subscribe("notifications")
            val received = async(start = CoroutineStart.UNDISPATCHED) { channel.events.first { it.name == "message.sent" } }
            pubb.connect()
            assertEquals(JsonPrimitive("over websocket"), withTimeout(5000) { received.await() }.data.jsonObject["message"])
            assertEquals("/socket?appKey=public-key", server.takeRequest(3, TimeUnit.SECONDS)?.path)
            pubb.disconnect()
        } finally { pubb.close(); dispose(http); server.close() }
    }

    @Test fun `HTTP authorization sends expected identity and rejects denial`() = runBlocking {
        val server = MockWebServer()
        server.enqueue(MockResponse().setBody("""{"auth":"key:signature","channel_data":"{\"user_id\":\"a\"}"}"""))
        server.enqueue(MockResponse().setResponseCode(403).setBody("do not echo"))
        server.start()
        val http = OkHttpClient()
        try {
            val authorizer = HttpAuthorizer(server.url("/auth").toString(), http) { mapOf("Authorization" to "Bearer user-session") }
            val result = authorizer.authorize("1.2", "presence-room")
            assertEquals("key:signature", result.auth)
            val request = server.takeRequest(3, TimeUnit.SECONDS)!!
            assertEquals("POST", request.method)
            assertEquals("Bearer user-session", request.getHeader("Authorization"))
            assertEquals(buildJsonObject { put("socket_id", "1.2"); put("channel_name", "presence-room") }, Json.parseToJsonElement(request.body.readUtf8()))
            val error = assertFailsWith<AuthorizationException> { authorizer.authorize("1.2", "presence-room") }
            assertEquals(403, error.status); assertFalse(error.message!!.contains("do not echo"))
        } finally { dispose(http); server.close() }
    }

    @Test fun `HTTP authorization cancellation terminates an in-flight request`() = runBlocking {
        val server = MockWebServer()
        server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE)); server.start()
        val http = OkHttpClient()
        try {
            val authorizer = HttpAuthorizer(server.url("/auth").toString(), http)
            val job = launch { authorizer.authorize("1.2", "private-room") }
            withContext(Dispatchers.IO) { assertNotNull(server.takeRequest(3, TimeUnit.SECONDS)) }
            withTimeout(2000) { job.cancelAndJoin() }
            until { http.dispatcher.runningCallsCount() == 0 }
        } finally { dispose(http); server.close() }
    }

    @Test fun `invalid configuration fails before opening a socket`() {
        assertFailsWith<IllegalArgumentException> { Pubb.socketRequest("", "wss://ws.pubb.io") }
        assertFailsWith<IllegalArgumentException> { Pubb.socketRequest("key", "https://example.com") }
    }
}
