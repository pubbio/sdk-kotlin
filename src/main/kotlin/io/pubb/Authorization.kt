package io.pubb

import java.io.IOException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.serialization.json.*
import okhttp3.*
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody

data class ChannelAuthorization(val auth: String, val channelData: String? = null)
fun interface ChannelAuthorizer { suspend fun authorize(socketId: String, channelName: String): ChannelAuthorization }
class AuthorizationException(val status: Int) : IOException("Channel authorization failed (HTTP $status)")

/** Supply your existing authenticated backend endpoint and user session headers. */
class HttpAuthorizer(
    endpoint: String,
    private val client: OkHttpClient,
    private val headers: () -> Map<String, String> = { emptyMap() },
) : ChannelAuthorizer {
    private val endpoint = endpoint.toHttpUrl()
    override suspend fun authorize(socketId: String, channelName: String): ChannelAuthorization {
        val payload = buildJsonObject { put("socket_id", socketId); put("channel_name", channelName) }
        val request = Request.Builder().url(endpoint)
            .post(payload.toString().toRequestBody("application/json".toMediaType()))
            .apply { headers().forEach { (name, value) -> header(name, value) } }
            .build()
        val response = client.newCall(request).await()
        if (response.status !in 200..299) throw AuthorizationException(response.status)
        val data = Json.parseToJsonElement(response.body).jsonObject
            val auth = data["auth"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotBlank() }
                ?: throw IOException("Invalid channel authorization response")
        return ChannelAuthorization(auth, data["channel_data"]?.jsonPrimitive?.contentOrNull)
    }
}

private data class AuthHTTPResponse(val status: Int, val body: String)

private suspend fun Call.await(): AuthHTTPResponse = suspendCancellableCoroutine { continuation ->
    continuation.invokeOnCancellation { cancel() }
    enqueue(object : Callback {
        override fun onFailure(call: Call, e: IOException) { if (!continuation.isCancelled) continuation.resumeWithException(e) }
        override fun onResponse(call: Call, response: Response) {
            try {
                val payload = response.use { AuthHTTPResponse(it.code, if (it.isSuccessful) it.body?.string() ?: "" else "") }
                if (!continuation.isCancelled) continuation.resume(payload)
            } catch (error: IOException) { if (!continuation.isCancelled) continuation.resumeWithException(error) }
        }
    })
}
