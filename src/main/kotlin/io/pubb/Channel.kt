package io.pubb

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.serialization.json.JsonElement

data class ChannelEvent(val name: String, val data: JsonElement)

class Channel internal constructor(val name: String) {
    private val mutableEvents = MutableSharedFlow<ChannelEvent>(extraBufferCapacity = 256, onBufferOverflow = BufferOverflow.DROP_OLDEST)
    private val mutableMembers = MutableStateFlow<Map<String, JsonElement>>(emptyMap())
    val events = mutableEvents.asSharedFlow()
    val members = mutableMembers.asStateFlow()
    internal fun emit(event: String, data: JsonElement) { mutableEvents.tryEmit(ChannelEvent(canonicalEvent(event), data)) }
    internal fun setMembers(value: Map<String, JsonElement>) { mutableMembers.value = value.toMap() }
    internal fun addMember(id: String, info: JsonElement) { mutableMembers.value = mutableMembers.value + (id to info) }
    internal fun removeMember(id: String) { mutableMembers.value = mutableMembers.value - id }
}

internal fun canonicalEvent(name: String): String {
    val events = setOf("subscription_succeeded", "subscription_error", "member_added", "member_removed", "connection_established", "error")
    val prefix = listOf("pusher_internal:", "pusher:").firstOrNull { name.startsWith(it) }
    if (prefix != null && name.removePrefix(prefix) in events) return "pubb:" + name.removePrefix(prefix)
    return if (name == "subscription_error") "pubb:subscription_error" else name
}
