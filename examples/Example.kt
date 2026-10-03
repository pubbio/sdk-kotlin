import io.pubb.Pubb
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect

fun main() = runBlocking {
    val key = System.getenv("PUBB_APP_KEY") ?: error("Set PUBB_APP_KEY to your public application key")
    val pubb = Pubb(key, System.getenv("PUBB_WS_HOST") ?: "wss://ws.pubb.io")
    try {
        val channel = pubb.subscribe("notifications")
        val messages = launch { channel.events.collect { println("${it.name}: ${it.data}") } }
        val states = launch { pubb.state.collect { println("Connection: $it") } }
        yield()
        pubb.connect()
        delay(60_000)
        pubb.disconnect()
        messages.cancelAndJoin(); states.cancelAndJoin()
    } finally { pubb.close() }
}
