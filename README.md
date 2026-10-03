# Pubb Kotlin SDK

Android/JVM realtime client with public/private/presence subscriptions, coroutine
flows and bounded reconnection. Uses OkHttp, Kotlin coroutines and kotlinx JSON.
Targets JVM 8 bytecode; Android API 21+ with the `INTERNET` permission. Build with
JDK 17+ and the included Gradle wrapper. Source: [pubbio/sdk-kotlin](https://github.com/pubbio/sdk-kotlin).

## Install locally

This repository does not publish to Maven Central automatically. Build the local
Maven repository, then point your application's Gradle repositories at it:

```sh
./gradlew test compileExampleKotlin publishSdkPublicationToLocalBuildRepository
```

```kotlin
repositories {
    maven { url = uri("/absolute/path/to/sdk-kotlin/build/repository") }
    mavenCentral()
}
dependencies { implementation("io.pubb:pubb-kotlin:0.2.0") }
```

Do not assume that these coordinates exist on a public registry before a release
is verified. Public GitHub source and package registry publication are separate.

## Subscribe

```kotlin
import io.pubb.Pubb
import kotlinx.coroutines.launch

val pubb = Pubb("YOUR_PUBLIC_APP_KEY")
// Inside a lifecycle-owned coroutine scope:
val channel = pubb.subscribe("notifications")
val listener = launch {
    channel.events.collect { event ->
        if (event.name == "message.sent") println(event.data)
    }
}
val connectionListener = launch { pubb.state.collect { println(it) } }
val errors = launch { pubb.errors.collect { println(it.message) } }
pubb.connect()

// On a lifecycle pause, if appropriate:
pubb.disconnect()
// On final teardown, release network resources and cancel your collectors:
listener.cancel(); connectionListener.cancel(); errors.cancel()
pubb.close()
```

`connect`, `disconnect`, `subscribe` and `unsubscribe` are suspending functions.
All protocol state is serialized on an internal dispatcher. Flow collectors run
in the coroutine scope you choose, so use your UI scope when updating views.
Repeated `connect()` calls do not open duplicate sockets. `close()` permanently
ends the client; `disconnect()` retains channels and permits a later reconnect.

Unexpected disconnections retry after 2, 4, 8, 16 and 30 seconds. Successful
handshakes reset that budget and resubscribe, including fresh private channel
authorization. Explicit disconnect cancels pending retry/auth jobs and clears
presence. Refetch durable app state after reconnect: offline messages are not
replayed. Collect event flows before connecting; they do not replay past events.
Event buffers retain up to 256 pending events per channel, dropping the oldest
when a collector cannot keep up. Use durable backend state for recovery.

## Private and presence channels

```kotlin
val http = okhttp3.OkHttpClient()
val authorizer = io.pubb.HttpAuthorizer(
    "https://your-app.example/api/pubb/auth", http,
) { mapOf("Authorization" to "Bearer $currentUserSessionToken") }
val pubb = Pubb("YOUR_PUBLIC_APP_KEY", authorizer = authorizer, httpClient = http)
val room = pubb.subscribe("presence-room")
launch { room.members.collect { println("Online users: ${it.keys}") } }
launch { room.events.collect { println("${it.name}: ${it.data}") } }
pubb.connect()
```

The authorizer posts JSON `socket_id` and `channel_name` to your backend. The
backend must authenticate the user and authorize channel access before returning
`auth` and optional `channel_data`. Presence identity comes from the verified
server session. The headers function runs on each request and can use refreshed
user credentials. A custom suspending `ChannelAuthorizer` can reuse your existing
networking code. Authorization jobs are canceled on unsubscribe/disconnect, and
responses belonging to an old socket cannot subscribe a new one.

Native `pubb:*` events and legacy `pusher:*`/`pusher_internal:*` protocol names are
accepted. Events exposed by this SDK use `pubb:*` for known protocol events.
This does not imply full compatibility with unmodified Pusher clients/servers.

Never embed a Pubb application secret in an Android app. Publish from a trusted
server using a server SDK or Pubb's HTTP API. Public channels must not carry
sensitive information. An injected `OkHttpClient` remains owned by its caller;
close its dispatcher and connection pool when no application component needs it.

## Checks and example

```sh
./gradlew test compileExampleKotlin publishSdkPublicationToLocalBuildRepository
PUBB_APP_KEY=your-public-key ./gradlew runExample
```

The console example listens for one minute. Send `message.sent` to `notifications`
from your trusted backend. Override `PUBB_WS_HOST` for a local server;
`.env.example` is a template and is not loaded automatically.

Tests include a real local WebSocket exchange through MockWebServer, HTTP auth
requests/cancellation, stale authorization races, presence, reconnect bounds,
cleanup and malformed messages. They do not contact a live Pubb deployment or run
an Android emulator. CI runs the checks and produces Maven artifacts locally.

SDK code: [MIT](LICENSE). The generated Gradle wrapper retains its Apache 2.0
license and notices in [gradle/LICENSE](gradle/LICENSE) and [gradle/NOTICE](gradle/NOTICE).
