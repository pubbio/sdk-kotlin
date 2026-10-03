import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.2.21"
    `java-library`
    `maven-publish`
}
group = "io.pubb"
version = "0.2.0"
java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withSourcesJar()
}
kotlin { compilerOptions { jvmTarget.set(JvmTarget.JVM_1_8) } }
dependencies {
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")
    api("com.squareup.okhttp3:okhttp:4.12.0")
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
tasks.test { useJUnitPlatform() }
tasks.withType<Jar>().configureEach { from("LICENSE") { into("META-INF") } }
val example = sourceSets.create("example")
configurations[example.implementationConfigurationName].extendsFrom(configurations.implementation.get())
example.compileClasspath += sourceSets.main.get().output
example.runtimeClasspath += sourceSets.main.get().output
kotlin.sourceSets["example"].kotlin.srcDir("examples")
tasks.register<JavaExec>("runExample") {
    dependsOn(example.classesTaskName)
    classpath = example.runtimeClasspath
    mainClass.set("ExampleKt")
}
publishing {
    publications {
        create<MavenPublication>("sdk") {
            from(components["java"])
            artifactId = "pubb-kotlin"
            pom {
                name.set("Pubb Kotlin SDK")
                description.set("Android/JVM client for Pubb realtime subscriptions")
                url.set("https://github.com/pubbio/sdk-kotlin")
                licenses { license { name.set("MIT"); url.set("https://opensource.org/license/mit") } }
                developers { developer { name.set("Pubb"); url.set("https://pubb.io") } }
                scm { url.set("https://github.com/pubbio/sdk-kotlin"); connection.set("scm:git:https://github.com/pubbio/sdk-kotlin.git") }
            }
        }
    }
    repositories { maven { name = "localBuild"; url = layout.buildDirectory.dir("repository").get().asFile.toURI() } }
}
