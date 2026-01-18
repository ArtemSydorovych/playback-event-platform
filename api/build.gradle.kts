plugins {
    java
    application
}

val otelAgentVersion = "2.1.0"

// Configuration for downloading the OpenTelemetry agent
val otelAgent: Configuration by configurations.creating

dependencies {
    implementation(project(":common"))

    // Javalin - lightweight REST framework
    implementation("io.javalin:javalin:6.1.3")

    // Jackson for JSON serialization
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.1")

    // Cassandra DataStax driver
    implementation("com.datastax.oss:java-driver-core:4.17.0")
    implementation("com.datastax.oss:java-driver-query-builder:4.17.0")

    // SLF4J for logging
    implementation("org.slf4j:slf4j-simple:2.0.9")

    // OpenTelemetry Java Agent for automatic instrumentation
    otelAgent("io.opentelemetry.javaagent:opentelemetry-javaagent:$otelAgentVersion")
}

application {
    mainClass.set("com.artemsydorovych.playback.api.ApiServer")
}

// Copy the agent JAR to build directory
val copyAgent by tasks.registering(Copy::class) {
    from(otelAgent)
    into(layout.buildDirectory.dir("agent"))
    rename { "opentelemetry-javaagent.jar" }
}

tasks.named<JavaExec>("run") {
    dependsOn(copyAgent)

    // Tracing enabled by default - set OTEL_TRACING_DISABLED=true to turn off
    val tracingDisabled = System.getenv("OTEL_TRACING_DISABLED")?.toBoolean() ?: false

    if (!tracingDisabled) {
        val otelEndpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") ?: "http://localhost:4318"

        jvmArgs = listOf(
            "-javaagent:${layout.buildDirectory.get()}/agent/opentelemetry-javaagent.jar"
        )
        environment("OTEL_EXPORTER_OTLP_ENDPOINT", otelEndpoint)
        environment("OTEL_SERVICE_NAME", "playback-api")
        environment("OTEL_TRACES_EXPORTER", "otlp")
        environment("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
        environment("OTEL_METRICS_EXPORTER", "none")
        environment("OTEL_LOGS_EXPORTER", "none")
    }
}
