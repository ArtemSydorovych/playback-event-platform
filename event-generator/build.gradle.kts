plugins {
    java
    application
}

val otelAgentVersion = "2.1.0"

// Configuration for downloading the OpenTelemetry agent
val otelAgent: Configuration by configurations.creating

dependencies {
    implementation(project(":common"))

    // Kafka client
    implementation("org.apache.kafka:kafka-clients:3.6.1")

    // Avro and Schema Registry
    implementation("org.apache.avro:avro:1.11.3")
    implementation("io.confluent:kafka-avro-serializer:7.5.3")

    // OpenTelemetry Java Agent for automatic instrumentation
    otelAgent("io.opentelemetry.javaagent:opentelemetry-javaagent:$otelAgentVersion")
}

application {
    mainClass.set("com.artemsydorovych.playback.generator.EventGeneratorApp")
}

// Copy the agent JAR to build directory
val copyAgent by tasks.registering(Copy::class) {
    from(otelAgent)
    into(layout.buildDirectory.dir("agent"))
    rename { "opentelemetry-javaagent.jar" }
}

tasks.named<JavaExec>("run") {
    dependsOn(copyAgent)
    standardInput = System.`in`

    // Tracing enabled by default - set OTEL_TRACING_DISABLED=true to turn off
    val tracingDisabled = System.getenv("OTEL_TRACING_DISABLED")?.toBoolean() ?: false

    if (!tracingDisabled) {
        val otelEndpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") ?: "http://localhost:4318"

        jvmArgs = listOf(
            "-javaagent:${layout.buildDirectory.get()}/agent/opentelemetry-javaagent.jar"
        )
        environment("OTEL_EXPORTER_OTLP_ENDPOINT", otelEndpoint)
        environment("OTEL_SERVICE_NAME", "playback-event-generator")
        environment("OTEL_TRACES_EXPORTER", "otlp")
        environment("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
        environment("OTEL_METRICS_EXPORTER", "none")
        environment("OTEL_LOGS_EXPORTER", "none")
    }
}
