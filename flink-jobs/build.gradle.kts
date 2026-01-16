plugins {
    java
    application
    id("com.gradleup.shadow") version "9.0.0-beta4"
}

val flinkVersion = "1.18.1"
val kafkaConnectorVersion = "3.1.0-1.18"

dependencies {
    // Project dependencies
    implementation(project(":common"))
    implementation(project(":event-consumer"))

    // Flink core (provided by cluster at runtime)
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    compileOnly("org.apache.flink:flink-runtime-web:$flinkVersion")

    // Flink connectors (bundled in fat JAR)
    implementation("org.apache.flink:flink-connector-kafka:$kafkaConnectorVersion")
    implementation("org.apache.flink:flink-avro-confluent-registry:$flinkVersion")
    implementation("org.apache.flink:flink-statebackend-rocksdb:$flinkVersion")

    // Avro
    implementation("org.apache.avro:avro:1.11.3")
    implementation("io.confluent:kafka-avro-serializer:7.5.3")

    // Cassandra driver
    implementation("com.datastax.oss:java-driver-core:4.17.0")
    implementation("com.datastax.oss:java-driver-query-builder:4.17.0")

    // For local testing with embedded Flink
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-clients:$flinkVersion")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
}

application {
    mainClass.set("com.artemsydorovych.playback.flink.PlaybackPipelineJob")
}

tasks.shadowJar {
    archiveBaseName.set("playback-flink-jobs")
    archiveClassifier.set("all")
    archiveVersion.set("")

    mergeServiceFiles()

    // Relocate to avoid conflicts with Flink's bundled dependencies
    relocate("com.google", "shaded.com.google")

    manifest {
        attributes(
            "Main-Class" to "com.artemsydorovych.playback.flink.PlaybackPipelineJob"
        )
    }
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
