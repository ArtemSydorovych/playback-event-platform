plugins {
    java
    application
}

dependencies {
    implementation(project(":common"))

    // Kafka client
    implementation("org.apache.kafka:kafka-clients:3.6.1")

    // Avro and Schema Registry
    implementation("org.apache.avro:avro:1.11.3")
    implementation("io.confluent:kafka-avro-serializer:7.5.3")

    // Cassandra DataStax driver
    implementation("com.datastax.oss:java-driver-core:4.17.0")
    implementation("com.datastax.oss:java-driver-query-builder:4.17.0")

    // Jackson for JSON serialization
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
}

application {
    mainClass.set("com.artemsydorovych.playback.consumer.ConsoleConsumerApp")
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}
