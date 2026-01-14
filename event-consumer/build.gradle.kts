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
}

application {
    mainClass.set("com.netflix.playback.consumer.ConsoleConsumerApp")
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}
