plugins {
    java
    application
}

dependencies {
    implementation(project(":common"))

    // Kafka client
    implementation("org.apache.kafka:kafka-clients:3.6.1")

    // Jackson for JSON
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.1")
}

application {
    mainClass.set("com.netflix.playback.generator.EventGeneratorApp")
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}
