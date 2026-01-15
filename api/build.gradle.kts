plugins {
    java
    application
}

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
}

application {
    mainClass.set("com.artemsydorovych.playback.api.ApiServer")
}
