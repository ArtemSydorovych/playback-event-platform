plugins {
    java
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

dependencies {
    // Avro
    implementation("org.apache.avro:avro:1.11.3")

    // Confluent Schema Registry client and Avro serializers
    implementation("io.confluent:kafka-avro-serializer:7.5.3")

    // Cassandra DataStax driver
    implementation("com.datastax.oss:java-driver-core:4.17.0")
    implementation("com.datastax.oss:java-driver-query-builder:4.17.0")

    // Jackson for JSON serialization (used by table writers)
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
}

avro {
    setCreateSetters(true)
    setCreateOptionalGetters(false)
    setGettersReturnOptional(false)
    setOptionalGettersForNullableFieldsOnly(false)
    setFieldVisibility("PRIVATE")
    setStringType("String")
    setEnableDecimalLogicalType(true)
}

sourceSets {
    main {
        java {
            srcDir("${layout.buildDirectory.get()}/generated-main-avro-java")
        }
    }
}

tasks.named<com.github.davidmc24.gradle.plugin.avro.GenerateAvroJavaTask>("generateAvroJava") {
    setSource("${rootProject.projectDir}/schemas/avro")
}
