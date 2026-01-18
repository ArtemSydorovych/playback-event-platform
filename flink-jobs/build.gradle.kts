plugins {
    java
    application
    id("com.gradleup.shadow") version "9.0.0-beta4"
}

val flinkVersion = "1.18.1"
val kafkaConnectorVersion = "3.1.0-1.18"
val icebergVersion = "1.5.0"
val hadoopVersion = "3.3.4"

/**
 * Production Job Classes (Application Mode - 1 job per container):
 * - RawEventsJob:        com.artemsydorovych.playback.flink.job.RawEventsJob
 * - ContentMetricsJob:   com.artemsydorovych.playback.flink.job.ContentMetricsJob
 * - SessionDetectionJob: com.artemsydorovych.playback.flink.job.SessionDetectionJob
 */

dependencies {
    // Project dependencies
    implementation(project(":common"))

    // Flink core (provided by cluster at runtime)
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    compileOnly("org.apache.flink:flink-runtime-web:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-common:$flinkVersion")

    // Flink connectors (bundled in fat JAR)
    implementation("org.apache.flink:flink-connector-kafka:$kafkaConnectorVersion")
    implementation("org.apache.flink:flink-avro-confluent-registry:$flinkVersion")
    implementation("org.apache.flink:flink-statebackend-rocksdb:$flinkVersion")

    // Prometheus metrics (for observability)
    implementation("org.apache.flink:flink-metrics-prometheus:$flinkVersion")

    // Iceberg connector for Data Lakehouse (Phase 9)
    implementation("org.apache.iceberg:iceberg-flink-runtime-1.18:$icebergVersion")
    implementation("org.apache.iceberg:iceberg-aws:$icebergVersion")
    implementation("software.amazon.awssdk:s3:2.20.131")
    implementation("software.amazon.awssdk:sts:2.20.131")
    implementation("software.amazon.awssdk:kms:2.20.131")
    implementation("software.amazon.awssdk:dynamodb:2.20.131")
    implementation("software.amazon.awssdk:glue:2.20.131")
    implementation("software.amazon.awssdk:url-connection-client:2.20.131")

    // Hadoop for S3 filesystem (Iceberg uses Hadoop FileSystem)
    implementation("org.apache.hadoop:hadoop-common:$hadoopVersion") {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
    }
    implementation("org.apache.hadoop:hadoop-aws:$hadoopVersion") {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
    }

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
    mainClass.set("com.artemsydorovych.playback.flink.job.RawEventsJob")
}

tasks.shadowJar {
    archiveBaseName.set("playback-flink-jobs")
    archiveClassifier.set("all")
    archiveVersion.set("")

    // Enable zip64 for large JARs (Iceberg + Hadoop dependencies)
    isZip64 = true

    mergeServiceFiles()

    // Relocate to avoid conflicts with Flink's bundled dependencies
    relocate("com.google", "shaded.com.google")

    manifest {
        attributes(
            "Main-Class" to "com.artemsydorovych.playback.flink.job.RawEventsJob"
        )
    }
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
