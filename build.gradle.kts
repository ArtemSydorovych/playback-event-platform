plugins {
    java
    idea
}

allprojects {
    group = "com.playback.platform"
    version = "1.0.0-SNAPSHOT"

    repositories {
        mavenCentral()
        maven("https://packages.confluent.io/maven/")
    }
}

// Task to build all Flink jobs and copy to deploy folder
tasks.register<Copy>("buildFlinkJobs") {
    group = "flink"
    description = "Build all Flink job JARs and copy to docker/build/flink-jars"

    dependsOn(":flink-jobs:shadowJar")

    from("flink-jobs/build/libs") {
        include("*-all.jar")
    }
    into("docker/build/flink-jars")

    doLast {
        println("Flink JARs copied to: docker/build/flink-jars/")
        println("To deploy, run: ./scripts/deploy-flink-jobs.ps1 -StartJobs")
    }
}

subprojects {
    apply(plugin = "java")

    java {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    dependencies {
        // Logging
        implementation("org.slf4j:slf4j-api:2.0.9")
        implementation("ch.qos.logback:logback-classic:1.4.14")

        // Lombok
        compileOnly("org.projectlombok:lombok:1.18.30")
        annotationProcessor("org.projectlombok:lombok:1.18.30")

        // Testing
        testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    tasks.test {
        useJUnitPlatform()
    }
}
