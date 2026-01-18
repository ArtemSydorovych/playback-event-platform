# 1. Clone/copy project
cd D:\EventProcessingJava

# 2. Build the JAR
.\gradlew :flink-jobs:shadowJar

# 3. Build all Flink Docker images
docker build -t playback-raw-events:latest -f docker/flink/Dockerfile.raw-events .
docker build -t playback-content-metrics:latest -f docker/flink/Dockerfile.content-metrics .
docker build -t playback-session-detection:latest -f docker/flink/Dockerfile.session-detection .
docker build -t playback-lakehouse:latest -f docker/flink/Dockerfile.lakehouse .

# 4. Start everything
cd docker
docker-compose up -d

# 5. Wait ~90 seconds for all services to initialize
timeout 90

# 6. Check all containers are running
docker ps --format "table {{.Names}}\t{{.Status}}"

# 7. Generate test events
cd ..
.\gradlew :event-generator:run --args="--count 100 --rate 10"

Service URLs:
┌───────────────┬──────────────────────────────────────────────────┐
│    Service    │                       URL                        │
├───────────────┼──────────────────────────────────────────────────┤
│ Kafka UI      │ http://localhost:8080                            │
├───────────────┼──────────────────────────────────────────────────┤
│ Flink Jobs    │ http://localhost:8083, :8084, :8085, :8087       │
├───────────────┼──────────────────────────────────────────────────┤
│ Cassandra Web │ http://localhost:8086                            │
├───────────────┼──────────────────────────────────────────────────┤
│ Trino         │ http://localhost:8089                            │
├───────────────┼──────────────────────────────────────────────────┤
│ MinIO         │ http://localhost:9001 (minioadmin/minioadmin123) │
├───────────────┼──────────────────────────────────────────────────┤
│ Grafana       │ http://localhost:3000 (admin/admin)              │
└───────────────┴──────────────────────────────────────────────────┘
To stop everything:
cd docker
docker-compose down

To stop and delete all data:
cd docker
docker-compose down -v