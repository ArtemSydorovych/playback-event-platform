# How to Run the Playback Event Platform

## Quick Start (Development)

```powershell
# 1. Start all infrastructure
docker-compose -f docker/docker-compose.yml up -d

# 2. Wait ~60 seconds for all services to be healthy
docker ps

# 3. Build and deploy Flink job
.\scripts\deploy-flink-jobs.ps1 -StartJobs
```

## Quick Start (Production / Production)

```powershell
# 1. Build the JAR
.\gradlew :flink-jobs:shadowJar

# 2. Build Docker images (from project root)
docker build -t playback-raw-events:latest -f docker/flink/Dockerfile.raw-events .
docker build -t playback-content-metrics:latest -f docker/flink/Dockerfile.content-metrics .
docker build -t playback-session-detection:latest -f docker/flink/Dockerfile.session-detection .

# 3. Start all services (3 independent jobs)
docker-compose -f docker/docker-compose.base.yml -f docker/flink/docker-compose.production.yml up -d
```

---

## Service URLs

| Service | URL | Description |
|---------|-----|-------------|
| **Flink UIs** | | |
| Raw Events Job | http://localhost:8083 | Raw events pipeline |
| Content Metrics Job | http://localhost:8084 | 5-min aggregations |
| Session Detection Job | http://localhost:8085 | User sessions |
| **Infrastructure** | | |
| Kafka UI | http://localhost:8080 | Topics, messages, consumers |
| Cassandra Web | http://localhost:8086 | Database browser |
| Schema Registry | http://localhost:8081 | Avro schemas |
| **Storage & HA** | | |
| MinIO Console | http://localhost:9001 | S3 checkpoints (admin/minioadmin123) |
| **Observability** | | |
| Prometheus | http://localhost:9090 | Metrics & alerts |
| Grafana | http://localhost:3000 | Dashboards (admin/admin) |

---

## Deployment Modes

### Mode 1: Development (Session Mode)
Single Flink cluster with jobs deployed via REST API.

```powershell
docker-compose -f docker/docker-compose.yml up -d
.\scripts\deploy-flink-jobs.ps1 -StartJobs
```

### Mode 2: Production (Application Mode)
Each job runs in its own container with dedicated resources.

```powershell
# Build JAR
.\gradlew :flink-jobs:shadowJar

# Build images
docker build -t playback-raw-events:latest -f docker/flink/Dockerfile.raw-events .
docker build -t playback-content-metrics:latest -f docker/flink/Dockerfile.content-metrics .
docker build -t playback-session-detection:latest -f docker/flink/Dockerfile.session-detection .

# Start
docker-compose -f docker/docker-compose.base.yml -f docker/flink/docker-compose.production.yml up -d
```

**Benefits:**
- Independent scaling per job
- Isolated failure domains
- Per-job resource allocation
- S3 checkpoints (survives restarts)
- ZooKeeper HA (automatic failover)

---

## Production Operations

### View All Job UIs
- Raw Events: http://localhost:8083
- Content Metrics: http://localhost:8084
- Session Detection: http://localhost:8085

### Create Savepoint
```powershell
.\scripts\create-savepoint.ps1 -JobName raw-events
.\scripts\create-savepoint.ps1 -JobName content-metrics
.\scripts\create-savepoint.ps1 -JobName session-detection
```

### Restore from Savepoint
```powershell
.\scripts\restore-from-savepoint.ps1 -JobName raw-events -SavepointPath "s3://flink-savepoints/raw-events/savepoint-xxx"
```

### Check Checkpoint Storage (MinIO)
```powershell
# Open MinIO Console
# URL: http://localhost:9001
# Login: minioadmin / minioadmin123
# Navigate to: flink-checkpoints bucket
```

### View Metrics in Grafana
1. Open http://localhost:3000
2. Login: admin / admin
3. Go to Dashboards -> Flink Jobs -> Flink Jobs Overview

### Restart Single Job
```powershell
# Stop job
docker-compose -f docker/docker-compose.base.yml -f docker/flink/docker-compose.production.yml stop raw-events-jobmanager raw-events-taskmanager

# Start job
docker-compose -f docker/docker-compose.base.yml -f docker/flink/docker-compose.production.yml up -d raw-events-jobmanager raw-events-taskmanager
```

---

## Generate Test Events

```powershell
.\gradlew :event-generator:run
```

---

## Verify Everything is Working

### Development Mode
```powershell
# Check all containers
docker ps

# Check Flink job
curl -s http://localhost:8083/jobs/overview

# Check Schema Registry
curl -s http://localhost:8081/subjects
```

### Production Mode
```powershell
# Check all 3 jobs running
curl -s http://localhost:8083/jobs/overview  # Raw Events
curl -s http://localhost:8084/jobs/overview  # Content Metrics
curl -s http://localhost:8085/jobs/overview  # Session Detection

# Check Prometheus targets
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[].labels.job'

# Check MinIO buckets
curl -s http://localhost:9000/minio/health/live
```

---

## Stop Everything

```powershell
# Development mode - stop (keeps data)
docker-compose -f docker/docker-compose.yml down

# Production mode - stop (keeps data)
docker-compose -f docker/docker-compose.base.yml -f docker/flink/docker-compose.production.yml down

# Remove all data (fresh start)
docker-compose -f docker/docker-compose.base.yml -f docker/flink/docker-compose.production.yml down -v
```

---

## Troubleshooting

### Job keeps restarting
```powershell
# Development mode
docker logs playback-flink-jobmanager -f

# Production mode (per job)
docker logs playback-raw-events-jm -f
docker logs playback-content-metrics-jm -f
docker logs playback-session-detection-jm -f
```

Common causes:
- Kafka not ready - wait for healthy status
- Schema Registry not running - check `docker ps`
- Wrong bootstrap server - should be `playback-kafka:29092`

### Schema Registry not starting
```powershell
docker logs playback-schema-registry
```
- Ensure Kafka is healthy first
- Check bootstrap server is `playback-kafka:29092`

### Cassandra connection issues
- Datacenter must be `datacenter1`
- Contact point is `playback-cassandra`
- Wait ~60s after startup for Cassandra to be ready

### Checkpoint failures (Production)
```powershell
# Check MinIO is running
curl -s http://localhost:9000/minio/health/live

# Check bucket exists
# Open http://localhost:9001 -> flink-checkpoints

# Check Flink logs for S3 errors
docker logs playback-raw-events-jm 2>&1 | grep -i s3
```

### Reset everything
```powershell
docker-compose -f docker/docker-compose.base.yml -f docker/flink/docker-compose.production.yml down -v
docker-compose -f docker/docker-compose.base.yml -f docker/flink/docker-compose.production.yml up -d
```

---

## Architecture

### Development Mode (Session)
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Event Generator │────▶│      Kafka      │────▶│   Flink Job     │
└─────────────────┘     │ playback-events │     │   (monolithic)  │
                        └─────────────────┘     └────────┬────────┘
                                                         │
                        ┌─────────────────┐              │
                        │ Schema Registry │◀─────────────┤
                        │   (Avro)        │              │
                        └─────────────────┘              ▼
                                                ┌─────────────────┐
                                                │    Cassandra    │
                                                └─────────────────┘
```

### Production Mode (Application Mode)
```
┌───────────────────────────────────────────────────────────────────────────┐
│                        Production Architecture                          │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                    Raw Events Job (Container)                       │  │
│  │  ┌─────────────┐   ┌────────────┐                                  │  │
│  │  │ JobManager  │───│TaskManager │──▶ Cassandra (6 tables)          │  │
│  │  └──────┬──────┘   └────────────┘                                  │  │
│  │         └──▶ S3: flink-checkpoints/raw-events/                     │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                 Content Metrics Job (Container)                     │  │
│  │  ┌─────────────┐   ┌────────────┐                                  │  │
│  │  │ JobManager  │───│TaskManager │──▶ Cassandra (content_metrics)   │  │
│  │  └──────┬──────┘   └────────────┘                                  │  │
│  │         └──▶ S3: flink-checkpoints/content-metrics/                │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                Session Detection Job (Container)                    │  │
│  │  ┌─────────────┐   ┌────────────┐                                  │  │
│  │  │ JobManager  │───│TaskManager │──▶ Cassandra (user_sessions)     │  │
│  │  └──────┬──────┘   └────────────┘                                  │  │
│  │         └──▶ S3: flink-checkpoints/session-detection/              │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌───────────┐  ┌───────────┐  ┌────────────┐  ┌─────────┐              │
│  │  MinIO    │  │ ZooKeeper │  │ Prometheus │  │ Grafana │              │
│  │ (S3 API)  │  │   (HA)    │  │ (Metrics)  │  │ (Dash)  │              │
│  └───────────┘  └───────────┘  └────────────┘  └─────────┘              │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## Internal Docker Network

All services communicate via `playback-network`:

| Service | Internal Hostname | Port |
|---------|-------------------|------|
| Kafka | `playback-kafka` | 29092 |
| Schema Registry | `playback-schema-registry` | 8081 |
| Cassandra | `playback-cassandra` | 9042 |
| MinIO | `minio` | 9000 |
| ZooKeeper | `zookeeper` | 2181 |
| Prometheus | `prometheus` | 9090 |
| Flink (Dev) | `jobmanager` | 6123 |
| Flink Raw Events | `raw-events-jobmanager` | 8081 |
| Flink Content Metrics | `content-metrics-jobmanager` | 8081 |
| Flink Session Detection | `session-detection-jobmanager` | 8081 |

---

## Adding a New Flink Job

1. Create job class extending `AbstractPlaybackJob`:
   ```java
   // flink-jobs/src/.../job/MyNewJob.java
   public class MyNewJob extends AbstractPlaybackJob { ... }
   ```

2. Create Dockerfile:
   ```dockerfile
   # docker/flink/Dockerfile.my-new-job
   FROM flink:1.18-java17
   COPY flink-s3-fs-hadoop-1.18.1.jar /opt/flink/plugins/s3-fs-hadoop/
   COPY playback-flink-jobs-all.jar /opt/flink/usrlib/
   CMD ["standalone-job", "--job-classname", "...MyNewJob"]
   ```

3. Add to `docker-compose.production.yml`:
   ```yaml
   my-new-job-jobmanager:
     build:
       context: ../..
       dockerfile: docker/flink/Dockerfile.my-new-job
     ports:
       - "8086:8081"
       - "9252:9249"
   ```

4. Add Prometheus scrape target in `prometheus.yml`

5. Rebuild and deploy
