# How to Run the Playback Event Platform

## Quick Start

```powershell
# 1. Start all infrastructure (Kafka, Cassandra, Flink, etc.)
docker-compose -f docker/docker-compose.yml up -d

# 2. Wait for services to be healthy (~60 seconds for Cassandra)
docker-compose -f docker/docker-compose.yml ps

# 3. Build and deploy Flink job
.\scripts\deploy-flink-jobs.ps1 -StartJobs
```

## Service URLs

| Service           | URL                    | Description              |
|-------------------|------------------------|--------------------------|
| Flink UI          | http://localhost:8083  | Job management           |
| Kafka UI          | http://localhost:8080  | Topics & messages        |
| Cassandra Web     | http://localhost:8084  | Database browser         |
| Schema Registry   | http://localhost:8081  | Avro schemas             |

---

## Detailed Steps

### 1. Start Infrastructure

```powershell
# Start everything
docker-compose -f docker/docker-compose.yml up -d

# Check status
docker-compose -f docker/docker-compose.yml ps

# View logs (optional)
docker-compose -f docker/docker-compose.yml logs -f
```

### 2. Build Flink Jobs

```powershell
# Option A: Just build
.\gradlew buildFlinkJobs

# Option B: Build shadow JAR directly
.\gradlew :flink-jobs:shadowJar
```

### 3. Deploy Flink Jobs

#### Option A: PowerShell Script (Recommended)
```powershell
# Build, upload, and start
.\scripts\deploy-flink-jobs.ps1 -StartJobs

# Just build and upload (manual start via UI)
.\scripts\deploy-flink-jobs.ps1

# Only build (no upload)
.\scripts\deploy-flink-jobs.ps1 -BuildOnly
```

#### Option B: Flink Web UI
1. Open http://localhost:8083
2. Click **"Submit New Job"**
3. Click **"+ Add New"**
4. Upload `flink-jobs/build/libs/playback-flink-jobs-all.jar`
5. Click **"Submit"**

#### Option C: REST API
```powershell
# Upload JAR
$response = Invoke-RestMethod -Uri "http://localhost:8083/jars/upload" `
    -Method Post `
    -Form @{ jarfile = Get-Item "flink-jobs/build/libs/playback-flink-jobs-all.jar" }

# Start job (use filename from response)
Invoke-RestMethod -Uri "http://localhost:8083/jars/$($response.filename.Split('/')[-1])/run" `
    -Method Post `
    -ContentType "application/json" `
    -Body '{"entryClass": "com.artemsydorovych.playback.flink.PlaybackPipelineJob"}'
```

#### Option D: Flink CLI (inside container)
```powershell
docker exec playback-flink-jobmanager flink run `
    -c com.artemsydorovych.playback.flink.PlaybackPipelineJob `
    /opt/flink/usrlib/playback-flink-jobs-all.jar
```

---

## Generate Test Events

```powershell
# Run the event generator
.\gradlew :event-generator:run
```

---

## Troubleshooting

### Job keeps restarting
Check Flink logs:
```powershell
docker logs playback-flink-jobmanager -f
```

Common issues:
- Kafka not ready: Wait for `playback-kafka` to be healthy
- Cassandra not ready: Wait ~60s after startup
- Wrong bootstrap server: Should be `playback-kafka:29092` (internal Docker)

### Can't connect to services
```powershell
# Check all containers are running
docker ps

# Check network connectivity
docker network inspect playback-platform_playback-network
```

### Reset everything
```powershell
# Stop and remove all containers + volumes
docker-compose -f docker/docker-compose.yml down -v

# Start fresh
docker-compose -f docker/docker-compose.yml up -d
```

---

## Development Workflow

```powershell
# 1. Make code changes to flink-jobs/

# 2. Rebuild and redeploy
.\scripts\deploy-flink-jobs.ps1 -StartJobs

# 3. Cancel old job in Flink UI if needed (http://localhost:8083)
```
