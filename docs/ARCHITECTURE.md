# Playback Event Platform - Architecture Guide

> **Version**: 1.0
> **Last Updated**: January 16, 2026

This document describes the architecture of the Playback Event Platform, how each component works and connects, and provides guidance for implementing future changes.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Flow](#2-data-flow)
3. [Component Deep Dive](#3-component-deep-dive)
4. [Flink Jobs Architecture](#4-flink-jobs-architecture)
5. [State Management](#5-state-management)
6. [Observability](#6-observability)
7. [Adding New Features](#7-adding-new-features)
8. [Deployment Modes](#8-deployment-modes)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              PLAYBACK EVENT PLATFORM                                 │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌─────────────┐      ┌─────────────────────────────────────────────────────────┐  │
│  │   Event     │      │                    KAFKA CLUSTER                         │  │
│  │  Generator  │─────▶│  Topic: playback-events (partitioned by userId)         │  │
│  │             │      │  Mode: KRaft (no ZooKeeper)                              │  │
│  └─────────────┘      └───────────────────────┬─────────────────────────────────┘  │
│                                               │                                     │
│                       ┌───────────────────────┼───────────────────────┐             │
│                       │                       │                       │             │
│                       ▼                       ▼                       ▼             │
│  ┌─────────────────────────┐ ┌─────────────────────────┐ ┌─────────────────────────┐│
│  │   RAW EVENTS JOB        │ │  CONTENT METRICS JOB    │ │ SESSION DETECTION JOB   ││
│  │                         │ │                         │ │                         ││
│  │ • Reads all events      │ │ • Reads all events      │ │ • Reads all events      ││
│  │ • Routes to 6 writers   │ │ • Keys by contentId     │ │ • Keys by userId        ││
│  │ • No windowing          │ │ • 5-min tumbling window │ │ • 30-min session gap    ││
│  │ • Stateless             │ │ • Stateful (RocksDB)    │ │ • Stateful (RocksDB)    ││
│  │                         │ │                         │ │                         ││
│  │ Consumer Group:         │ │ Consumer Group:         │ │ Consumer Group:         ││
│  │ flink-raw-events        │ │ flink-content-metrics   │ │ flink-session-detection ││
│  └───────────┬─────────────┘ └───────────┬─────────────┘ └───────────┬─────────────┘│
│              │                           │                           │              │
│              ▼                           ▼                           ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐│
│  │                              CASSANDRA CLUSTER                                   ││
│  │                                                                                  ││
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐              ││
│  │  │ events_by_user   │  │ content_metrics  │  │ user_sessions    │              ││
│  │  │ events_by_session│  │ _5min            │  │                  │              ││
│  │  │ events_by_content│  │                  │  │                  │              ││
│  │  │ user_progress    │  │                  │  │                  │              ││
│  │  │ continue_watching│  │                  │  │                  │              ││
│  │  │ quality_events   │  │                  │  │                  │              ││
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘              ││
│  └─────────────────────────────────────────────────────────────────────────────────┘│
│                                                                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │
│  │   MinIO     │  │  ZooKeeper  │  │ Prometheus  │  │   Grafana   │               │
│  │ Checkpoints │  │  Flink HA   │  │   Metrics   │  │ Dashboards  │               │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘               │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Job Independence** | Each Flink job runs in its own container pair (JobManager + TaskManager) |
| **Exactly-Once** | Checkpointing with RocksDB state backend and Kafka offsets |
| **Fault Tolerance** | S3 checkpoints + ZooKeeper HA + exponential restart |
| **Observability** | Prometheus metrics + Grafana dashboards per job |
| **Scalability** | Each job can scale independently based on load |

---

## 2. Data Flow

### Event Lifecycle

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│ 1. EVENT GENERATION                                                               │
│                                                                                   │
│    EventGeneratorApp.java                                                         │
│    ├── PlaybackActivityGenerator.java  (creates realistic events)                │
│    └── PlaybackEventPublisher.java     (serializes with Avro, sends to Kafka)    │
│                                                                                   │
│    Event Types: PLAY_START, PAUSE, RESUME, SEEK, PROGRESS, QUALITY_CHANGE,       │
│                 REBUFFER_START, REBUFFER_END, PLAYBACK_END                        │
└────────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ 2. KAFKA INGESTION                                                                │
│                                                                                   │
│    Topic: playback-events                                                         │
│    ├── Partitions: 6 (partitioned by userId for ordering)                        │
│    ├── Replication: 1 (dev) / 3 (prod)                                           │
│    └── Retention: 7 days                                                          │
│                                                                                   │
│    Schema Registry:                                                               │
│    ├── Compatibility: BACKWARD                                                    │
│    └── Format: Avro                                                               │
└────────────────────────────────────────────────────────────────────────────────────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    │                    │                    │
                    ▼                    ▼                    ▼
┌───────────────────────┐  ┌───────────────────────┐  ┌───────────────────────┐
│ 3A. RAW EVENTS JOB    │  │ 3B. CONTENT METRICS   │  │ 3C. SESSION DETECTION │
│                       │  │                       │  │                       │
│ KafkaSource           │  │ KafkaSource           │  │ KafkaSource           │
│     │                 │  │     │                 │  │     │                 │
│     ▼                 │  │     ▼                 │  │     ▼                 │
│ WatermarkStrategy     │  │ WatermarkStrategy     │  │ WatermarkStrategy     │
│ (30s bounded)         │  │ (30s bounded)         │  │ (30s bounded)         │
│     │                 │  │     │                 │  │     │                 │
│     ▼                 │  │     ▼                 │  │     ▼                 │
│ CassandraSinkFunction │  │ keyBy(contentId)      │  │ keyBy(userId)         │
│ (routes to 6 writers) │  │     │                 │  │     │                 │
│                       │  │     ▼                 │  │     ▼                 │
│                       │  │ TumblingWindow(5min)  │  │ SessionDetector       │
│                       │  │     │                 │  │ (KeyedProcessFunction)│
│                       │  │     ▼                 │  │ (30min gap timer)     │
│                       │  │ Aggregate+Process     │  │     │                 │
│                       │  │     │                 │  │     ▼                 │
│                       │  │     ▼                 │  │ SessionSinkFunction   │
│                       │  │ MetricsSinkFunction   │  │                       │
└───────────────────────┘  └───────────────────────┘  └───────────────────────┘
          │                          │                          │
          ▼                          ▼                          ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│ 4. CASSANDRA STORAGE                                                              │
│                                                                                   │
│    Raw Events Tables (written by RawEventsJob):                                   │
│    ├── events_by_user      (partition: user_id)      - User timeline            │
│    ├── events_by_session   (partition: session_id)   - Session replay           │
│    ├── events_by_content   (partition: content_id)   - Content analytics        │
│    ├── user_content_progress (partition: user_id)    - Resume playback          │
│    ├── continue_watching   (partition: user_id)      - "Continue Watching"      │
│    └── playback_quality    (partition: content_id)   - QoS events               │
│                                                                                   │
│    Aggregation Tables:                                                            │
│    ├── content_metrics_5min (partition: content_id)  - 5-min windows            │
│    └── user_sessions       (partition: user_id)      - Session analytics        │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Deep Dive

### 3.1 Event Generator

**Location**: `event-generator/src/main/java/com/artemsydorovych/playback/generator/`

| Class | Purpose |
|-------|---------|
| `EventGeneratorApp.java` | CLI entry point with multiple modes (regular, infinite, stress) |
| `PlaybackActivityGenerator.java` | Creates realistic playback event sequences |
| `PlaybackEventPublisher.java` | Serializes events with Avro and publishes to Kafka |

**Key Features**:
- Simulates realistic user behavior (binge-watching, casual viewing)
- Generates correlated sessions (events follow logical sequence)
- Supports stress testing with multi-threaded publishing

### 3.2 Kafka

**Location**: `docker/kafka/docker-compose.yml`

**Configuration**:
```yaml
Mode: KRaft (no separate ZooKeeper)
Broker: playback-kafka:29092 (internal) / localhost:9092 (external)
Topic: playback-events
  - Partitions: 6
  - Replication Factor: 1
  - Cleanup Policy: delete
  - Retention: 7 days
```

**Why KRaft?**
- Simplified deployment (no ZooKeeper dependency)
- Faster controller operations
- Better resource utilization

### 3.3 Schema Registry

**Location**: `docker/schema-registry/docker-compose.yml`

**Purpose**: Manages Avro schema versions and enforces compatibility.

**Schema Location**: `schemas/avro/com/artemsydorovych/playback/`
```
PlaybackEvent.avsc      - Main event schema (union of payloads)
DeviceInfo.avsc         - Device metadata
enums/
  EventType.avsc        - PLAY_START, PAUSE, etc.
  DeviceType.avsc       - IOS, ANDROID, WEB, etc.
payloads/
  SeekPayload.avsc
  QualityChangePayload.avsc
  RebufferPayload.avsc
  PlaybackEndPayload.avsc
```

### 3.4 Cassandra

**Location**: `docker/cassandra/docker-compose.yml` + `init/schema.cql`

**Table Design Patterns**:

| Pattern | Tables | Query Example |
|---------|--------|---------------|
| **Time-series by entity** | events_by_user, events_by_session, events_by_content | "Get all events for user X in last hour" |
| **Latest state** | user_content_progress, continue_watching | "Where did user X stop watching content Y?" |
| **Aggregations** | content_metrics_5min, user_sessions | "Get content metrics for last 24 hours" |

### 3.5 MinIO (S3-Compatible Storage)

**Location**: `docker/minio/docker-compose.yml`

**Buckets**:
| Bucket | Purpose |
|--------|---------|
| `flink-checkpoints` | Checkpoint data for recovery |
| `flink-savepoints` | Manual savepoints for upgrades |
| `flink-ha` | High Availability metadata |

**Credentials**: minioadmin / minioadmin123

### 3.6 ZooKeeper (Flink HA)

**Location**: `docker/zookeeper/docker-compose.yml`

**Purpose**: Coordinates JobManager leadership election for High Availability.

**Note**: This ZooKeeper is ONLY for Flink HA. Kafka uses KRaft mode.

---

## 4. Flink Jobs Architecture

### 4.1 Job Hierarchy

```
AbstractPlaybackJob (abstract base class)
├── RawEventsJob        - Writes raw events to Cassandra
├── ContentMetricsJob   - Aggregates content metrics (5-min windows)
└── SessionDetectionJob - Detects user sessions (30-min gap)
```

### 4.2 AbstractPlaybackJob

**Location**: `flink-jobs/src/main/java/.../flink/job/AbstractPlaybackJob.java`

Provides common functionality:
- Execution environment setup
- Checkpointing configuration
- Restart strategy (exponential backoff)
- Kafka source creation
- Parameter parsing

```java
// Key configuration methods
protected StreamExecutionEnvironment createEnvironment() {
    env.enableCheckpointing(60000);  // 60 seconds
    env.setStateBackend(new EmbeddedRocksDBStateBackend());
    env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(...));
    return env;
}

protected KafkaSource<PlaybackEvent> createKafkaSource(String consumerGroup) {
    // Creates Kafka source with Avro deserialization
}
```

### 4.3 RawEventsJob

**Purpose**: Routes all events to appropriate Cassandra tables.

**Flow**:
```
Kafka → Watermarks → CassandraSinkFunction → EventRouter → 6 TableWriters
```

**Key Classes**:
| Class | Purpose |
|-------|---------|
| `CassandraSinkFunction` | Wraps EventRouter for Flink compatibility |
| `EventRouter` | Routes events to appropriate writers based on type |
| `EventsByUserWriter` | Writes to events_by_user table |
| `EventsBySessionWriter` | Writes to events_by_session table |
| etc. | One writer per table |

**Cassandra Tables Written**:
- events_by_user
- events_by_session
- events_by_content
- user_content_progress
- continue_watching
- playback_quality_events

### 4.4 ContentMetricsJob

**Purpose**: Aggregates playback metrics per content in 5-minute windows.

**Flow**:
```
Kafka → Watermarks → keyBy(contentId) → TumblingWindow(5min) → Aggregate → MetricsSinkFunction
```

**Key Classes**:
| Class | Purpose |
|-------|---------|
| `ContentMetricsAggregator` | Aggregate function + process window function |
| `ContentMetrics` | POJO holding aggregated metrics |
| `MetricsSinkFunction` | Writes to content_metrics_5min table |

**Metrics Calculated**:
- View count
- Unique viewers
- Total watch time
- Completion rate
- Device breakdown (mobile/desktop/TV)
- Rebuffer count and duration
- Pause/seek/quality change counts

### 4.5 SessionDetectionJob

**Purpose**: Detects user sessions based on 30-minute inactivity gaps.

**Flow**:
```
Kafka → Watermarks → keyBy(userId) → SessionDetector (KeyedProcessFunction) → SessionSinkFunction
```

**Key Classes**:
| Class | Purpose |
|-------|---------|
| `SessionDetector` | KeyedProcessFunction with event-time timers |
| `UserSession` | POJO holding session data |
| `SessionSinkFunction` | Writes to user_sessions table |

**Session Logic**:
1. First event for a user starts a new session
2. Each event updates session state and resets the gap timer
3. When 30 minutes pass without events, timer fires and emits session
4. Session includes: duration, event count, content watched, watch time, etc.

---

## 5. State Management

### 5.1 State Backends

| Job | State Backend | State Size |
|-----|---------------|------------|
| RawEventsJob | None (stateless) | 0 |
| ContentMetricsJob | RocksDB | ~1GB (window state) |
| SessionDetectionJob | RocksDB | ~5GB (user sessions) |

### 5.2 Checkpointing

```
Mode: EXACTLY_ONCE
Interval: 60 seconds
Storage: s3://flink-checkpoints/{job-name}/
Retention: RETAIN_ON_CANCELLATION
```

**Checkpoint Flow**:
1. JobManager triggers checkpoint
2. TaskManagers snapshot their state
3. State uploaded to MinIO (S3)
4. Kafka offsets committed
5. Checkpoint marked complete

### 5.3 State TTL

| Job | State | TTL |
|-----|-------|-----|
| ContentMetricsJob | Window state | Cleared after window fires |
| SessionDetectionJob | User session | 24 hours (configurable) |

---

## 6. Observability

### 6.1 Prometheus Metrics

**Custom Metrics** (defined in `PlaybackMetrics.java`):

```java
// Raw Events Job
playback_raw_events_processed_total     // Counter
playback_raw_playbacks_started_total    // Counter
playback_raw_playbacks_ended_total      // Counter

// Session Detection Job
playback_sessions_detected_total        // Counter
playback_sessions_active_sessions       // Gauge
playback_sessions_session_duration_seconds // Histogram
playback_sessions_events_per_session    // Histogram

// Content Metrics Job
playback_content_metrics_windows_processed_total // Counter
playback_content_metrics_views_per_window       // Histogram

// Cassandra Sinks
playback_cassandra_writes_total         // Counter (labeled by table)
playback_cassandra_write_errors_total   // Counter
playback_cassandra_write_latency_ms     // Histogram
```

### 6.2 Prometheus Configuration

**Location**: `docker/monitoring/prometheus/prometheus.yml`

```yaml
scrape_configs:
  - job_name: 'flink-raw-events'
    static_configs:
      - targets: ['raw-events-jobmanager:9249', 'raw-events-taskmanager:9249']
  - job_name: 'flink-content-metrics'
    static_configs:
      - targets: ['content-metrics-jobmanager:9249', 'content-metrics-taskmanager:9249']
  - job_name: 'flink-session-detection'
    static_configs:
      - targets: ['session-detection-jobmanager:9249', 'session-detection-taskmanager:9249']
```

### 6.3 Grafana Dashboards

| Dashboard | Panels | Purpose |
|-----------|--------|---------|
| Flink Jobs Overview | 6 | Checkpoint success, duration, records in/out |
| Playback Business Metrics | 17 | Playback starts, sessions, QoS, Cassandra writes |

**Datasource UID**: `PBFA97CFB590B2093`

### 6.4 Alerting Rules

**Location**: `docker/monitoring/prometheus/alerting-rules.yml`

| Alert | Condition | Severity |
|-------|-----------|----------|
| FlinkJobDown | Job not running for 2 min | critical |
| CheckpointFailed | Checkpoint failures > 0 | warning |
| HighKafkaLag | Lag > 10000 events | warning |
| CassandraWriteErrors | Error rate > 1% | critical |

---

## 7. Adding New Features

### 7.1 Adding a New Flink Job

**Step 1: Create Job Class**

```java
// flink-jobs/src/main/java/.../flink/job/MyNewJob.java
public class MyNewJob extends AbstractPlaybackJob {

    public static void main(String[] args) throws Exception {
        new MyNewJob().run(args);
    }

    @Override
    protected void buildPipeline(
            StreamExecutionEnvironment env,
            DataStream<PlaybackEvent> events,
            JobParameters params) {

        // Your pipeline logic here
        events
            .keyBy(event -> event.getUserId().toString())
            .process(new MyProcessFunction())
            .addSink(new MySinkFunction());
    }

    @Override
    protected String getJobName() {
        return "my-new-job";
    }

    @Override
    protected String getConsumerGroup() {
        return "flink-my-new-job";
    }
}
```

**Step 2: Create Dockerfile**

```dockerfile
# docker/flink/Dockerfile.my-new-job
FROM flink:1.18-java17

# Add S3 filesystem plugin
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop
COPY --from=builder /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

# Copy the fat JAR
COPY flink-jobs/build/libs/playback-flink-jobs-all.jar /opt/flink/usrlib/

# Set environment
ENV FLINK_PROPERTIES="jobmanager.rpc.address: my-new-job-jobmanager"
```

**Step 3: Add to docker-compose.production.yml**

```yaml
my-new-job-jobmanager:
  build:
    context: ../..
    dockerfile: docker/flink/Dockerfile.my-new-job
  image: playback-my-new-job:latest
  container_name: playback-my-new-job-jm
  hostname: my-new-job-jobmanager
  command: standalone-job --job-classname com.artemsydorovych.playback.flink.job.MyNewJob
  ports:
    - "8086:8081"   # Web UI (next available)
    - "9252:9249"   # Prometheus metrics
  environment:
    FLINK_PROPERTIES: |
      jobmanager.rpc.address: my-new-job-jobmanager
      high-availability.cluster-id: my-new-job
      state.checkpoints.dir: s3://flink-checkpoints/my-new-job
      # ... rest of config
  networks:
    - playback-network

my-new-job-taskmanager:
  image: playback-my-new-job:latest
  container_name: playback-my-new-job-tm
  command: taskmanager
  environment:
    FLINK_PROPERTIES: |
      jobmanager.rpc.address: my-new-job-jobmanager
      # ... rest of config
  networks:
    - playback-network
```

**Step 4: Add Prometheus Scrape Target**

```yaml
# docker/monitoring/prometheus/prometheus.yml
- job_name: 'flink-my-new-job'
  static_configs:
    - targets: ['my-new-job-jobmanager:9249', 'my-new-job-taskmanager:9249']
```

**Step 5: Build and Deploy**

```powershell
# Build JAR
.\gradlew :flink-jobs:shadowJar

# Build Docker image
docker-compose -f docker/flink/docker-compose.production.yml build my-new-job-jobmanager

# Start
docker-compose -f docker/docker-compose.yml -f docker/flink/docker-compose.production.yml up -d
```

### 7.2 Adding a New Cassandra Table

**Step 1: Add to schema.cql**

```cql
-- docker/cassandra/init/schema.cql
CREATE TABLE IF NOT EXISTS my_new_table (
    partition_key text,
    clustering_key timestamp,
    data text,
    PRIMARY KEY (partition_key, clustering_key)
) WITH CLUSTERING ORDER BY (clustering_key DESC);
```

**Step 2: Create TableWriter**

```java
// event-consumer/src/main/java/.../cassandra/writer/MyNewTableWriter.java
public class MyNewTableWriter extends AbstractTableWriter {

    private static final String INSERT_CQL = """
        INSERT INTO my_new_table (partition_key, clustering_key, data)
        VALUES (?, ?, ?)
        """;

    @Override
    protected String getInsertCql() {
        return INSERT_CQL;
    }

    @Override
    protected Object[] extractValues(PlaybackEvent event) {
        return new Object[] {
            event.getUserId().toString(),
            Instant.now(),
            event.toString()
        };
    }

    @Override
    public boolean accepts(PlaybackEvent event) {
        return true;  // or filter logic
    }
}
```

**Step 3: Register in EventRouter**

```java
// event-consumer/src/main/java/.../cassandra/EventRouter.java
private void initWriters() {
    writers.add(new EventsByUserWriter());
    writers.add(new EventsBySessionWriter());
    // ... existing writers ...
    writers.add(new MyNewTableWriter());  // Add here
}
```

### 7.3 Adding Custom Metrics

**Step 1: Add to PlaybackMetrics.java**

```java
// flink-jobs/src/main/java/.../flink/metrics/PlaybackMetrics.java

public static class MyNewMetrics {
    private final Counter myCounter;
    private final Gauge<Long> myGauge;
    private final AtomicLong gaugeValue = new AtomicLong(0);

    public MyNewMetrics(MetricGroup metricGroup) {
        MetricGroup group = metricGroup
            .addGroup("playback")
            .addGroup("my_feature");

        this.myCounter = group.counter("events_total");
        this.myGauge = group.gauge("active_count", gaugeValue::get);
    }

    public void recordEvent() {
        myCounter.inc();
    }

    public void setActiveCount(long count) {
        gaugeValue.set(count);
    }
}

public static MyNewMetrics createMyNewMetrics(MetricGroup group) {
    return new MyNewMetrics(group);
}
```

**Step 2: Use in Flink Operator**

```java
public class MyProcessFunction extends KeyedProcessFunction<String, PlaybackEvent, Result> {

    private transient PlaybackMetrics.MyNewMetrics metrics;

    @Override
    public void open(Configuration parameters) {
        metrics = PlaybackMetrics.createMyNewMetrics(getRuntimeContext().getMetricGroup());
    }

    @Override
    public void processElement(PlaybackEvent event, Context ctx, Collector<Result> out) {
        metrics.recordEvent();
        // ... processing logic
    }
}
```

### 7.4 Adding a New Event Type

**Step 1: Update Avro Schema**

```json
// schemas/avro/com/artemsydorovych/playback/enums/EventType.avsc
{
  "type": "enum",
  "name": "EventType",
  "symbols": [
    "PLAY_START", "PAUSE", "RESUME", "SEEK", "PROGRESS",
    "QUALITY_CHANGE", "REBUFFER_START", "REBUFFER_END", "PLAYBACK_END",
    "MY_NEW_EVENT"  // Add here
  ]
}
```

**Step 2: Create Payload (if needed)**

```json
// schemas/avro/com/artemsydorovych/playback/payloads/MyNewEventPayload.avsc
{
  "type": "record",
  "name": "MyNewEventPayload",
  "namespace": "com.artemsydorovych.playback.avro",
  "fields": [
    {"name": "customField", "type": "string"}
  ]
}
```

**Step 3: Update PlaybackEvent.avsc**

```json
// Add to the "payload" union
"payload": [
  "null",
  "SeekPayload",
  "QualityChangePayload",
  "RebufferPayload",
  "PlaybackEndPayload",
  "MyNewEventPayload"  // Add here
]
```

**Step 4: Regenerate Avro Classes**

```powershell
.\gradlew :schemas:generateAvro
```

**Step 5: Handle in Processors**

Update relevant switch statements in:
- `ContentMetricsAggregator.java`
- `SessionDetector.java`
- Table writers

---

## 8. Deployment Modes

### 8.1 Development Mode (Session Mode)

Single Flink cluster running all jobs.

```powershell
# Start infrastructure + session mode Flink
docker-compose -f docker/docker-compose.yml up -d

# Submit jobs manually
docker exec playback-flink-jobmanager flink run /opt/flink/usrlib/playback-flink-jobs-all.jar \
  --class com.artemsydorovych.playback.flink.job.RawEventsJob
```

**Pros**: Simple, quick iteration
**Cons**: Jobs share resources, single point of failure

### 8.2 Production Mode (Application Mode)

Each job in its own container.

```powershell
# Build images
docker-compose -f docker/flink/docker-compose.production.yml build

# Start everything
docker-compose -f docker/docker-compose.yml -f docker/flink/docker-compose.production.yml up -d
```

**Pros**: Job isolation, independent scaling, true HA
**Cons**: More containers, higher resource usage

### 8.3 Port Mapping

| Mode | Raw Events | Content Metrics | Session Detection |
|------|------------|-----------------|-------------------|
| Session | 8082 (shared) | 8082 (shared) | 8082 (shared) |
| Application | 8083 | 8084 | 8085 |

---

## 9. Troubleshooting

### 9.1 Common Issues

**Issue: TaskManager can't connect to JobManager**

```
Cause: Hostname resolution in Docker
Fix: Ensure hostnames match in FLINK_PROPERTIES
     jobmanager.rpc.address must match container hostname
```

**Issue: Checkpoint failures**

```
Cause: S3/MinIO connectivity
Fix:
1. Check MinIO is running: curl http://localhost:9001
2. Verify S3 settings in FLINK_PROPERTIES
3. Check credentials: s3.access-key, s3.secret-key
```

**Issue: Kafka consumer lag increasing**

```
Cause: Processing slower than ingestion
Fix:
1. Increase parallelism: FLINK_PARALLELISM env var
2. Add TaskManager slots
3. Check for slow sinks (Cassandra latency)
```

**Issue: Grafana shows "No data"**

```
Cause: Wrong datasource UID
Fix: Ensure dashboard JSON uses UID: PBFA97CFB590B2093
```

### 9.2 Useful Commands

```powershell
# Check Flink job status
docker exec playback-raw-events-jm flink list

# View job logs
docker logs -f playback-raw-events-jm

# Check Kafka consumer groups
docker exec playback-kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups

# Query Cassandra
docker exec -it playback-cassandra cqlsh -e "SELECT * FROM playback.events_by_user LIMIT 5;"

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets[].labels.job'

# Check MinIO buckets
docker exec playback-minio mc ls local/flink-checkpoints/
```

### 9.3 Recovery Procedures

**Recover from Checkpoint**:
```powershell
# Jobs automatically recover from latest checkpoint on restart
docker restart playback-raw-events-jm
```

**Recover from Savepoint**:
```powershell
# Create savepoint before upgrade
docker exec playback-raw-events-jm flink savepoint <job-id> s3://flink-savepoints/raw-events/

# Start from savepoint (modify docker-compose command)
command: standalone-job --job-classname ...RawEventsJob -s s3://flink-savepoints/raw-events/<savepoint-dir>
```

---

## Appendix: File Reference

| File | Purpose |
|------|---------|
| `docker/docker-compose.yml` | Master orchestrator |
| `docker/flink/docker-compose.yml` | Production Application Mode |
| `flink-jobs/build.gradle.kts` | Shadow JAR configuration |
| `flink-jobs/.../job/AbstractPlaybackJob.java` | Base class for all jobs |
| `flink-jobs/.../metrics/PlaybackMetrics.java` | Custom Prometheus metrics |
| `docker/monitoring/prometheus/prometheus.yml` | Scrape configuration |
| `docker/monitoring/grafana/provisioning/dashboards/` | Dashboard JSONs |
| `docker/cassandra/init/schema.cql` | All table definitions |
