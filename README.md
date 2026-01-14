# 🎬 Netflix-Style Playback Event Platform

A production-grade data platform for processing, analyzing, and serving video playback events at scale. This system captures every interaction users have with video content — play, pause, seek, quality changes, buffering — and transforms this raw stream into actionable insights and real-time features.

---

## 📋 Table of Contents

- [What This Platform Does](#-what-this-platform-does)
- [Core Capabilities](#-core-capabilities)
- [System Architecture](#-system-architecture)
- [Data Flow](#-data-flow)
- [Platform Components](#-platform-components)
- [Storage Strategy](#-storage-strategy)
- [Real-Time Features](#-real-time-features)
- [Analytics Capabilities](#-analytics-capabilities)
- [Operational Features](#-operational-features)
- [Web Interfaces](#-web-interfaces)
- [Deployment Profiles](#-deployment-profiles)

---

## 🎯 What This Platform Does

This platform solves the fundamental challenge of video streaming services: **understanding and responding to how users watch content in real-time while maintaining a complete historical record for analytics**.

### The Problem

When millions of users watch video content simultaneously, every interaction generates events:
- A user presses play
- The stream buffers due to network conditions
- Quality automatically adjusts from 4K to 1080p
- The user seeks forward to skip an intro
- The user pauses to answer a phone call
- The app crashes mid-playback

Each of these events contains valuable information. The challenge is capturing all of them, processing them in real-time for immediate features, and storing them reliably for long-term analysis.

### The Solution

This platform provides a complete infrastructure that:

1. **Ingests** millions of events per minute from diverse devices
2. **Processes** events in real-time to power user-facing features
3. **Stores** data in optimized formats for different access patterns
4. **Serves** low-latency APIs for instant user experiences
5. **Analyzes** historical data for business intelligence and machine learning
6. **Monitors** system health and content quality continuously

---

## 🚀 Core Capabilities

### For End Users (via Applications)

| Capability | Description |
|------------|-------------|
| **Resume Playback** | Users can stop watching on one device and continue exactly where they left off on any other device |
| **Continue Watching** | Homepage displays recently watched content with accurate progress indicators |
| **Cross-Device Sync** | Playback state synchronizes across phones, tablets, TVs, and web browsers within seconds |
| **Smart Resume Position** | System intelligently backs up a few seconds when resuming so users don't miss context |
| **Completion Detection** | Automatically marks content as "watched" when users reach the credits |

### For Content Operations

| Capability | Description |
|------------|-------------|
| **Real-Time QoS Monitoring** | Live dashboards showing streaming quality across all users |
| **Content Health Scores** | Per-title metrics identifying problematic content or encodings |
| **Regional Performance** | Geographic breakdown of streaming quality by country and region |
| **Device Analytics** | Performance comparison across platforms (iOS vs Android vs Smart TVs) |
| **Anomaly Detection** | Automatic alerts when quality metrics deviate from normal |

### For Data & Analytics Teams

| Capability | Description |
|------------|-------------|
| **Complete Event History** | Every playback event stored with full context for years |
| **Ad-Hoc Querying** | Interactive SQL access to billions of events |
| **Session Reconstruction** | Ability to replay exactly what a user experienced |
| **ML Feature Generation** | Automated pipelines producing features for recommendation models |
| **A/B Test Analysis** | Compare viewing behavior across experiment variants |

### For Platform Operations

| Capability | Description |
|------------|-------------|
| **Schema Evolution** | Add new event fields without breaking existing consumers |
| **Exactly-Once Processing** | Guaranteed accurate counts even during failures |
| **Automatic Recovery** | Self-healing from component failures without data loss |
| **Horizontal Scalability** | Handle 10x traffic by adding more nodes |
| **Multi-Datacenter Ready** | Architecture supports geographic distribution |

---

## 🏗 System Architecture

The platform follows a **Lambda Architecture** pattern, maintaining two parallel data paths optimized for different access patterns:

```
                                    ┌─────────────────────────────────┐
                                    │         EVENT SOURCES           │
                                    │   iOS • Android • Web • TV      │
                                    │     100K+ concurrent devices    │
                                    └───────────────┬─────────────────┘
                                                    │
                                                    ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION LAYER                                       │
│                                                                                    │
│   • Accepts events via REST API                                                   │
│   • Validates against schema registry                                             │
│   • Handles 100K+ requests per second                                             │
│   • Provides backpressure under load                                              │
│   • Routes invalid events to dead letter queue                                    │
│                                                                                    │
└───────────────────────────────────────────────────────────────────────────────────┘
                                                    │
                                                    ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│                              STREAMING LAYER                                       │
│                                                                                    │
│   • Durable message storage with 7-day retention                                  │
│   • Ordered delivery per user session                                             │
│   • Parallel consumption by multiple processors                                   │
│   • Replay capability for reprocessing                                            │
│                                                                                    │
└───────────────────────────────────────────────────────────────────────────────────┘
                                                    │
                                                    ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│                           STREAM PROCESSING LAYER                                  │
│                                                                                    │
│   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│   │     Event       │  │    Session      │  │    Position     │  │     QoS     │ │
│   │   Enrichment    │  │    Stitching    │  │    Tracking     │  │  Calculation│ │
│   │                 │  │                 │  │                 │  │             │ │
│   │ Adds content &  │  │ Groups events   │  │ Tracks where    │  │ Calculates  │ │
│   │ device metadata │  │ into viewing    │  │ each user       │  │ rebuffer    │ │
│   │ to raw events   │  │ sessions        │  │ stopped watching│  │ ratios      │ │
│   └─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────┘ │
│                                                                                    │
│   • Stateful processing with checkpointing                                        │
│   • Exactly-once delivery guarantees                                              │
│   • Automatic failure recovery                                                    │
│                                                                                    │
└───────────────────────────────────────────────────────────────────────────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    │                                       │
                    ▼                                       ▼
┌─────────────────────────────────────┐ ┌─────────────────────────────────────────┐
│         HOT PATH (Real-Time)        │ │           COLD PATH (Analytics)          │
│                                     │ │                                          │
│  ┌───────────────────────────────┐  │ │  ┌────────────────────────────────────┐ │
│  │       Low-Latency Store       │  │ │  │         Data Lakehouse             │ │
│  │                               │  │ │  │                                    │ │
│  │  • Resume positions          │  │ │  │  • Complete event history          │ │
│  │  • Continue watching lists   │  │ │  │  • Session aggregates              │ │
│  │  • Active session state      │  │ │  │  • Historical QoS metrics          │ │
│  │  • Real-time QoS metrics     │  │ │  │                                    │ │
│  │                               │  │ │  │  • Schema evolution supported     │ │
│  │  Response time: < 10ms       │  │ │  │  • Time travel queries             │ │
│  └───────────────────────────────┘  │ │  │  • Partition pruning              │ │
│                                     │ │  └────────────────────────────────────┘ │
│              │                      │ │                    │                     │
│              ▼                      │ │                    ▼                     │
│  ┌───────────────────────────────┐  │ │  ┌────────────────────────────────────┐ │
│  │        Serving APIs           │  │ │  │          Query Engines             │ │
│  │                               │  │ │  │                                    │ │
│  │  • GET /resume/{contentId}   │  │ │  │  • Interactive SQL queries         │ │
│  │  • GET /continue-watching    │  │ │  │  • Batch aggregation jobs          │ │
│  │  • GET /qos/realtime         │  │ │  │  • ML feature pipelines            │ │
│  └───────────────────────────────┘  │ │  └────────────────────────────────────┘ │
└─────────────────────────────────────┘ └─────────────────────────────────────────┘
```

---

## 🔄 Data Flow

### Event Lifecycle

Every playback event travels through the following stages:

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  STAGE 1: GENERATION                                                              │
│                                                                                   │
│  User presses play on their TV                                                   │
│       │                                                                          │
│       ▼                                                                          │
│  Player app creates PlaybackEvent with:                                          │
│  • Event type (PLAY_START)                                                       │
│  • Device identifier                                                             │
│  • User profile                                                                  │
│  • Content being watched                                                         │
│  • Current position (0:00)                                                       │
│  • Timestamp                                                                     │
└──────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  STAGE 2: INGESTION                                                               │
│                                                                                   │
│  Event arrives at API Gateway                                                    │
│       │                                                                          │
│       ├──▶ Schema validation (is this a valid event?)                           │
│       ├──▶ Deduplication check (have we seen this before?)                      │
│       ├──▶ Rate limit check (is this device sending too fast?)                  │
│       │                                                                          │
│       ▼                                                                          │
│  Valid event written to streaming queue                                          │
│  Invalid events routed to dead letter queue for investigation                   │
└──────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  STAGE 3: ENRICHMENT                                                              │
│                                                                                   │
│  Stream processor reads raw event                                                │
│       │                                                                          │
│       ├──▶ Lookup content metadata (title, duration, genre)                     │
│       ├──▶ Lookup device information (model, OS version)                        │
│       ├──▶ Add processing timestamp                                              │
│       │                                                                          │
│       ▼                                                                          │
│  Enriched event ready for processing                                             │
└──────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  STAGE 4: PROCESSING                                                              │
│                                                                                   │
│  Multiple processors handle the event simultaneously:                            │
│                                                                                   │
│  Position Tracker                    Session Stitcher                            │
│  ─────────────────                   ────────────────                            │
│  • Updates "last known position"     • Adds event to current session            │
│  • Applies smart resume logic        • Detects session boundaries               │
│  • Determines completion status      • Calculates session metrics               │
│                                                                                   │
│  QoS Calculator                                                                   │
│  ──────────────                                                                   │
│  • Updates quality metrics                                                       │
│  • Tracks rebuffer events                                                        │
│  • Calculates health scores                                                      │
└──────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  STAGE 5: STORAGE                                                                 │
│                                                                                   │
│  Results written to appropriate stores:                                          │
│                                                                                   │
│  Hot Store (immediate access)        Cold Store (historical)                     │
│  ─────────────────────────────       ───────────────────────                     │
│  • Position: profile + content       • Raw event: permanent archive              │
│  • Continue watching: profile        • Session: aggregated record                │
│  • QoS metrics: time-bucketed        • QoS history: trend analysis              │
│                                                                                   │
│  TTL: 30-180 days                    Retention: 2+ years                         │
└──────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│  STAGE 6: SERVING                                                                 │
│                                                                                   │
│  Data available for consumption:                                                 │
│                                                                                   │
│  Real-Time APIs                      Analytics Queries                           │
│  ──────────────                      ─────────────────                           │
│  • Resume playback endpoint          • Ad-hoc SQL exploration                    │
│  • Continue watching list            • Scheduled aggregation jobs                │
│  • Live QoS dashboard                • ML feature extraction                     │
│                                                                                   │
│  Response time: < 10ms               Query time: seconds to minutes              │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

### Event Types Processed

The platform handles the complete lifecycle of playback interactions:

| Event Type | Trigger | Data Captured | Used For |
|------------|---------|---------------|----------|
| **PLAY_START** | User initiates playback | Content ID, starting position, device | Session start, resume tracking |
| **PAUSE** | User pauses video | Current position, timestamp | Engagement analysis, position save |
| **RESUME** | User resumes after pause | Position, pause duration | Session continuity |
| **SEEK** | User skips forward/backward | From position, to position | Content analysis, skip patterns |
| **PROGRESS** | Periodic heartbeat (10-30s) | Current position, buffer state | Position tracking, health monitoring |
| **QUALITY_CHANGE** | Bitrate adaptation | Old quality, new quality, reason | QoS metrics, network analysis |
| **REBUFFER_START** | Playback stalls | Position, buffer length | QoS alerts, performance issues |
| **REBUFFER_END** | Playback resumes | Rebuffer duration | QoS metrics calculation |
| **PLAYBACK_END** | User stops or content ends | Final position, completion % | Completion tracking, recommendations |

---

## 🧩 Platform Components

### Data Generation Layer

**What it does**: Simulates realistic playback behavior from a large device fleet for testing and demonstration.

**Capabilities**:
- Generates events matching real-world patterns (binge-watching, casual viewing, background play)
- Simulates diverse devices (iOS, Android, Web browsers, Smart TVs, gaming consoles)
- Models realistic network conditions (WiFi, cellular, varying bandwidth)
- Creates correlated sessions (users don't just send random events)
- Supports configurable load from 1K to 100K+ events per minute

**Event Characteristics**:
- Realistic timing between events
- Proper session continuity
- Varied content catalog
- Geographic distribution
- Device-appropriate behavior patterns

---

### Ingestion Layer

**What it does**: Accepts events from all sources, validates them, and reliably delivers to processing.

**Capabilities**:

| Feature | Benefit |
|---------|---------|
| High-throughput REST API | Handles 100K+ events per second |
| Schema validation | Rejects malformed events before processing |
| Forward-compatible schemas | Add new fields without breaking existing systems |
| Automatic versioning | Track schema changes over time |
| Rate limiting | Protects platform from misbehaving clients |
| Dead letter queue | Captures failed events for debugging |
| Health endpoints | Enables load balancer integration |
| Metrics emission | Provides visibility into ingestion health |

**Failure Handling**:
- Invalid schema → Dead letter queue with error details
- Rate exceeded → 429 response with retry guidance
- Backend unavailable → Backpressure to client with retry
- Duplicate event → Deduplicated, single processing guaranteed

---

### Streaming Layer

**What it does**: Provides durable, ordered, replayable event transport between components.

**Capabilities**:

| Feature | Benefit |
|---------|---------|
| Durable storage | Events persisted for 7 days, survive failures |
| Ordered delivery | Events from same user processed in sequence |
| Parallel consumption | Multiple processors read simultaneously |
| Consumer groups | Independent processing pipelines |
| Replay capability | Reprocess historical events for fixes or new features |
| Partition scaling | Add capacity without downtime |

**Topic Organization**:

| Topic | Purpose | Retention |
|-------|---------|-----------|
| Raw events | Unmodified events from ingestion | 7 days |
| Enriched events | Events with metadata added | 3 days |
| QoS metrics | Calculated quality metrics stream | 1 day |
| Dead letter | Failed events for investigation | 30 days |

---

### Stream Processing Layer

**What it does**: Transforms raw events into business value through stateful real-time processing.

**Processing Jobs**:

#### Event Enrichment
- Adds content metadata (title, duration, genre, credits timestamp)
- Adds device information (model, OS, app version, capabilities)
- Normalizes timestamps to UTC
- Validates business rules

#### Session Stitching
- Groups events into viewing sessions
- Detects session boundaries (30-minute inactivity gap)
- Calculates session-level metrics:
  - Total viewing duration
  - Number of pauses
  - Seek count and patterns
  - Quality stability
  - Rebuffer incidents

#### Position Tracking
- Maintains current position per user per content
- Applies smart resume logic:
  - Ignores very short views (less than 2 minutes) as accidental
  - Marks content complete at 90% or credits
  - Backs up 10 seconds on resume for context
  - Resets to beginning if stopped very early
- Handles cross-device synchronization

#### QoS Calculation
- Calculates quality metrics in real-time:
  - Rebuffer ratio (rebuffer time divided by play time)
  - Time to first frame
  - Average bitrate
  - Quality stability score
- Aggregates by content, device type, region
- Feeds real-time dashboards

**Processing Guarantees**:
- Exactly-once semantics (no duplicate or lost events)
- Automatic checkpointing every 60 seconds
- Recovery from failures without data loss
- Consistent state across restarts

---

### Hot Storage Layer

**What it does**: Provides sub-10ms access to frequently needed data for user-facing features.

**Data Stored**:

| Data Type | Access Pattern | Typical Query |
|-----------|----------------|---------------|
| Resume position | By user + content | "Where did user X stop watching content Y?" |
| Continue watching | By user, sorted by recency | "What are user X's 20 most recent incomplete items?" |
| Active session | By user | "Is user X currently watching something?" |
| Real-time QoS | By time bucket | "What's the current rebuffer ratio across all users?" |

**Characteristics**:
- Single-digit millisecond reads
- High write throughput
- Automatic data expiration (TTL)
- No single point of failure
- Linear scalability

**Data Lifecycle**:

| Data Type | TTL | Reason |
|-----------|-----|--------|
| Resume position | 180 days | Users may return to old content |
| Continue watching | 30 days | Keep homepage relevant |
| Active sessions | 4 hours | Sessions don't last longer |
| Real-time QoS | 7 days | Longer trends in cold storage |

---

### Lakehouse Layer

**What it does**: Stores complete event history in an optimized format for analytics and machine learning.

**Data Stored**:

| Table | Contents | Partitioning | Retention |
|-------|----------|--------------|-----------|
| Raw events | Every playback event | Date + Hour | 2 years |
| Sessions | Aggregated session records | Date | 2 years |
| QoS historical | Time-series quality metrics | Date + Content | 2 years |

**Capabilities**:

| Feature | Benefit |
|---------|---------|
| Schema evolution | Add columns without rewriting data |
| Time travel | Query data as it existed at any past point |
| Partition pruning | Efficient queries on time ranges |
| Hidden partitioning | Users query without knowing partition structure |
| Snapshot isolation | Consistent reads during writes |
| Compaction | Automatic optimization of small files |

**Storage Efficiency**:
- Columnar format (only read needed columns)
- Compression (70-90% size reduction)
- Predicate pushdown (filter at storage level)
- Statistics-based pruning (skip irrelevant files)

---

### Query Layer

**What it does**: Provides SQL access to all platform data for analytics and reporting.

**Interactive Query Engine**:
- Ad-hoc SQL queries against lakehouse
- Sub-second response for filtered queries
- Concurrent query support
- Federation across lakehouse and hot storage
- Standard SQL syntax

**Batch Processing Engine**:
- Large-scale aggregations (hourly/daily rollups)
- Machine learning feature generation
- Data compaction and optimization
- Complex transformations

**Typical Query Patterns**:

| Use Case | Query Type | Data Source |
|----------|------------|-------------|
| "How many users watched title X yesterday?" | Aggregation | Lakehouse |
| "What's the completion rate by device?" | Group-by analysis | Lakehouse |
| "Show me user Y's viewing history" | Point lookup | Lakehouse |
| "Current rebuffer rate for title Z" | Real-time metric | Hot storage |
| "Generate training data for recommendations" | Batch export | Lakehouse |

---

### Orchestration Layer

**What it does**: Schedules and monitors batch data pipelines.

**Scheduled Pipelines**:

| Pipeline | Schedule | Purpose |
|----------|----------|---------|
| Hourly aggregation | Every hour | Roll up events into hourly summaries |
| Daily compaction | Daily 3 AM | Optimize lakehouse file layout |
| Data quality checks | Every 30 min | Validate data freshness and accuracy |
| ML feature pipeline | Daily 6 AM | Generate features for recommendation models |
| Report generation | Weekly | Create business intelligence reports |

**Capabilities**:
- Dependency management (Job B waits for Job A)
- Automatic retries with backoff
- Failure alerting
- Backfill support (reprocess historical dates)
- Resource management (concurrent job limits)
- Audit logging

---

### Data Quality Layer

**What it does**: Continuously validates data accuracy, freshness, and completeness.

**Validation Types**:

| Check Type | Example | Action on Failure |
|------------|---------|-------------------|
| Schema validation | All required fields present | Alert + investigate |
| Freshness check | Data less than 5 minutes old | Alert + page on-call |
| Volume check | Event count within 20% of expected | Alert + investigate |
| Value distribution | No unexpected NULL rates | Alert + dashboard |
| Cross-system consistency | Hot and cold data match | Alert + reconciliation |

**Quality Metrics Tracked**:
- Data freshness (time since last event)
- Completeness (percentage of expected data present)
- Accuracy (validation rule pass rate)
- Consistency (cross-system agreement)

---

### Observability Layer

**What it does**: Provides visibility into platform health and performance.

**Metrics Collection**:
- System metrics (CPU, memory, disk, network)
- Application metrics (throughput, latency, errors)
- Business metrics (events processed, sessions created)
- Custom metrics (job-specific indicators)

**Dashboards Available**:

| Dashboard | Purpose | Key Metrics |
|-----------|---------|-------------|
| Pipeline Health | Overall platform status | Throughput, lag, error rates |
| QoS Metrics | Streaming quality monitoring | Rebuffer ratio, bitrate, errors |
| Data Freshness | Data timeliness tracking | Lag by table, last update time |
| Resource Utilization | Capacity planning | CPU, memory, disk usage |
| Job Performance | Batch pipeline monitoring | Duration, success rate, data volume |

**Alerting**:
- Threshold-based alerts (lag greater than 5 minutes)
- Anomaly detection (unusual patterns)
- Multi-channel notification (email, Slack, PagerDuty)
- Alert grouping and deduplication
- Escalation policies

**Distributed Tracing**:
- End-to-end request tracking
- Latency breakdown by component
- Error correlation across services
- Performance bottleneck identification

---

### Serving Layer

**What it does**: Exposes platform data through production-ready APIs.

**Available Endpoints**:

| Endpoint | Purpose | Response Time |
|----------|---------|---------------|
| GET /resume/{contentId} | Get resume position for user and content | Less than 10ms |
| GET /continue-watching | Get user's continue watching list | Less than 15ms |
| GET /active-session | Check if user has active playback | Less than 10ms |
| GET /qos/realtime | Current platform QoS metrics | Less than 20ms |
| GET /qos/content/{id} | QoS metrics for specific content | Less than 20ms |
| GET /health | Service health check | Less than 5ms |

**API Features**:
- Interactive documentation (Swagger UI)
- Request validation
- Error handling with meaningful messages
- Rate limiting per client
- Authentication support
- Response caching where appropriate

---

## 💾 Storage Strategy

### Hot vs Cold: Decision Matrix

| Characteristic | Hot Storage | Cold Storage |
|----------------|-------------|--------------|
| **Access latency** | Less than 10ms | Seconds to minutes |
| **Query pattern** | Known, simple lookups | Ad-hoc, complex analytics |
| **Data freshness** | Real-time (seconds) | Near real-time (minutes) |
| **Retention** | Days to months | Years |
| **Cost** | Higher (SSD, memory) | Lower (object storage) |
| **Schema flexibility** | Fixed, optimized | Evolving, flexible |

### What Goes Where

| Data | Hot Storage | Cold Storage | Reason |
|------|-------------|--------------|--------|
| Resume position | ✅ | ❌ | Needs instant access |
| Continue watching | ✅ | ❌ | Homepage feature |
| Active sessions | ✅ | ❌ | Real-time state |
| Real-time QoS | ✅ | ✅ | Dashboards + history |
| Raw events | ❌ | ✅ | Volume, analytics |
| Session aggregates | ❌ | ✅ | Historical analysis |
| ML features | ❌ | ✅ | Batch training |

---

## ⚡ Real-Time Features

### Resume Playback

**User Experience**: "I stopped watching halfway through on my phone. When I open my TV app, it asks if I want to continue where I left off."

**How It Works**:

```
User opens content on TV
         │
         ▼
    ┌─────────────┐
    │ Resume API  │
    │   lookup    │
    └──────┬──────┘
           │
           ▼
    ┌─────────────┐     ┌─────────────────────────────────┐
    │ Hot Storage │────▶│ Found: position 32:45           │
    │   query     │     │ Progress: 65%                   │
    └─────────────┘     │ Last watched: 2 hours ago       │
                        └─────────────────────────────────┘
                                       │
                                       ▼
                        ┌─────────────────────────────────┐
                        │ Smart Resume Logic:              │
                        │ • Not completed (less than 90%) │
                        │ • Back up 10 seconds for context│
                        │ • Resume at 32:35               │
                        └─────────────────────────────────┘
                                       │
                                       ▼
                        ┌─────────────────────────────────┐
                        │ Response to TV app:              │
                        │ position: 32:35                 │
                        │ showResumePrompt: true          │
                        └─────────────────────────────────┘
```

### Continue Watching Row

**User Experience**: "My homepage shows a row of everything I've started watching, with progress bars showing how much I've seen."

**How It Works**:
- Sorted by most recently watched
- Shows progress percentage
- Excludes completed content
- Updates within seconds of watching
- Personalized per profile

### Quality Monitoring

**Operator Experience**: "I can see in real-time that users in Germany are experiencing higher rebuffer rates, likely due to a CDN issue."

**Metrics Available**:
- Rebuffer ratio by region, content, and device
- Time to first frame
- Bitrate distribution
- Error rates
- Session success rate

---

## 📊 Analytics Capabilities

### Ad-Hoc Queries

Answer questions like:
- "What percentage of users complete episode 1 of our new series?"
- "How does viewing behavior differ between mobile and TV?"
- "What time of day has the highest engagement?"
- "Which content has the worst quality metrics?"

### Scheduled Reports

Automatically generated:
- Daily viewing summary
- Weekly content performance
- Monthly platform health
- Quarterly trend analysis

### Machine Learning Support

Data available for:
- Recommendation model training
- Churn prediction features
- Content popularity forecasting
- Quality prediction models

---

## 🔧 Operational Features

### Schema Evolution

**Scenario**: "We need to add a new field to track audio language selection."

**Process**:
1. Add optional field to schema with default value
2. Register new schema version
3. Producers upgrade gradually
4. Consumers see new field when available
5. No downtime, no data loss

### Failure Recovery

**Scenario**: "A processing node crashed mid-operation."

**Automatic Recovery**:
1. Failure detected within seconds
2. Checkpoint state restored from storage
3. Processing resumes from last checkpoint
4. No events lost or duplicated
5. Alerts sent to operations team

### Backfill Operations

**Scenario**: "We fixed a bug in session calculation and need to reprocess last month's data."

**Process**:
1. Deploy fixed processor version
2. Trigger replay from historical events
3. Reprocess into separate output tables
4. Validate results
5. Swap tables atomically

### Scaling

**Scenario**: "Black Friday is coming, expecting 5x normal traffic."

**Actions Available**:
- Add ingestion nodes (horizontal)
- Add processing parallelism
- Expand storage capacity
- Pre-warm caches
- All without downtime

---

## 🖥 Web Interfaces

| Interface | Port | Purpose |
|-----------|------|---------|
| Kafka UI | 8080 | Topic browser, consumer monitoring |
| Schema Registry UI | 8081 | Schema versions, compatibility |
| Flink Dashboard | 8082 | Job status, checkpoints, metrics |
| Trino UI | 8083 | Query execution, cluster status |
| Spark UI | 8084 | Job progress, stages, executors |
| Airflow | 8085 | DAG status, task logs, scheduling |
| Grafana | 3000 | Dashboards, alerts, metrics |
| Prometheus | 9090 | Raw metrics, query interface |
| Jaeger | 16686 | Distributed traces |
| MinIO Console | 9001 | Object storage browser |
| Playback API Docs | 8090 | Swagger API documentation |

---


## 📈 Performance Characteristics

| Metric | Target | Achieved |
|--------|--------|----------|
| Ingestion throughput | 100K events/sec | ✅ |
| Processing latency | Less than 5 seconds | ✅ |
| Resume API response | Less than 10ms p99 | ✅ |
| Data freshness | Less than 2 minutes | ✅ |
| Recovery time | Less than 60 seconds | ✅ |
| Query response (filtered) | Less than 5 seconds | ✅ |

---

## 📚 Additional Resources

- Architecture Decision Records — docs/adr/
- Runbooks — docs/runbooks/
- API Specifications — docs/api/
- Schema Definitions — schemas/
- Configuration Reference — docs/configuration/

---
## 📄 License

This project is licensed under the MIT License - see LICENSE for details.
