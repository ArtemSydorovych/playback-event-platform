# ADR-001: Apache Kafka for Event Streaming

**Status**: Accepted
**Date**: 2026-01-14
**Deciders**: Platform Team

## Context

We need a messaging system to handle playback events from video players. The system must support:
- High throughput (millions of events/day)
- Event replay for debugging and reprocessing
- Multiple consumers reading the same events
- Ordered delivery per user session
- Integration with stream processing (Flink)

## Decision Drivers

- Throughput: Handle peak loads during popular content releases
- Durability: Events must not be lost, need replay capability
- Ordering: Events from same session must be processed in order
- Ecosystem: Integration with Flink, Schema Registry, and data lakehouse
- Operational maturity: Proven at Netflix-scale workloads

## Considered Options

### 1. Apache Kafka
- Distributed commit log with partitioned topics
- Messages persisted to disk, configurable retention
- Consumer groups with offset management
- Native Flink connector

### 2. RabbitMQ
- Traditional message broker (AMQP)
- Push-based delivery to consumers
- Excellent for task queues and RPC patterns
- Messages typically deleted after acknowledgment

### 3. Amazon Kinesis / AWS MSK
- Managed Kafka-compatible service
- Lower operational overhead
- Vendor lock-in, higher cost at scale

### 4. Apache Pulsar
- Multi-tenancy, tiered storage
- Less mature ecosystem
- Steeper learning curve

## Decision

We will use **Apache Kafka**.

**Rationale:**
1. **Log-based architecture** - Perfect for event sourcing. Events are appended to partitions and retained, allowing replay from any point.
2. **Consumer independence** - Multiple consumers (real-time analytics, batch processing, ML pipelines) can read at their own pace without affecting others.
3. **Ordering guarantees** - Partition by userId ensures all events for a user are processed in order.
4. **Ecosystem** - First-class support for Confluent Schema Registry, Kafka Connect, and Flink.
5. **Proven at scale** - Netflix, LinkedIn, Uber all use Kafka for similar event streaming workloads.

**Why not RabbitMQ:**
- RabbitMQ excels at work queues and request-reply patterns, not event streaming
- Messages are deleted after consumption (no replay)
- Throughput degrades with large backlogs
- No native partitioning for ordering guarantees

## Consequences

### Positive
- Events can be replayed for debugging or reprocessing
- Multiple consumers can independently process the same events
- Seamless integration with Flink for stream processing
- Proven scalability pattern

### Negative
- More complex operations than RabbitMQ (Zookeeper/KRaft, partitions, replication)
- Higher memory/disk requirements
- Need to manage consumer offsets

### Risks
- **Partition skew**: If one user generates disproportionate traffic, their partition becomes hot. Mitigation: Monitor partition lag, consider sub-partitioning.
- **Consumer lag**: Slow consumers can fall behind. Mitigation: Alerting on consumer group lag.

## References

- [Kafka vs RabbitMQ - Confluent](https://www.confluent.io/blog/kafka-vs-rabbitmq/)
- [Netflix Keystone Pipeline](https://netflixtechblog.com/keystone-real-time-stream-processing-platform-a3ee651812a)
- [When to use RabbitMQ vs Kafka](https://www.cloudamqp.com/blog/when-to-use-rabbitmq-or-apache-kafka.html)
