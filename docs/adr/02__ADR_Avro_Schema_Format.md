# ADR-002: Apache Avro for Event Serialization

**Status**: Accepted
**Date**: 2026-01-14
**Deciders**: Platform Team

## Context

We need a serialization format for playback events in Kafka. Requirements:
- Type safety: Catch schema mismatches at compile time
- Schema evolution: Add fields without breaking consumers
- Compact: Minimize network/storage overhead
- Tooling: Code generation, schema registry integration
- Cross-language: Java producers, potential Python/Go consumers

## Decision Drivers

- Schema evolution with backward/forward compatibility
- Compact binary format for high-throughput messaging
- Integration with Confluent Schema Registry
- Strong typing with code generation
- Industry adoption for event streaming

## Considered Options

### 1. Apache Avro
- Binary format with schema
- Schema stored in registry, not in each message
- Native Schema Registry integration
- Code generation from .avsc files

### 2. Protocol Buffers (Protobuf)
- Binary format from Google
- Excellent for RPC (gRPC)
- Schema evolution via field numbers
- Smaller wire format than Avro

### 3. JSON
- Human-readable text format
- No schema enforcement at runtime
- Larger message size
- Flexible but error-prone

### 4. JSON Schema
- JSON with schema validation
- Human-readable
- Less mature tooling for Kafka
- Larger messages than binary formats

### 5. Apache Thrift
- Binary/compact protocols
- Less common for Kafka streaming
- Weaker schema evolution story

## Decision

We will use **Apache Avro** with Confluent Schema Registry.

**Rationale:**

1. **Schema Registry Integration**
   - Avro is the native format for Confluent Schema Registry
   - Schemas registered once, referenced by ID in messages
   - 5-byte overhead per message (magic byte + 4-byte schema ID)
   - Automatic compatibility checking on schema updates

2. **Schema Evolution**
   - BACKWARD compatibility: New schema can read old data
   - FORWARD compatibility: Old schema can read new data
   - Add optional fields without breaking existing consumers
   - Default values enable seamless upgrades

3. **Compact Wire Format**
   - Binary encoding without field names in each message
   - Typically 50-80% smaller than JSON
   - Schema stored once in registry, not repeated

4. **Code Generation**
   - Generate Java classes from .avsc schema files
   - Type-safe builders: `PlaybackEvent.newBuilder().setUserId(...).build()`
   - Compile-time validation prevents field name typos

5. **Industry Standard**
   - Netflix, LinkedIn, Twitter use Avro for Kafka events
   - Mature tooling and documentation
   - Wide language support (Java, Python, Go, C#)

**Why not Protobuf:**
- Less native Schema Registry support (requires separate plugin)
- Field numbers add maintenance burden
- Better suited for gRPC than event streaming

**Why not JSON:**
- No compile-time type safety
- 3-5x larger message size
- Schema drift issues in production
- No built-in evolution guarantees

## Consequences

### Positive
- Type-safe code with generated classes
- Compact messages (important at scale)
- Safe schema evolution with compatibility checks
- Single source of truth for event structure

### Negative
- Schema files must be maintained alongside code
- Build step required for code generation
- Binary format harder to debug (use Kafka UI or avro-tools)
- Learning curve for Avro union types

### Risks
- **Schema drift**: If schemas evolve incorrectly, consumers break. Mitigation: Schema Registry compatibility enforcement.
- **Complex unions**: Avro unions require careful handling. Mitigation: Document payload patterns clearly.

## References

- [Avro vs Protobuf vs JSON - Confluent](https://www.confluent.io/blog/avro-kafka-data/)
- [Schema Registry Overview](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Netflix Data Mesh and Avro](https://netflixtechblog.com/data-mesh-a-data-movement-and-processing-platform-netflix-1288bcab2873)
