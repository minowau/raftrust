# ADR-005: gRPC + Protobuf for All RPCs

## Status
Accepted

## Context
We need a wire protocol for both node-to-node Raft communication and client-to-server KV operations.

## Options Considered

### Custom TCP protocol
- **Pros**: Minimal overhead, full control over framing
- **Cons**: Must implement framing, serialization, connection management, TLS, load balancing from scratch

### gRPC + Protobuf
- **Pros**: Strongly typed schema, streaming support (Watch, LeaseKeepAlive), built-in HTTP/2 multiplexing, mature Rust ecosystem (tonic), client codegen for any language
- **Cons**: HTTP/2 overhead, protobuf schema evolution constraints

### JSON over HTTP
- **Pros**: Human-readable, easy debugging
- **Cons**: Parsing overhead, no streaming, no schema enforcement, poor for high-throughput replication

## Decision
We chose **gRPC + Protobuf** (via tonic/prost) because:

1. **Streaming**: The Watch API and LeaseKeepAlive require bidirectional streaming, which gRPC supports natively.
2. **Type safety**: Protobuf schemas catch wire format mismatches at compile time.
3. **Performance**: Binary encoding + HTTP/2 multiplexing provide good throughput for Raft replication.
4. **etcd compatibility**: etcd uses gRPC for its entire API surface. Our proto definitions are compatible with etcd's client patterns.

## Consequences
- Debugging raw traffic requires protobuf-aware tools (grpcurl, grpcui)
- Schema changes must maintain backward compatibility (protobuf field numbering)
- The HTTP operational endpoints (/metrics, /health, /ready) use a separate lightweight HTTP/1.1 server since they don't need gRPC features

## What Would Change This Decision
If we needed sub-millisecond replication latency, a custom TCP protocol with zero-copy serialization (flatbuffers, cap'n proto) would reduce overhead. For our use case, gRPC's overhead is negligible compared to disk I/O and network RTT.
