# Benchmarks

Performance characteristics of Mini-Redis/Cassandra.

## Test Environment

- Python 3.10
- 3-node cluster on localhost
- No fault injection

---

## Mini-Redis/Cassandra Performance

### Single-Node Operations

| Operation | Throughput | Latency (avg) |
|-----------|------------|---------------|
| SET | ~4,000 ops/sec | 0.24ms |
| GET (ANY) | ~4,300 ops/sec | 0.23ms |
| GET (QUORUM) | ~4,400 ops/sec | 0.22ms |
| GET (STRONG) | ~4,000 ops/sec | 0.24ms |
| DELETE | ~4,000 ops/sec | 0.24ms |

### Cluster Operations

| Metric | Value |
|--------|-------|
| Leader election time | ~2.4s |
| Write replication latency | <10ms |
| Node join time | ~1s |
| Failure detection | 3-5s |
| Rebalance (1000 keys) | ~5s |

---

## Comparison with Production Systems

| Metric | Mini-Redis/Cassandra | Redis | Cassandra |
|--------|---------------------|-------|-----------|
| **SET ops/sec** | 4,000 | 100,000+ | 50,000+ |
| **GET ops/sec** | 4,300 | 200,000+ | 80,000+ |
| **Latency (p99)** | <1ms | <1ms | 2-10ms |
| **Election time** | ~2.4s | N/A (Sentinel: ~30s) | N/A (Paxos) |
| **Nodes supported** | 3-10 | 1000+ | 1000+ |

### Why the Difference?

| Factor | Mini-Redis/Cassandra | Production Systems |
|--------|---------------------|-------------------|
| **Language** | Pure Python (interpreted) | C / Java (compiled/JIT) |
| **Threading** | Single-threaded per connection | Multi-threaded, async I/O |
| **Memory** | Python objects (~500B/key) | Optimized structs (~50B/key) |
| **Network** | Blocking sockets | epoll/kqueue, zero-copy |
| **Persistence** | Simple JSON files | Memory-mapped, binary formats |

---

## What We Prioritized

| Priority | Description |
|----------|-------------|
| **Inspectability** | Code and state transitions are visible and well-commented |
| **Correctness** | Core invariants are exercised by focused tests |
| **Simplicity** | No external dependencies |
| **Operability** | Easy to run, observe, and failure-test |

---

## When to Use Mini-Redis/Cassandra

✅ **Good for:**
- Distributed-systems experimentation and systems R&D
- Prototyping replication, sharding, repair, and failure behavior
- Integration with failure-testing workflows
- Studying implementation trade-offs against Redis/Cassandra architectures

❌ **Not for:**
- Production workloads
- High-throughput applications
- Large datasets (>10,000 keys)
- Mission-critical systems

---

## Scalability Notes

- Uses consistent hashing with 150 virtual nodes
- Adding nodes redistributes ~25% of keys (for 3→4 nodes)
- Memory usage: ~500 bytes per key (Python object overhead)
- Max tested: 10 nodes, 50,000 keys

---

Mini-Redis/Cassandra is designed for **experimental infrastructure and systems research**, not production workloads. For production, use [Redis](https://redis.io/) or [Apache Cassandra](https://cassandra.apache.org/) until the guarantees and security controls required by your deployment have been independently established.
