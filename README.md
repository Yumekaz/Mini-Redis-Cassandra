# Mini-Redis/Cassandra — Experimental Replicated Distributed Datastore

[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![No Dependencies](https://img.shields.io/badge/dependencies-none-green.svg)

An **experimental replicated distributed datastore** built from scratch in Python. It combines a Redis-like command surface with Cassandra-inspired sharding, replica-set reads, persistence, failure injection, and term-based cluster coordination. It is an in-memory-first systems infrastructure project designed to make replication and failure behavior observable.

> ⚠️ **Scope**: This is serious systems infrastructure research/prototyping, but it is not production-ready. Leader election, replication, repair, and persistence are intentionally bounded implementations; the limitations below are part of the design contract.

---

## ⚡ Quick Start (2 Minutes)

```bash
# Terminal 1 - Start first node
python -m minidb.main --node-id node1 --port 7001 --cluster-port 8001 --data-dir ./data/node1

# Terminal 2 - Join second node
python -m minidb.main --node-id node2 --port 7002 --cluster-port 8002 --data-dir ./data/node2 --seed localhost:8001

# Terminal 3 - Join third node
python -m minidb.main --node-id node3 --port 7003 --cluster-port 8003 --data-dir ./data/node3 --seed localhost:8001

# Terminal 4 - Connect CLI
python -m minidb.cli localhost 7001
```

Or use the automated launcher:
```bash
./run.sh cluster      # Linux/macOS
.\run_dbms.bat        # Windows
```

---

## ✨ Features

| Category | Features |
|----------|----------|
| **Storage** | In-memory KV store, TTL support, pattern matching |
| **Persistence** | AOF logging + periodic snapshots |
| **Clustering** | Gossip membership, simplified term-based leader election |
| **Sharding** | Consistent hashing with shard-owner write routing |
| **Consistency** | Tunable reads (`ANY`, `QUORUM`, `ALL`, `STRONG`) and replica-set write acknowledgments |
| **Fault Tolerance** | Basic failover, read repair, anti-entropy, fault injection |
| **Chaos Testing** | Built-in fault injection for testing resilience |

---

## 📊 Performance (Indicative)

Local testing on a 3-node cluster shows:
- Sustained thousands of read/write operations per second under light contention
- Leader re-election within a few seconds during node failure scenarios

These measurements validate system behavior, not production performance. See [BENCHMARK.md](docs/BENCHMARK.md) for methodology.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT APPLICATIONS                       │
└─────────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
            ┌─────────────┐         ┌─────────────┐
            │   NODE 1    │◄───────►│   NODE 2    │◄───────►...
            │ (Shard Peer)│ Gossip  │ (Shard Peer)│
            └──────┬──────┘         └──────┬──────┘
                   │                       │
            ┌──────┴──────┐         ┌──────┴──────┐
            │   Storage   │         │   Storage   │
            │  (KV + AOF) │         │  (KV + AOF) │
            └─────────────┘         └─────────────┘
```

**Key Components:**
- **Storage Engine** - Thread-safe HashMap with TTL and statistics
- **Cluster Coordinator** - Gossip-based membership and simplified leader election
- **Sharding Layer** - Consistent hash ring with 150 virtual nodes
- **Persistence** - Write-ahead AOF log + periodic snapshots with metadata recovery
- **Read Coordinator** - Consistency-aware reads with deterministic read repair

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed design.

---

### Example 1: Basic Operations
```
minidb:7001> SET user:1 "Alice"
OK

minidb:7001> GET user:1
"Alice"

minidb:7001> SETEX session:token 3600 "abc123"   # TTL in seconds
OK
```

### Example 2: Cluster Management
```
minidb:7001> NODES
NODE ID    ADDRESS         ROLE      STATE
node1      localhost:7001  LEADER    ALIVE
node2      localhost:7002  FOLLOWER  ALIVE
node3      localhost:7003  FOLLOWER  ALIVE

minidb:7001> CONSISTENCY QUORUM   # Set read consistency
```

### Example 3: Fault Injection
```
minidb:7001> FAULT ENABLE
minidb:7001> FAULT DELAY 100      # Add 100ms latency
minidb:7001> FAULT PARTITION node2 # Isolate node2
minidb:7001> FAULT CLEAR
```

See [CLI Commands](docs/cli-commands.md) for full reference.

---

## 🧪 Testing

```bash
# Run KV store unit tests
python tests/test_kv.py

# Run project validation
python tests/test_validation.py

# Run quick health check
python tests/test_quick.py

# Run full cluster tests
python tests/test_cluster.py

# Run failure-oriented distributed behavior tests
python tests/test_resilience.py
```

---

## ⚖️ Comparison with Redis & Cassandra

*This comparison highlights conceptual similarities, not production parity or behavioral equivalence.*

| Feature | Mini-Redis/Cassandra | Redis Cluster | Cassandra |
|---------|--------|---------------|-----------|
| **Language** | Pure Python | C | Java |
| **Dependencies** | None | Many | Many |
| **Consistency** | Tunable reads + replica-set write acks | Eventual | Tunable |
| **Leader Election** | Simplified term-based election | Gossip | Paxos |
| **Sharding** | Consistent Hash | Hash Slots | Vnodes |
| **Persistence** | AOF + Snapshot | RDB + AOF | SSTable |
| **Use Case** | Experimental infrastructure | Production | Production |

**What this project provides:**
- Consistent hashing concepts (similar to Cassandra)
- AOF persistence pattern (similar to Redis)
- Tunable consistency trade-offs
- Shard-owner write coordination with replica-set replication
- Term-based leader coordination and failure-oriented cluster behavior

---

## 🎯 Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Pure Python** | No dependencies, portable, inspectable, and easy to extend |
| **JSON Protocol** | Human-readable, easy debugging |
| **Simplified Leader Election** | Easier to follow than full Raft, but not consensus-safe |
| **Virtual Nodes** | Better load distribution than simple hashing |
| **Read Repair** | Eventual consistency without background anti-entropy |
| **In-Memory First** | Performance over durability (with optional persistence) |

See [DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md) for detailed rationale.

---

## 📚 Operating the Project

1. **Clone the repository**
   ```bash
   git clone https://github.com/Yumekaz/Mini-Redis-Cassandra.git
   cd Mini-Redis-Cassandra
   ```

2. **Start a cluster** (see [Quick Start](#-quick-start-2-minutes))

3. **Read the docs:**
   - [Quick Start](docs/quickstart.md)
   - [CLI Commands Reference](docs/cli-commands.md)
   - [Consistency Models](docs/consistency-models.md)

---

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | Ensure node is running on that port |
| No leader available | Wait 2-3 seconds for election |
| Primary owner unavailable | Check cluster health and replica availability |
| Rate limited | Reduce request rate, retry later |

See [docs/troubleshooting.md](docs/troubleshooting.md) for detailed solutions.

---

## FailForge (seed 42)

This repo is a first-party target of **[FailForge](https://github.com/Yumekaz/FAILFORGE)** (`failforge_minidb.yml`). Seed **42** historically failed `read_after_acknowledged_write` under concurrent clients (happy path after minimize — no faults required). QUORUM defaults, fail-closed empty remotes, and connection-pool fixes closed most of that class.

- **Postmortem:** [docs/postmortems/2026-07-failforge-seed42-raw.md](docs/postmortems/2026-07-failforge-seed42-raw.md)
- **Regression tests:** `tests/test_quorum_raw.py`
- **Re-run:** from a sibling FailForge checkout:  
  `./bin/failforge run failforge_minidb.yml --seed 42`

Stack context: [Cairn `docs/STACK.md`](https://github.com/Yumekaz/Cairn/blob/main/docs/STACK.md).

---

## ⚠️ Current Engineering Boundaries

This project intentionally stops short of several production distributed-systems guarantees:

- **Consensus safety** - leader election is simplified and is not a full Raft implementation
- **Split-brain prevention** - no fencing or lease-based protection is implemented
- **Read guarantees** - `STRONG` reads go to the key's primary owner, not a globally linearizable consensus layer
- **Delete semantics** - delete propagation is simpler than Cassandra tombstones and compaction
- **Scalability** - tested on small clusters; not designed for large deployments

These limitations are discussed in detail in [DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md).

---

## 📄 License & Author

**License:** MIT - see [LICENSE](LICENSE)

**Author:** See [AUTHORS.md](AUTHORS.md)

---

## 🗂️ Project Structure

```
Mini-Redis-Cassandra/
├── minidb/           # Core database code
│   ├── chaos/        # Fault injection & rate limiting
│   ├── cluster/      # Coordination, election, replication
│   ├── network/      # TCP protocol
│   ├── repair/       # Anti-entropy
│   ├── sharding/     # Consistent hashing, routing
│   └── storage/      # KV store, AOF, snapshots
├── docs/             # Documentation
├── examples/         # Demo scripts & sample data
└── tests/            # Test suite
```
