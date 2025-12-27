# Mini-Redis/Cassandra - Distributed Key-Value Database

[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![No Dependencies](https://img.shields.io/badge/dependencies-none-green.svg)

An **educational distributed, fault-tolerant, in-memory key-value database** built from scratch in Python. Demonstrates core concepts from Redis, Cassandra, and etcd — ideal for learning distributed systems.

> ⚠️ **Note**: This is a learning/demonstration project, not intended for production use. It implements simplified versions of distributed consensus, replication, and persistence to illustrate how real systems work.

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
| **Clustering** | Gossip protocol, simplified leader election |
| **Sharding** | Consistent hashing with virtual nodes |
| **Consistency** | Tunable levels: ANY, QUORUM, ALL, STRONG |
| **Fault Tolerance** | Basic failover, read repair, anti-entropy (simplified) |
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
            │  (Leader)   │ Gossip  │ (Follower)  │
            └──────┬──────┘         └──────┬──────┘
                   │                       │
            ┌──────┴──────┐         ┌──────┴──────┐
            │   Storage   │         │   Storage   │
            │  (KV + AOF) │         │  (KV + AOF) │
            └─────────────┘         └─────────────┘
```

**Key Components:**
- **Storage Engine** - Thread-safe HashMap with TTL and statistics
- **Cluster Coordinator** - Gossip-based membership and Raft-lite election
- **Sharding Layer** - Consistent hash ring with 150 virtual nodes
- **Persistence** - Write-ahead AOF log + periodic snapshots
- **Read Coordinator** - Consistency-aware reads with read repair

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
```

---

## ⚖️ Comparison with Redis & Cassandra

*This comparison highlights conceptual similarities, not production parity or behavioral equivalence.*

| Feature | Mini-Redis/Cassandra | Redis Cluster | Cassandra |
|---------|--------|---------------|-----------|
| **Language** | Pure Python | C | Java |
| **Dependencies** | None | Many | Many |
| **Consistency** | Tunable | Eventual | Tunable |
| **Leader Election** | Raft-lite (simplified) | Gossip | Paxos |
| **Sharding** | Consistent Hash | Hash Slots | Vnodes |
| **Persistence** | AOF + Snapshot | RDB + AOF | SSTable |
| **Use Case** | Learning/Demo | Production | Production |

**What this project demonstrates:**
- Consistent hashing concepts (similar to Cassandra)
- AOF persistence pattern (similar to Redis)
- Tunable consistency trade-offs
- Simplified leader election (inspired by Raft)

---

## 🎯 Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Pure Python** | No dependencies, easy to understand and modify |
| **JSON Protocol** | Human-readable, easy debugging |
| **Raft-Lite** | Simpler than full Raft, sufficient for demo |
| **Virtual Nodes** | Better load distribution than simple hashing |
| **Read Repair** | Eventual consistency without background anti-entropy |
| **In-Memory First** | Performance over durability (with optional persistence) |

See [DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md) for detailed rationale.

---

## 📚 Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/Yumekaz/Mini-Redis-Cassandra.git
   cd Mini-Redis-Cassandra
   ```

2. **Start a cluster** (see [Quick Start](#-quick-start-2-minutes))

3. **Read the docs:**
   - [Quick Start Tutorial](docs/quickstart.md)
   - [CLI Commands Reference](docs/cli-commands.md)
   - [Consistency Models](docs/consistency-models.md)

---

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | Ensure node is running on that port |
| No leader available | Wait 2-3 seconds for election |
| Not leader error | Connect to leader node for writes |
| Rate limited | Reduce request rate, retry later |

See [docs/troubleshooting.md](docs/troubleshooting.md) for detailed solutions.

---

## ⚠️ Known Limitations

This project intentionally simplifies several aspects of production distributed systems:

- **Consensus safety** - Raft-lite does not provide full safety guarantees under network partitions
- **Split-brain prevention** - No fencing or quorum-based protection implemented
- **Clock skew** - No handling for clock drift between nodes
- **Crash recovery** - AOF replay may not fully recover all edge cases
- **Scalability** - Tested with 3-10 nodes; not designed for large clusters

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
