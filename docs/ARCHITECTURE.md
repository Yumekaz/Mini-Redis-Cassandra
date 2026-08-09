# Mini-Redis/Cassandra Architecture

## System Overview

Mini-Redis/Cassandra is an experimental replicated distributed datastore implementing a Redis-like command surface, Cassandra-inspired sharding, and simplified leader-based coordination.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           CLIENT APPLICATIONS                           │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            NETWORK LAYER                                │
│                    (Custom TCP Protocol - JSON-based)                   │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
           ┌─────────────────────────┼─────────────────────────┐
           ▼                         ▼                         ▼
    ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
    │   NODE 1    │◄─────────►│   NODE 2    │◄─────────►│   NODE 3    │
    │ (Shard Peer)│  Gossip   │ (Shard Peer)│  Gossip   │ (Shard Peer)│
    └─────────────┘           └─────────────┘           └─────────────┘
           │                         │                         │
           ▼                         ▼                         ▼
    ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
    │  Storage    │           │  Storage    │           │  Storage    │
    │  Engine     │           │  Engine     │           │  Engine     │
    │ (KV + AOF)  │           │ (KV + AOF)  │           │ (KV + AOF)  │
    └─────────────┘           └─────────────┘           └─────────────┘
```

## Design Principles

| Principle | Description |
|-----------|-------------|
| **Decentralized** | No single point of failure; any node can accept reads |
| **Consistent** | Shard-owner writes use replica-set acknowledgments and deterministic version ordering |
| **Partition-Tolerant** | System continues operating during network splits |
| **Self-Healing** | Automatic repair of inconsistencies via read repair and anti-entropy |

---

## Component Architecture

### 1. Storage Engine (`minidb/storage/`)

In-memory key-value store with TTL support.

```
┌─────────────────────────────────────┐
│           KV Store                  │
│  ┌─────────────────────────────┐   │
│  │  HashMap: key → (value,     │   │
│  │           version, expiry)  │   │
│  └─────────────────────────────┘   │
│              │                      │
│              ▼                      │
│  ┌─────────────────────────────┐   │
│  │  Background TTL Cleanup     │   │
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
```

**Key Features:**
- Thread-safe operations with RLock
- Automatic TTL expiration
- Statistics tracking (hits, misses, hit rate)

---

### 2. Persistence Layer (`minidb/storage/`)

Dual persistence strategy for durability.

| Mechanism | Purpose | Trade-off |
|-----------|---------|-----------|
| **AOF (Append-Only File)** | Every write logged | Durable but larger files |
| **Snapshots** | Periodic full dump | Fast recovery, some data loss |

**Recovery Process:**
1. Load latest snapshot (fast, but might be slightly old)
2. Replay AOF entries after snapshot (fills in recent changes)
3. Database fully recovered

---

### 3. Cluster Coordination (`minidb/cluster/`)

Manages cluster membership and leader election.

```
                    ┌───────────────┐
                    │  Coordinator  │
                    └───────────────┘
                           │
         ┌─────────────────┼─────────────────┐
         ▼                 ▼                 ▼
┌─────────────────┐ ┌─────────────┐ ┌─────────────────┐
│   Membership    │ │  Election   │ │  Replication    │
│   (Gossip)      │ │ (Simplified)│ │  Manager        │
└─────────────────┘ └─────────────┘ └─────────────────┘
```

**Subcomponents:**
- **Membership** - Gossip-based node discovery and failure detection
- **Election** - simplified term-based leader election for cluster coordination
- **Replication** - shard-owner replication to the key's replica set

---

### 4. Simplified Leader Election

Used for cluster coordination and failover experiments. This is intentionally simpler than full Raft and should not be treated as consensus-safe under partitions.

```
┌──────────┐     timeout      ┌───────────┐    wins vote    ┌──────────┐
│ FOLLOWER │ ───────────────► │ CANDIDATE │ ──────────────► │  LEADER  │
└──────────┘                  └───────────┘                 └──────────┘
      ▲                             │                             │
      │         loses vote          │                             │
      └─────────────────────────────┘                             │
      │                                                           │
      │              discovers higher term                        │
      └───────────────────────────────────────────────────────────┘
```

**Election Timeline:**
```
Time    Node A          Node B          Node C
─────────────────────────────────────────────────
T=0     Follower        Follower        Follower
T=1     (timeout!)      Follower        Follower
T=2     Candidate       Follower        Follower
        "Vote for me!"
T=3     Candidate       Voted: A        Voted: A
T=4     LEADER          Follower        Follower
T=5     ♥ heartbeat →   Follower        Follower
```

---

### 5. Sharding (`minidb/sharding/`)

Distributes data across nodes using consistent hashing.

```
                    0
                    │
           Node C ──┼── Node A
                   ╱│╲
                  ╱ │ ╲
            Node B ─┴── Node D
```

**Key Routing:**
```python
"user:1"    → hash = 1234    → Node A (owns 1000-2000)
"user:2"    → hash = 5678    → Node B (owns 5000-7000)
"product:1" → hash = 8901    → Node C (owns 8000-10000)
```

**Benefits:**
- Adding/removing nodes only affects neighboring keys
- Load distributed evenly with virtual nodes
- No central routing table needed

---

### 6. Consistency Levels

Tunable consistency for read operations.

| Level | Reads From | Speed | Consistency |
|-------|------------|-------|-------------|
| **ANY** | Local node | Fastest | May be stale |
| **QUORUM** | Majority (2/3) | Medium | Strong |
| **ALL** | All nodes | Slow | Strongest |
| **STRONG** | Primary owner only | Medium | Primary-owner latest view |

---

### 7. Anti-Entropy Repair (`minidb/repair/`)

Detects and fixes data inconsistencies using Merkle trees.

```
                    Root Hash
                   /          \
            Hash(A+B)        Hash(C+D)
            /      \         /      \
        Hash(A)  Hash(B)  Hash(C)  Hash(D)
           │        │        │        │
         Key A    Key B    Key C    Key D
```

**Sync Process:**
1. Compare root hashes between nodes
2. If different, compare child hashes  
3. Recurse until divergent keys found
4. Exchange only differing keys

---

### 8. Network Protocol (`minidb/network/`)

JSON-based TCP protocol for client and cluster communication.

**Request Format:**
```json
{
  "type": "CLIENT",
  "command": "SET user:1 Alice"
}
```

**Response Format:**
```json
{
  "success": true,
  "data": "OK"
}
```

---

### 9. Fault Injection (`minidb/chaos/`)

Built-in chaos engineering for testing resilience.

| Fault Type | Effect |
|------------|--------|
| `FAULT DELAY ms` | Add network latency |
| `FAULT DROP %` | Random packet drops |
| `FAULT PARTITION node` | Isolate a node |

---

## Data Flow

### Write Path

```
Client         Any Node       Primary Owner      Replica Nodes
   │               │                 │               │
   │──SET foo=bar─►│                 │               │
   │               │──forward───────►│               │
   │               │                 │──replicate───►│
   │               │                 │◄────ACK───────│
   │               │                 │ (QUORUM met)  │
   │◄─────OK───────│◄────────────────│               │
```

### Read Path (QUORUM)

```
Client          Coordinator     Node A          Node B
   │               │               │               │
   │──GET foo─────►│               │               │
   │               │──read────────►│               │
   │               │──read──────────────────────►│
   │               │◄───value,v1───│               │
   │               │◄───value,v1───────────────────│
   │               │ (compare versions)            │
   │◄───value──────│               │               │
```

---

## File Structure

```
minidb/
├── __init__.py           # Package exports
├── config.py             # Configuration classes
├── node.py               # Main DatabaseNode class
├── main.py               # CLI entry point
├── cli.py                # Interactive CLI
│
├── storage/
│   ├── kv_store.py       # In-memory KV store
│   ├── aof.py            # Append-only file
│   └── snapshot.py       # Snapshot persistence
│
├── cluster/
│   ├── coordinator.py    # Cluster coordination
│   ├── election.py       # Simplified leader election
│   ├── membership.py     # Gossip protocol
│   ├── replication.py    # Replica-set replication
│   └── read_coordinator.py # Consistency-aware reads
│
├── sharding/
│   ├── consistent_hash.py # Hash ring
│   ├── partition.py      # Partition manager
│   ├── router.py         # Key routing
│   └── migration.py      # Shard migration
│
├── network/
│   ├── server.py         # TCP server
│   ├── client.py         # TCP client
│   └── protocol.py       # Message protocol
│
├── repair/
│   └── anti_entropy.py   # Merkle tree repair
│
└── chaos/
    ├── fault_injection.py # Chaos testing
    └── rate_limiter.py    # Rate limiting
```
