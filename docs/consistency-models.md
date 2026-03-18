# Consistency Models

Mini-Redis/Cassandra supports tunable consistency levels for read operations, allowing you to balance performance and freshness across a key's replica set.

## Overview

| Level | Reads From | Latency | Consistency | Use Case |
|-------|------------|---------|-------------|----------|
| **ANY** | Local replica or first reachable replica | Fastest | Lowest | Caching, analytics |
| **QUORUM** | Majority | Medium | High | General purpose |
| **ALL** | All nodes | Slow | Highest | Critical data |
| **STRONG** | Primary owner | Medium | Primary-owner latest view | Owner-directed reads |

---

## ANY Consistency

**Behavior:** Read from the local replica when possible, otherwise the first reachable replica.

```
minidb:7001> CONSISTENCY ANY
minidb:7001> GET mykey
```

**Trade-offs:**
- ✅ Fastest response time
- ✅ Works even if other nodes are down
- ❌ May return stale data

**Best for:** Caching layers, analytics queries, read-heavy workloads where slight staleness is acceptable.

---

## QUORUM Consistency

**Behavior:** Read from a majority of replica nodes and return the most recent value.

```
minidb:7001> CONSISTENCY QUORUM
minidb:7001> GET mykey
```

For a 3-node cluster with replication factor 3:
- Reads from 2 out of 3 nodes
- Compares versions to find newest
- Triggers read-repair if versions differ

**Trade-offs:**
- ✅ Good balance of speed and consistency
- ✅ Tolerates 1 node failure
- ✅ Self-healing via read repair
- ❌ Higher latency than ANY

**Best for:** Most applications. This is the default.

---

## ALL Consistency

**Behavior:** Read from all replicas for the key.

```
minidb:7001> CONSISTENCY ALL
minidb:7001> GET mykey
```

**Trade-offs:**
- ✅ Highest consistency guarantee
- ❌ Slowest (waits for all nodes)
- ❌ Fails if any node is down

**Best for:** Critical reads where you need absolute certainty.

---

## STRONG Consistency

**Behavior:** Read only from the key's current primary owner in the hash ring.

```
minidb:7001> CONSISTENCY STRONG
minidb:7001> GET mykey
```

**Trade-offs:**
- ✅ Reads from the shard owner that coordinates writes for that key
- ✅ Avoids comparing multiple replica responses
- ❌ Not a globally linearizable consensus read
- ❌ Fails if the primary owner is down

**Best for:** Cases where you want the key's primary owner view without paying for a quorum read.

---

## Read Repair

When reading with QUORUM or ALL consistency, if nodes have different values:

1. Mini-Redis/Cassandra compares `(version, updated_at, coordinator_id)`
2. The newest version is returned to the client
3. Stale nodes are updated in the background (read repair)

This provides **eventual consistency** - even if writes fail to some nodes, reads will eventually repair them.

---

## Setting Consistency

### Per-Session Default
```
minidb:7001> CONSISTENCY QUORUM
Default consistency set to: QUORUM
```

### Per-Request
```
minidb:7001> GET mykey STRONG
```

---

## Write Consistency

Writes are routed to the key's primary owner and replicated only to that key's replica set. The number of acknowledgments required depends on the node's configured default consistency level (`ONE`, `ANY`, `QUORUM`, `ALL`, or `STRONG`).
