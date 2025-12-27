# Consistency Models

Mini-Redis/Cassandra supports tunable consistency levels for read operations, allowing you to balance between performance and data freshness.

## Overview

| Level | Reads From | Latency | Consistency | Use Case |
|-------|------------|---------|-------------|----------|
| **ANY** | Local node | Fastest | Lowest | Caching, analytics |
| **QUORUM** | Majority | Medium | High | General purpose |
| **ALL** | All nodes | Slow | Highest | Critical data |
| **STRONG** | Leader only | Medium | Linearizable | Financial transactions |

---

## ANY Consistency

**Behavior:** Read from the local node only.

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

**Behavior:** Read from all replica nodes.

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

**Behavior:** Read only from the current leader.

```
minidb:7001> CONSISTENCY STRONG
minidb:7001> GET mykey
```

**Trade-offs:**
- ✅ Linearizable - always returns the latest committed value
- ✅ Simple mental model
- ❌ Single point of read (leader)
- ❌ Fails if leader is down

**Best for:** Financial transactions, counters, any operation requiring linearizability.

---

## Read Repair

When reading with QUORUM or ALL consistency, if nodes have different values:

1. Mini-Redis/Cassandra compares version vectors
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

All writes go through the leader and are replicated to followers before acknowledgment. The write quorum is determined by the cluster configuration.
