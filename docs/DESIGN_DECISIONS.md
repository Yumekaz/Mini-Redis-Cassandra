# Design Decisions

This document explains key architectural choices and their rationale.

## Why Pure Python?

- **Zero dependencies** - Runs anywhere with Python 3.7+
- **Educational clarity** - No framework magic to understand
- **Easy modification** - Students can experiment directly

## Why JSON Protocol?

- **Human-readable** - Debug with telnet, nc, or any text tool
- **Self-describing** - No schema files needed
- **Trade-off accepted** - Slower than binary, but clearer

## Why Simplified Leader Election Instead of Full Raft?

Full Raft requires:
- Log replication
- Log compaction
- Membership changes
- Snapshotting

Our simplified version implements:
- Leader election via term-based voting
- Heartbeat-based failure detection
- Automatic re-election on leader failure

**Intentionally omitted**: full consensus-safe log replication and durable quorum commit.

## Why 150 Virtual Nodes?

Research shows 100-200 vnodes provide good balance:
- Enough for even distribution across 3-10 nodes
- Not so many that ring operations become slow
- 150 is the Cassandra default

## Why In-Memory First?

- **Performance** - Sub-millisecond latency
- **Simplicity** - No LSM tree complexity
- **Persistence via AOF** - Durability when needed

## What Is Intentionally Unsafe?

For simplicity, we skip:
- TLS encryption
- Authentication
- Authorization
- Network partition handling (split-brain prevention)

**Do not use for production workloads.**

## Why Gossip for Membership?

- **Decentralized** - No single point of failure
- **Scalable** - O(log N) convergence
- **Robust** - Tolerates network partitions

## Why Tunable Consistency?

Demonstrates the CAP theorem trade-offs:
- `ANY` - Availability over consistency
- `QUORUM` - Balanced
- `ALL` - Consistency over availability
- `STRONG` - Primary-owner reads without a full linearizable consensus layer
