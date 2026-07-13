# Postmortem: FailForge MiniDB seed 42 read-after-write (Phase C)

**Date**: 2026-07-13  
**Scope**: Mini-Redis-Cassandra + FailForge closed loop (seed 42)  
**Non-goals**: multi-node production, MiniDB as Cairn store  

## Summary

FailForge seed **42** against a 3-node MiniDB cluster (`failforge_minidb.yml`, QUORUM) consistently failed the `read_after_acknowledged_write` checker with **~20–40 ERROR** violations. Minimization removed all process/network faults and still reproduced RAW on the happy path — proving a **correctness bug under concurrent client load**, not a fault-injection artifact.

After the initial Phase C fixes, seed 42 was **dramatically improved** but still showed intermittent **≤3 residual** corrupt-read ERRORs under restarts. A second residual-close pass (below) achieved **5/5 consecutive seed-42 full runs with 0 ERROR**.

## Symptoms

- Stale GET after an acknowledged PUT (older value or `null`).
- “Corrupt” GET: value never present in any successful PUT history.
- Proxy intercept count **0** (MiniDB uses cooperative `PROXY_PORT` TCP checks, not HTTP reverse-proxy bodies).
- Minimize: **0 faults**, 1 client, still RAW → happy-path bug class.

## Root causes

### 1. Write QUORUM + read ANY (primary)

- Cluster started with `--consistency QUORUM` (write path).
- `GET` without an explicit level defaulted to **`ConsistencyLevel.ANY`** (local replica).
- QUORUM writes only need majority (`RF//2+1`), so a lagging replica is legal; local GETs returned stale/`null` after acked writes.
- FailForge workload also issued bare `GET` without a consistency arg.

### 2. QUORUM success with zero remote replicas

- `ReplicationManager.replicate()` returned **True** when `targets` was empty (“no followers, always succeed”).
- Early cluster join left the ring with a single node; writes acked local-only, then other nodes served empty GETs.

### 3. Shared TCP connection pool interleaving (contributing)

- One pooled cluster TCP stream was shared across threads (gossip / election / replicate).
- Length-prefixed messages could interleave → rare phantom values not in client history.

### 4. Quorum-read failure reported as success+null

- Failed quorum assembly returned `success=true, data=null`, which FailForge scored as a stale null after an acked write.
- Now returns **error** when quorum cannot be formed.

## Fixes

| Area | Change |
|------|--------|
| `minidb/node.py` | Default GET consistency = `config.default_consistency`; ring not ready (`ring_size < RF`) refuses writes; quorum-read failure → error |
| `minidb/cluster/replication.py` | RF-based required acks; empty targets fail closed for QUORUM/ALL/STRONG when `RF > 1` |
| `minidb/cluster/read_coordinator.py` | Quorum/all return `(result, quorum_failed, key_absent)` |
| `minidb/network/client.py` | Thread-local connection pool |
| `minidb/cluster/coordinator.py` | Fresh TCP client per replicate send |
| `FAILFORGE/failforge_minidb.yml` | Portable `PYTHONPATH="${MINIDB_ROOT:-../Mini-Redis-Cassandra}"` |
| `FAILFORGE/internal/workload/generator.go` | MiniDB GET sends `STRONG` (pairs with ALL writes) |
| `FAILFORGE/failforge_minidb.yml` | `--consistency ALL` for FailForge nodes |
| `tests/test_quorum_raw.py` | Residual class: empty-target fail, never-acked invisible, single-leader serialize |

## Residual-close pass (same day)

### Residual root causes

1. **Hash-ring co-primaries** allowed two nodes to ACK concurrent SETs on the same key → later GETs saw values never recorded as successful (or stale).  
2. **Eager REPLICATE apply** made values durable on followers before the client received OK; under restart/kill the PUT failed in history while GET returned the value.  
3. **Partial REPLICATE when RF quorum was already impossible** left orphans; **STRONG** falling back to ring owner could read them.

### Residual fixes

| Area | Change |
| --- | --- |
| Write path | **Single election leader** coordinates all writes (`__LEADER_WRITE`); ring no longer co-primary for SET |
| Replication | **Prepare/commit**: REPLICATE `prepare` stages only; `commit` applies; no visible value without commit path |
| Replication | Fail closed **before send** if `len(targets) < required_acks` |
| Reads | STRONG = **write leader only** (no ring fallback); FailForge GET uses STRONG; writes use ALL |
| Pool / sockets | Thread-local pool + fresh TCP per replicate (from Phase C) |

### Verification (residual closed)

| Step | Result |
| --- | --- |
| Before seed 42 (Phase C start) | **23 ERROR** |
| After Phase C first pass | intermittent ≤3 residual |
| `pytest tests/test_quorum_raw.py` (+ partition repair) | **6 passed** |
| **5× consecutive** `failforge_minidb.yml --seed 42` (full fault profile) | **0 ERROR each** — rids `run-1783934884643222615` … `run-1783934946603752140` |

## Remaining limits (not residual RAW)

- FailForge HTTP proxy still shows **0 intercepts** for MiniDB (TCP cooperative `PROXY_PORT` path only).  
- Multi-node / production MiniDB still out of scope.

## Reproduction

```bash
cd /path/to/FAILFORGE
go build -o bin/failforge ./cmd/failforge
# sibling Mini-Redis-Cassandra checkout, or: export MINIDB_ROOT=/path/to/Mini-Redis-Cassandra
./bin/failforge run failforge_minidb.yml --seed 42
./bin/failforge minimize runs/minidb-42   # if FAILED
```
