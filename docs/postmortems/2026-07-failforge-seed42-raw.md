# Postmortem: FailForge MiniDB seed 42 read-after-write (Phase C)

**Date**: 2026-07-13  
**Scope**: Mini-Redis-Cassandra + FailForge closed loop (seed 42)  
**Non-goals**: multi-node production, MiniDB as Cairn store  

## Summary

FailForge seed **42** against a 3-node MiniDB cluster (`failforge_minidb.yml`, QUORUM) consistently failed the `read_after_acknowledged_write` checker with **~20–40 ERROR** violations. Minimization removed all process/network faults and still reproduced RAW on the happy path — proving a **correctness bug under concurrent client load**, not a fault-injection artifact.

After fixes, seed 42 is **dramatically improved** (often **0 ERROR**, intermittent **≤3 residual** corrupt-read ERRORs under restarts). Unit tests lock in the main QUORUM contracts.

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
| `FAILFORGE/internal/workload/generator.go` | MiniDB GET sends `QUORUM` explicitly |
| `tests/test_quorum_raw.py` | Unit/integration coverage for empty-target fail, default-QUORUM RAW, remote-ack requirement |

## Verification

| Step | Result |
|------|--------|
| Before seed 42 | **23 ERROR** (`failforge_seed42_before.log`) |
| Minimize | 0 faults, still RAW (`failforge_minimize.log`) |
| `pytest tests/test_quorum_raw.py` | **3 passed** |
| After seed 42 (full faults) | **0 ERROR** on run `run-1783930464762324993`; **3 residual ERROR** on verify re-run (corrupt-read under restarts) |
| Happy-path (no faults) | **3 residual ERROR** (corrupt + rare stale) |

## Honest residual

Not claimed fully closed:

1. **Intermittent corrupt reads** (value in store without a successful client PUT in FailForge history), especially around restarts / long replication waits — likely response-loss after apply or residual ownership races.
2. **Happy-path residual ≤3 ERROR** under 3 concurrent clients without faults — needs further ownership linearization or linearizable register semantics if zero residual is required.
3. FailForge HTTP proxy still shows **0 intercepts** for MiniDB (TCP cooperative path only). Network partition injection via proxy is weak for this adapter.

## Reproduction

```bash
cd /path/to/FAILFORGE
go build -o bin/failforge ./cmd/failforge
# sibling Mini-Redis-Cassandra checkout, or: export MINIDB_ROOT=/path/to/Mini-Redis-Cassandra
./bin/failforge run failforge_minidb.yml --seed 42
./bin/failforge minimize runs/minidb-42   # if FAILED
```

## Follow-ups (deferred)

- Single global write leader or fencing tokens for shard owners under membership churn.
- Two-phase replicate (prepare/commit) so failed writes never leave durable majority state.
- FailForge MiniDB adapter: wait-for-cluster-ready before workload; optional read deadline.
- Multi-node still deferred (portfolio stack strategy).
