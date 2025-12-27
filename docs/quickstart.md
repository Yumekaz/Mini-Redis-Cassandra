# Quick Start Guide

Get Mini-Redis/Cassandra running in under 5 minutes.

## Prerequisites

- Python 3.7 or higher
- No external dependencies required

## Starting a Cluster

### Option 1: Using the Control Scripts

```bash
./run.sh cluster      # Linux/macOS
.\run_dbms.bat        # Windows (select option [2])
```

### Option 2: Manual Startup

Open 3 terminal windows:

**Terminal 1 - Node 1 (Leader)**
```bash
python -m minidb.main --node-id node1 --port 7001 --cluster-port 8001 --data-dir ./data/node1
```

**Terminal 2 - Node 2**
```bash
python -m minidb.main --node-id node2 --port 7002 --cluster-port 8002 --data-dir ./data/node2 --seed localhost:8001
```

**Terminal 3 - Node 3**
```bash
python -m minidb.main --node-id node3 --port 7003 --cluster-port 8003 --data-dir ./data/node3 --seed localhost:8001
```

## Connecting to the Cluster

Open a new terminal:

```bash
python -m minidb.cli localhost 7001
```

## Basic Operations

```
minidb:7001> SET user:1 "Alice"
OK

minidb:7001> GET user:1
"Alice"

minidb:7001> SET session:token "abc123" 60
OK    # Key expires in 60 seconds

minidb:7001> DELETE user:1
OK

minidb:7001> KEYS *
["session:token"]
```

## Check Cluster Status

```
minidb:7001> NODES
NODE ID    ADDRESS         ROLE      STATE
node1      localhost:7001  LEADER    ALIVE
node2      localhost:7002  FOLLOWER  ALIVE
node3      localhost:7003  FOLLOWER  ALIVE

minidb:7001> LEADER
Current leader: node1
```

## Consistency Levels

```
minidb:7001> CONSISTENCY QUORUM   # Balance of speed and consistency
minidb:7001> GET mykey            # Reads from majority

minidb:7001> CONSISTENCY ANY      # Fastest, may be stale
minidb:7001> GET mykey            # Reads from local node only

minidb:7001> CONSISTENCY STRONG   # Guaranteed latest
minidb:7001> GET mykey            # Reads from leader
```

## Next Steps

- [CLI Commands Reference](cli-commands.md)
- [Consistency Models](consistency-models.md)
- [Architecture Overview](ARCHITECTURE.md)
