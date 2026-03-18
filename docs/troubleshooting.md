# Troubleshooting

Common issues and solutions.

---

## Connection Issues

### "Connection refused" when starting CLI

**Cause:** Node is not running on that port.

**Fix:**
```bash
# Verify node is running
python -m minidb.main --node-id node1 --port 7001 --cluster-port 8001 --data-dir ./data/node1

# Then connect
python -m minidb.cli localhost 7001
```

### "No leader available" error

**Cause:** Cluster hasn't completed leader election yet.

**Fix:** Wait 2-3 seconds after starting nodes, or check with `NODES` to see node states.

---

## Cluster Issues

### Nodes not discovering each other

**Cause:** Wrong seed address or port.

**Fix:** Ensure the `--seed` parameter uses the **cluster port** (8001), not the client port (7001):
```bash
# Correct
--seed localhost:8001

# Wrong
--seed localhost:7001
```

### Leader election keeps happening

**Cause:** Network instability or nodes timing out.

**Fix:** Check that all nodes are running and can communicate. Increase election timeout if needed.

---

## Data Issues

### "Primary owner unavailable" or replication failures

**Cause:** Writes are routed to the key's primary shard owner, and that owner could be unavailable or unable to reach enough replicas.

**Fix:** Check node health with `NODES`, confirm the cluster has formed, and retry once the owner and enough replicas are reachable.

### Data not persisting after restart

**Cause:** Data directory not specified or corrupted.

**Fix:**
```bash
# Always specify data directory
--data-dir ./data/node1
```

---

## Performance Issues

### Slow responses

**Possible causes:**
1. High consistency level (try `QUORUM` instead of `ALL`)
2. Network latency between nodes
3. Large number of keys

**Fix:**
```bash
# Check current consistency
minidb:7001> CONSISTENCY

# Use faster level for reads
minidb:7001> CONSISTENCY ANY
```

---

## Fault Injection Issues

### Commands not working after fault injection

**Cause:** Faults still active.

**Fix:**
```bash
minidb:7001> FAULT CLEAR
```

---

## Common Error Messages

| Error | Meaning | Fix |
|-------|---------|-----|
| `Rate limited` | Too many requests | Wait and retry |
| `Replication failed` | Followers unreachable | Check node health |
| `Unknown command` | Typo or unsupported | Check `HELP` |

---

## Getting Help

1. Check node logs in terminal
2. Use `INFO` and `STATS` commands
3. Review [Architecture](ARCHITECTURE.md)
