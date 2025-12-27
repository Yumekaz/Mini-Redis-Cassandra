# CLI Commands Reference

Complete reference for all Mini-Redis/Cassandra CLI commands.

---

## Data Commands

### SET
Store a key-value pair.
```
SET <key> <value> [ttl_seconds]
```
**Examples:**
```
SET user:1 "Alice"
SET session:abc "data" 3600    # Expires in 1 hour
```

### SETEX
Store with explicit TTL (Redis-compatible syntax).
```
SETEX <key> <seconds> <value>
```
**Example:**
```
SETEX cache:user 60 "{\"name\":\"Alice\"}"
```

### GET
Retrieve a value with optional consistency level.
```
GET <key> [ANY|QUORUM|ALL|STRONG]
```
**Examples:**
```
GET user:1
GET user:1 QUORUM
GET user:1 STRONG
```

### DELETE / DEL
Remove a key.
```
DELETE <key>
DEL <key>
```

### EXISTS
Check if a key exists (returns 1 or 0).
```
EXISTS <key>
```

### KEYS
List keys matching a pattern.
```
KEYS <pattern>
```
**Example:**
```
KEYS user:*
KEYS *
```

---

## Cluster Commands

### NODES
List all cluster nodes.
```
NODES
```

### LEADER
Show the current leader.
```
LEADER
```

### CLUSTER
Show detailed cluster state.
```
CLUSTER
```

### INFO
Show node information and statistics.
```
INFO
```

### STATS
Show detailed statistics.
```
STATS
```

### PING
Health check.
```
PING
```
Returns: `PONG`

### FAILOVER
Force a new leader election.
```
FAILOVER
```

---

## Sharding Commands

### RING
View the consistent hash ring.
```
RING [samples]
```

### SHARDS
Show shard distribution across nodes.
```
SHARDS
```

### ROUTE
Show which node owns a key.
```
ROUTE <key>
```

### REPLICAS
Show replica nodes for a key.
```
REPLICAS <key>
```

### REBALANCE
Trigger cluster rebalancing.
```
REBALANCE
```

### MIGRATE
Show migration status.
```
MIGRATE STATUS
```

---

## Consistency Commands

### CONSISTENCY
Get or set default consistency level.
```
CONSISTENCY [ANY|QUORUM|ALL|STRONG]
```
**Examples:**
```
CONSISTENCY           # Show current
CONSISTENCY QUORUM    # Set default
```

---

## Fault Injection Commands

### FAULT ENABLE
Enable fault injection mode.
```
FAULT ENABLE
```

### FAULT DELAY
Add network latency.
```
FAULT DELAY <milliseconds>
```

### FAULT DROP
Randomly drop packets.
```
FAULT DROP <percentage>
```

### FAULT PARTITION
Isolate a node.
```
FAULT PARTITION <node_id>
```

### FAULT LIST
List active faults.
```
FAULT LIST
```

### FAULT CLEAR
Remove all faults.
```
FAULT CLEAR
```

### FAULT DISABLE
Disable fault injection.
```
FAULT DISABLE
```

---

## Rate Limiting

### RATELIMIT
Show rate limit statistics.
```
RATELIMIT
```

---

## CLI Utility Commands

### HELP
Show help.
```
HELP [command]
```

### DEBUG
Toggle debug mode (shows timing).
```
DEBUG on
DEBUG off
```

### QUIT / EXIT
Exit the CLI.
```
QUIT
EXIT
```
