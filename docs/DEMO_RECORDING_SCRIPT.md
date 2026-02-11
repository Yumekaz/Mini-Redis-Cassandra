# 🎬 Mini-Redis/Cassandra Demo Recording Script

> **Target Duration:** 90-120 seconds  
> **Purpose:** Demonstrate distributed systems expertise for LinkedIn/README/Interviews  
> **Platform:** Windows/Linux/macOS (Python 3.7+)

---

## Pre-Recording Checklist

```bash
# 1. Navigate to project directory
cd /path/to/Mini-Redis-Cassandra

# 2. Clear terminal for clean recording
clear  # Linux/Mac
cls    # Windows

# 3. Ensure no old cluster is running (kill any python processes using ports 7001-7003)

# 4. Clean any leftover data directories
rm -rf ./data/  # Linux/Mac
rmdir /s /q .\data\  # Windows
```

---

## 🎥 Recording Script (Copy-Paste Ready)

### Scene 1: Cluster Formation & Leader Election (25 seconds)

**What this proves:** Gossip protocol, Raft-lite consensus, distributed coordination

**Terminal 1 (Leave running):**
```bash
python -m minidb.main --node-id node1 --port 7001 --cluster-port 8001 --data-dir ./data/node1
```

**Terminal 2 (Leave running):**
```bash
python -m minidb.main --node-id node2 --port 7002 --cluster-port 8002 --data-dir ./data/node2 --seed localhost:8001
```

**Terminal 3 (Leave running):**
```bash
python -m minidb.main --node-id node3 --port 7003 --cluster-port 8003 --data-dir ./data/node3 --seed localhost:8001
```

**Terminal 4 - CLI (wait ~3 seconds for election):**
```bash
python -m minidb.cli localhost 7001
```

**Inside CLI - Prove cluster formation:**
```
NODES
```

**Expected Output:**
```
NODE ID    ADDRESS         ROLE      STATE
node1      localhost:7001  LEADER    ALIVE
node2      localhost:7002  FOLLOWER  ALIVE
node3      localhost:7003  FOLLOWER  ALIVE
```

**What recruiters learn:** You implemented gossip-based cluster discovery and Raft-lite leader election.

---

### Scene 2: Consistent Hashing & Sharding (20 seconds)

**What this proves:** Data partitioning, replica placement, consistent hashing with virtual nodes

**In CLI:**
```
SET user:alice "{'name': 'Alice', 'score': 100}"
SET user:bob "{'name': 'Bob', 'score': 85}"
SET config:app "production"
```

**Check routing and replicas:**
```
ROUTE user:alice
REPLICAS user:alice
RING 5
```

**Expected Output:**
```
Primary: node2, Replicas: [node3, node1], Hash: 0x7f2a...
```

**What recruiters learn:** You understand consistent hashing and how Cassandra/Redis distribute data.

---

### Scene 3: Tunable Consistency Levels (25 seconds) ⭐ DIFFERENTIATOR

**What this proves:** CAP theorem understanding, consistency vs availability tradeoffs

**In CLI:**
```
# Strong consistency - read from leader only (linearizable)
GET user:alice STRONG

# Quorum consistency - majority must agree
GET user:alice QUORUM

# Any consistency - fastest, read from any replica
GET user:alice ANY
```

**Show timing difference:**
```
DEBUG on
GET user:alice STRONG
GET user:alice ANY
DEBUG off
```

**Expected Output:**
```
STRONG: "{'name': 'Alice', 'score': 100}" (1.2ms)
ANY: "{'name': 'Alice', 'score': 100}" (0.3ms)
```

**What recruiters learn:** You understand tunable consistency (like Cassandra's CL.QUORUM) and can explain tradeoffs.

---

### Scene 4: Fault Tolerance & Elections (30 seconds) ⭐ DIFFERENTIATOR

**What this proves:** Automatic failover, leader re-election, cluster resilience

**Step 1 - Kill the leader (Ctrl+C in Terminal 1 running node1)**

**Step 2 - Watch election happen in CLI:**
```
LEADER
```
(May show "No leader" briefly, then a new leader)

**Step 3 - Wait 3-5 seconds and check again:**
```
LEADER
NODES
```

**Expected Output:**
```
Current leader: node2

NODE ID    ADDRESS         ROLE      STATE
node1      localhost:7001  FOLLOWER  DEAD
node2      localhost:7002  LEADER    ALIVE
node3      localhost:7003  FOLLOWER  ALIVE
```

**Step 4 - Data is still available!**
```
GET user:alice
```

**Expected Output:**
```
"{'name': 'Alice', 'score': 100}"
```

**What recruiters learn:** You implemented automatic leader election and data survives node failures.

---

### Scene 5: Chaos Engineering & Fault Injection (20 seconds) ⭐ BONUS

**What this proves:** Production-grade testing mindset, Netflix-style chaos engineering

**In CLI:**
```
# Enable fault injection
FAULT ENABLE

# Simulate network latency
FAULT DELAY 100

# Simulate network partition
FAULT PARTITION node3

# Check active faults
FAULT LIST

# Clear all faults
FAULT CLEAR
FAULT DISABLE
```

**Expected Output:**
```
Fault injection: ENABLED
Network delay: 100ms
Node node3: PARTITIONED
Active faults: delay=100ms, partitions=[node3]
Faults cleared
```

**What recruiters learn:** You think about failure modes and built chaos testing into your system.

---

## 🎯 One-Shot Recording Script (All-in-One)

For a single continuous recording, use the Windows batch file:

```batch
.\run_dbms.bat
# Select [1] for Automated Showcase Demo
```

Or run the Python script directly:
```bash
python examples/failure_demo.py
```

This runs an automated demo covering all 12 features with colored output!

---

## 📝 Recording Tips

### Terminal Recorder Options

1. **[asciinema](https://asciinema.org/)** - Best for LinkedIn (embeddable terminal recordings)
   ```bash
   asciinema rec demo.cast
   # ... run the demo ...
   # Press Ctrl+D to stop
   asciinema upload demo.cast
   ```

2. **[Terminalizer](https://terminalizer.com/)** - GIF output for README
   ```bash
   terminalizer record demo
   terminalizer render demo
   ```

3. **[OBS Studio](https://obsproject.com/)** - Full video with editing

### Terminal Setup

```bash
# Increase font size for visibility (16-20pt recommended)
# Use a dark theme with good contrast
# Clean prompt for clarity
export PS1='$ '   # Linux/Mac
# Or use Windows Terminal with custom settings
```

### Recording Flow

1. Practice the script 2-3 times before recording
2. Keep typing at a natural, readable pace
3. Pause 1-2 seconds after each command to let viewers read output
4. Speed up in post with 1.5x-2x if needed
5. Total final video: 90-120 seconds

---

## 🏆 What Makes This Demo Stand Out

| Shown | What Recruiters Learn |
|-------|----------------------|
| `NODES` shows 3-node cluster | You built gossip-based cluster discovery |
| Leader election in ~2-3s | You implemented Raft-lite consensus |
| `ROUTE`/`REPLICAS` commands | You understand consistent hashing (Cassandra-style) |
| `GET ... STRONG` vs `GET ... ANY` | You understand CAP theorem & tunable consistency |
| Node kill → new leader | You built automatic failover |
| `FAULT PARTITION nodeX` | You implemented chaos engineering tools |

**Most distributed systems projects don't show:**
- Tunable consistency levels
- Automatic leader election
- Chaos engineering/fault injection

This is your **differentiator** from typical CRUD demos.

---

## 📋 README Placement

After recording, structure your README like this:

```markdown
# 🗄️ Mini-Redis/Cassandra

> A distributed, fault-tolerant, in-memory key-value database implementing
> Raft consensus, consistent hashing, and tunable consistency levels.

![Demo](link-to-your-gif-or-asciinema-embed)

▶️ [Full terminal recording](https://asciinema.org/a/your-recording-id)

## Key Features Demonstrated
- ✅ 3-node cluster with gossip discovery
- ✅ Automatic leader election (~2.4s failover)
- ✅ Tunable consistency (ANY/QUORUM/ALL/STRONG)
- ✅ Consistent hashing with 150 virtual nodes
- ✅ Fault injection & chaos testing
```

---

## 🎤 Interview Talking Points

When presenting this demo, be ready to explain:

1. **"Why did you build this?"**
   - "To understand how production databases like Redis and Cassandra work internally."

2. **"What's the hardest part?"**
   - "Leader election edge cases - handling split-brain scenarios and ensuring exactly one leader."

3. **"How does consistency work?"**
   - "QUORUM reads require N/2+1 nodes to agree, balancing availability and consistency per CAP theorem."

4. **"What would you do differently?"**
   - "In production, I'd use proper Raft with persistent log, add CRC for network corruption detection."

5. **"How does this compare to Redis?"**
   - "Redis Cluster uses hash slots, we use Cassandra-style consistent hashing. We're ~25x slower due to Python, but the algorithms are conceptually similar."

---

## 📊 Quick Commands Reference

| Command | Purpose |
|---------|---------|
| `NODES` | Show cluster topology |
| `LEADER` | Show current leader |
| `SET k v` | Store a key-value |
| `GET k [STRONG\|QUORUM\|ANY]` | Read with consistency level |
| `ROUTE k` | Show which node owns key |
| `REPLICAS k` | Show replica locations |
| `RING` | View hash ring |
| `FAULT ENABLE/DELAY/PARTITION/CLEAR` | Chaos testing |
| `FAILOVER` | Force new election |

---

**Good luck with the recording! 🎬**
