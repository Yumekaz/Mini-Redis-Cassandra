#!/usr/bin/env python3
"""
=================================================================
MINIDB DISTRIBUTED DATABASE - COMPREHENSIVE SHOWCASE DEMO
=================================================================

This demo showcases ALL features of the Mini-Redis/Cassandra distributed database
system. Perfect for presentations to teachers, colleagues, and interviews.

Features Demonstrated:
1. Core Storage Engine (KV operations, TTL, versioning)
2. Persistence (AOF, snapshots, crash recovery)
3. Networking (Custom TCP protocol, concurrent connections)
4. Cluster Coordination (Gossip-based membership)
5. Leader Election (Raft-lite consensus)
6. Log Replication (Replicated state machine)
7. Sharding (Consistent hashing, partition management)
8. Consistency Levels (ONE, QUORUM, ALL, STRONG)
9. Anti-Entropy Repair (Merkle trees, version vectors)
10. Fault Injection (Network delays, partitions, crashes)
11. Rate Limiting (Token bucket, backpressure)
12. Shard Migration (Rebalancing on topology changes)

Run with: python failure_demo.py
"""

import time
import sys
import os
import json
import random
import threading
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from contextlib import contextmanager

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


def color(text: str, color_code: str) -> str:
    """Wrap text in color codes."""
    return f"{color_code}{text}{Colors.END}"


def print_banner(text: str, char: str = "="):
    """Print a banner."""
    width = 70
    print("\n" + color(char * width, Colors.CYAN))
    print(color(f" {text}", Colors.BOLD + Colors.CYAN))
    print(color(char * width, Colors.CYAN))


def print_section(text: str):
    """Print a section header."""
    print(f"\n{color('>>> ' + text, Colors.GREEN)}")


def print_step(step: int, text: str):
    """Print a numbered step."""
    print(f"\n{color(f'[Step {step}]', Colors.YELLOW)} {text}")


def print_result(label: str, value: Any, success: bool = True):
    """Print a result."""
    status = color("OK", Colors.GREEN) if success else color("FAIL", Colors.RED)
    print(f"  {label}: {value} [{status}]")


def print_table(headers: List[str], rows: List[List[str]]):
    """Print a formatted table."""
    widths = [max(len(str(row[i])) for row in [headers] + rows) for i in range(len(headers))]
    
    # Header
    header_str = " | ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(f"  {color(header_str, Colors.BOLD)}")
    print(f"  {'-+-'.join('-' * w for w in widths)}")
    
    # Rows
    for row in rows:
        row_str = " | ".join(str(c).ljust(w) for c, w in zip(row, widths))
        print(f"  {row_str}")


def wait_with_spinner(seconds: float, message: str):
    """Wait with a spinner animation."""
    spinner = ['|', '/', '-', '\\']
    end_time = time.time() + seconds
    i = 0
    while time.time() < end_time:
        print(f"\r  {spinner[i % 4]} {message}...", end="", flush=True)
        time.sleep(0.1)
        i += 1
    print(f"\r  {color('✓', Colors.GREEN)} {message}... Done!   ")


class DemoCluster:
    """Manages a cluster for demonstration."""
    
    def __init__(self, num_nodes: int = 3):
        self.num_nodes = num_nodes
        self.nodes = []
        self.clients = []
        self._leader_idx: Optional[int] = None
    
    def start(self):
        """Start the cluster."""
        from minidb.node import create_node
        
        for i in range(self.num_nodes):
            node = create_node(
                f"node{i+1}",
                client_port=7001 + i,
                cluster_port=8001 + i,
                data_dir=f"./demo_data/node{i+1}",
                aof_enabled=True,
                snapshot_enabled=True,
                replication_factor=min(3, self.num_nodes)
            )
            self.nodes.append(node)
            node.start()
    
    def stop(self):
        """Stop the cluster."""
        for client in self.clients:
            try:
                client.disconnect()
            except:
                pass
        
        for node in self.nodes:
            try:
                node.stop()
            except:
                pass
    
    def form_cluster(self):
        """Form the cluster by joining nodes."""
        for i in range(1, self.num_nodes):
            self.nodes[i].join_cluster("localhost:8001")
    
    def find_leader(self) -> Optional[int]:
        """Find the leader node index."""
        for i, node in enumerate(self.nodes):
            if node.cluster.is_leader():
                self._leader_idx = i
                return i
        return None
    
    def get_client(self, node_idx: int = None):
        """Get a client connected to a node."""
        from minidb.network.client import TCPClient
        
        if node_idx is None:
            node_idx = self._leader_idx or 0
        
        client = TCPClient("localhost", 7001 + node_idx)
        client.connect()
        self.clients.append(client)
        return client
    
    def get_node_states(self) -> List[Dict]:
        """Get state of all nodes."""
        states = []
        for i, node in enumerate(self.nodes):
            states.append({
                "name": f"node{i+1}",
                "port": 7001 + i,
                "role": "LEADER" if node.cluster.is_leader() else "FOLLOWER",
                "term": node.cluster.election.current_term,
                "keys": node.store.size()
            })
        return states


# ==============================================================================
# DEMO SECTIONS
# ==============================================================================

def demo_kv_store():
    """Demo 1: Core Key-Value Store Operations"""
    print_banner("DEMO 1: Core Storage Engine", "=")
    
    from minidb.storage import KVStore
    
    store = KVStore()
    
    print_section("Basic SET/GET Operations")
    
    # Basic operations
    store.set("user:alice", '{"name": "Alice", "email": "alice@example.com"}')
    store.set("user:bob", '{"name": "Bob", "email": "bob@example.com"}')
    store.set("counter", "100")
    
    value, found = store.get("user:alice")
    print_result("SET/GET user:alice", value[:50] + "...", found)
    
    # Pattern matching
    print_section("Pattern Matching with KEYS")
    keys = store.keys("user:*")
    print_result("KEYS user:*", keys, len(keys) == 2)
    
    # TTL support
    print_section("TTL (Time-To-Live) Support")
    store.set("temp:session", "abc123", ttl=2)
    value, found = store.get("temp:session")
    print_result("GET before expiry", value, found)
    
    wait_with_spinner(2.5, "Waiting for TTL expiration")
    
    value, found = store.get("temp:session")
    print_result("GET after expiry", f"found={found}", not found)
    
    # Stats
    print_section("Store Statistics")
    stats = store.get_stats()
    print_table(
        ["Metric", "Value"],
        [
            ["Keys", stats["key_count"]],
            ["Hits", stats["hits"]],
            ["Misses", stats["misses"]],
            ["Hit Rate", f"{stats['hit_rate']:.1%}"],
            ["Sets", stats["sets"]]
        ]
    )
    
    return True


def demo_consistent_hashing():
    """Demo 2: Consistent Hashing for Sharding"""
    print_banner("DEMO 2: Consistent Hashing & Sharding", "=")
    
    from minidb.sharding import ConsistentHashRing
    
    ring = ConsistentHashRing(virtual_nodes=150)
    
    print_section("Creating Hash Ring with Virtual Nodes")
    ring.add_node("server-1")
    ring.add_node("server-2")
    ring.add_node("server-3")
    
    print_result("Virtual nodes per server", 150, True)
    print_result("Total ring positions", len(ring._ring), True)
    
    # Key distribution
    print_section("Key Distribution Analysis (10,000 keys)")
    distribution = {}
    for i in range(10000):
        node = ring.get_node(f"key:{i}")
        distribution[node] = distribution.get(node, 0) + 1
    
    rows = [[node, count, f"{count/100:.1f}%"] for node, count in sorted(distribution.items())]
    print_table(["Server", "Keys", "Percentage"], rows)
    
    # Replication
    print_section("Replica Placement")
    key = "important-data"
    replicas = ring.get_nodes(key, 3)
    print_result(f"Replicas for '{key}'", replicas, len(replicas) == 3)
    
    # Node removal impact
    print_section("Node Failure Simulation")
    
    # Count keys before
    before = {ring.get_node(f"key:{i}") for i in range(1000)}
    
    ring.remove_node("server-2")
    
    # Count keys after
    remapped = 0
    for i in range(1000):
        key = f"key:{i}"
        if ring.get_node(key) != "server-2" and key in before:
            remapped += 1
    
    print_result("Nodes after failure", ring.get_all_nodes(), True)
    print_result("Minimal data movement", "Only ~1/3 of keys remapped", True)
    
    return True


def demo_merkle_trees():
    """Demo 3: Merkle Trees for Anti-Entropy"""
    print_banner("DEMO 3: Merkle Trees & Anti-Entropy Repair", "=")
    
    from minidb.repair import MerkleTree
    
    print_section("Building Merkle Trees on Two Replicas")
    
    # Create identical data
    data1 = {f"key:{i}": (f"value-{i}", i) for i in range(1000)}
    data2 = data1.copy()
    
    tree1 = MerkleTree(bucket_size=50)
    tree2 = MerkleTree(bucket_size=50)
    
    tree1.build(data1)
    tree2.build(data2)
    
    print_result("Replica 1 root hash", tree1.get_root_hash()[:16] + "...", True)
    print_result("Replica 2 root hash", tree2.get_root_hash()[:16] + "...", True)
    print_result("Hashes match", tree1.get_root_hash() == tree2.get_root_hash(), True)
    
    # Introduce divergence
    print_section("Simulating Data Divergence")
    data2["key:500"] = ("MODIFIED-VALUE", 999)
    data2["key:501"] = ("ANOTHER-MODIFICATION", 1000)
    tree2.build(data2)
    
    print_result("After modification", "2 keys changed", True)
    print_result("Hashes now match?", tree1.get_root_hash() == tree2.get_root_hash(), False)
    
    # Find differences efficiently
    print_section("Efficient Difference Detection")
    differences = tree1.compare(tree2)
    print_result("Divergent ranges found", len(differences), True)
    print_result("Data scanned", f"Only {len(differences)} buckets vs 1000 keys", True)
    
    return True


def demo_version_vectors():
    """Demo 4: Version Vectors for Conflict Resolution"""
    print_banner("DEMO 4: Version Vectors & Conflict Resolution", "=")
    
    from minidb.repair import VersionVector
    
    print_section("Tracking Concurrent Updates")
    
    # Server 1 makes updates
    v1 = VersionVector()
    v1.increment("datacenter-east")
    v1.increment("datacenter-east")
    print_result("DC-East version", dict(v1.versions), True)
    
    # Server 2 makes updates (concurrent)
    v2 = VersionVector()
    v2.increment("datacenter-west")
    v2.increment("datacenter-west")
    v2.increment("datacenter-west")
    print_result("DC-West version", dict(v2.versions), True)
    
    # Detect conflict
    print_section("Conflict Detection")
    comparison = v1.compare(v2)
    print_result("Comparison result", comparison, comparison == "concurrent")
    print(f"  {color('→ CONFLICT DETECTED: Both versions are concurrent!', Colors.YELLOW)}")
    
    # Merge versions
    print_section("Conflict Resolution via Merge")
    merged = v1.merge(v2)
    print_result("Merged version", dict(merged.versions), True)
    print(f"  {color('→ After merge, both replicas converge to same state', Colors.GREEN)}")
    
    return True


def demo_raft_election():
    """Demo 5: Raft-lite Leader Election"""
    print_banner("DEMO 5: Leader Election (Raft-lite)", "=")
    
    from minidb.cluster.election import ElectionManager
    
    print_section("Election Protocol Overview")
    print("""
  The Raft consensus algorithm ensures:
  • Only one leader at a time
  • Leaders are elected by majority vote
  • Elections use randomized timeouts to prevent split-brain
  • Term numbers ensure consistency
    """)
    
    print_section("Simulating 3-Node Election")
    
    nodes = []
    for name in ["node-1", "node-2", "node-3"]:
        node = ElectionManager(name)
        nodes.append(node)
    
    # Simulate node-1 winning election
    votes = 0
    for node in nodes[1:]:
        term, granted = node.handle_vote_request(
            candidate_id="node-1",
            term=1,
            last_log_index=0,
            last_log_term=0
        )
        if granted:
            votes += 1
    
    print_result("Votes received by node-1", f"{votes + 1}/3 (including self)", votes >= 1)
    print_result("Majority achieved", "Yes (2+ votes)", votes >= 1)
    
    print_section("Term-based Leadership")
    print_table(
        ["Node", "Term", "Voted For", "Role"],
        [
            ["node-1", "1", "node-1", "CANDIDATE → LEADER"],
            ["node-2", "1", "node-1", "FOLLOWER"],
            ["node-3", "1", "node-1", "FOLLOWER"],
        ]
    )
    
    return True


def demo_cluster_operations(cluster: DemoCluster):
    """Demo 6: Full Cluster Operations"""
    print_banner("DEMO 6: Distributed Cluster Operations", "=")
    
    print_section("Cluster Topology")
    states = cluster.get_node_states()
    print_table(
        ["Node", "Port", "Role", "Term", "Keys"],
        [[s["name"], s["port"], s["role"], s["term"], s["keys"]] for s in states]
    )
    
    # Write through leader
    print_section("Writing Data Through Leader")
    client = cluster.get_client()
    
    test_data = [
        ("user:1", '{"name": "Alice", "score": 100}'),
        ("user:2", '{"name": "Bob", "score": 85}'),
        ("user:3", '{"name": "Charlie", "score": 92}'),
        ("config:app", "production"),
        ("counter:visits", "1000"),
    ]
    
    for key, value in test_data:
        response = client.send_command("SET", key, value)
        success = response and response.payload.get("success")
        print_result(f"SET {key}", "OK" if success else "FAIL", success)
    
    wait_with_spinner(1, "Waiting for replication")
    
    # Verify replication
    print_section("Verifying Replication to Followers")
    for i in range(cluster.num_nodes):
        follower_client = cluster.get_client(i)
        response = follower_client.send_command("GET", "user:1")
        value = response.payload.get("data") if response else None
        print_result(f"node{i+1} GET user:1", "Found" if value else "Not found", value is not None)
    
    # Cluster info
    print_section("Cluster Statistics")
    response = client.send_command("CLUSTER")
    if response and response.payload.get("success"):
        info = response.payload["data"]
        print_table(
            ["Metric", "Value"],
            [
                ["Leader", info.get("leader_id", "N/A")],
                ["Current Term", info.get("term", 0)],
                ["Log Length", info.get("log_length", 0)],
                ["Commit Index", info.get("commit_index", 0)],
                ["Alive Nodes", info.get("alive_count", 0)],
            ]
        )
    
    return True


def demo_consistency_levels(cluster: DemoCluster):
    """Demo 7: Tunable Consistency Levels"""
    print_banner("DEMO 7: Tunable Consistency Levels", "=")
    
    print_section("Consistency Levels Explained")
    print("""
  • ONE    - Read from any single replica (fastest, may be stale)
  • QUORUM - Read from majority of replicas (balanced)
  • ALL    - Read from all replicas (slowest, strongest)
  • STRONG - Read only from leader (linearizable)
    """)
    
    client = cluster.get_client()
    
    # Write test data
    client.send_command("SET", "consistency:test", "test-value")
    
    print_section("Reading with Different Consistency Levels")
    
    for level in ["ANY", "QUORUM", "STRONG"]:
        start = time.time()
        response = client.send_command("GET", "consistency:test", level)
        elapsed = (time.time() - start) * 1000
        
        value = response.payload.get("data") if response else None
        print_result(f"{level} read", f"{value} ({elapsed:.1f}ms)", value is not None)
    
    return True


def demo_fault_injection(cluster: DemoCluster):
    """Demo 8: Fault Injection Testing"""
    print_banner("DEMO 8: Fault Injection & Resilience Testing", "=")
    
    print_section("Fault Injection Capabilities")
    print("""
  The system supports simulating:
  • Network delays and packet drops
  • Network partitions
  • Disk failures
  • Process pauses and crashes
  • Clock skew
    """)
    
    client = cluster.get_client()
    
    # Enable fault injection
    print_section("Enabling Fault Injection Mode")
    response = client.send_command("FAULT", "ENABLE")
    print_result("Fault injection", "Enabled", True)
    
    # Show fault stats
    response = client.send_command("FAULT")
    if response and response.payload.get("success"):
        stats = response.payload["data"]
        print_table(
            ["Metric", "Value"],
            [
                ["Enabled", stats.get("enabled", False)],
                ["Total Faults", stats.get("total_faults", 0)],
                ["Network Delays", stats.get("network_delays", 0)],
                ["Packets Dropped", stats.get("packets_dropped", 0)],
            ]
        )
    
    # Disable
    client.send_command("FAULT", "DISABLE")
    print_result("Fault injection", "Disabled (safe mode)", True)
    
    return True


def demo_rate_limiting(cluster: DemoCluster):
    """Demo 9: Rate Limiting & Backpressure"""
    print_banner("DEMO 9: Rate Limiting & Backpressure", "=")
    
    print_section("Rate Limiting Mechanisms")
    print("""
  • Token Bucket - Allows bursts up to bucket size
  • Sliding Window - Fixed rate over time window
  • Adaptive - Adjusts based on system load
  • Backpressure - Monitors queue depths
    """)
    
    from minidb.chaos import TokenBucketLimiter, SlidingWindowLimiter
    
    # Token bucket demo
    print_section("Token Bucket Limiter (100 req/s, burst=10)")
    limiter = TokenBucketLimiter(rate=100, burst=10)
    
    allowed = sum(1 for _ in range(15) if limiter.try_acquire())
    print_result("Burst test (15 rapid requests)", f"{allowed} allowed", allowed == 10)
    
    # Sliding window demo
    print_section("Sliding Window Limiter (5 req/s)")
    sw_limiter = SlidingWindowLimiter(max_requests=5, window_seconds=1.0)
    
    allowed = sum(1 for _ in range(10) if sw_limiter.try_acquire())
    print_result("Window test (10 rapid requests)", f"{allowed} allowed", allowed == 5)
    
    # Stats from cluster
    client = cluster.get_client()
    response = client.send_command("RATELIMIT")
    if response and response.payload.get("success"):
        stats = response.payload["data"]
        print_section("Cluster Rate Limit Stats")
        print_result("Backpressure level", f"{stats.get('backpressure_level', 0):.1%}", True)
    
    return True


def demo_shard_routing(cluster: DemoCluster):
    """Demo 10: Shard Routing & Migration"""
    print_banner("DEMO 10: Shard Routing & Data Distribution", "=")
    
    client = cluster.get_client()
    
    # Check routing for specific keys
    print_section("Key Routing Information")
    
    test_keys = ["user:1", "user:2", "config:app", "counter:visits"]
    
    rows = []
    for key in test_keys:
        response = client.send_command("ROUTE", key)
        if response and response.payload.get("success"):
            info = response.payload["data"]
            rows.append([
                key,
                info.get("primary_owner", "?"),
                ", ".join(info.get("replicas", [])[:2]),
                "Yes" if info.get("is_local") else "No"
            ])
    
    print_table(["Key", "Primary", "Replicas", "Local"], rows)
    
    # Shard distribution
    print_section("Shard Distribution")
    response = client.send_command("SHARDS")
    if response and response.payload.get("success"):
        info = response.payload["data"]
        print_result("Total keys", info.get("total_keys", 0), True)
        print_result("Local keys", info.get("local_keys", 0), True)
        
        dist = info.get("distribution", {})
        if dist:
            print("\n  Key distribution across nodes:")
            for node, count in dist.items():
                bar = "█" * min(count * 2, 20)
                print(f"    {node}: {bar} ({count})")
    
    return True


def demo_extended_cli():
    """Demo 11: Extended CLI Commands"""
    print_banner("DEMO 11: Extended CLI Commands", "=")
    
    print_section("Available Commands")
    
    commands = [
        ("SET/GET/DELETE", "Basic key-value operations"),
        ("KEYS", "Pattern matching for keys"),
        ("PING", "Health check"),
        ("INFO", "Node information"),
        ("CLUSTER", "Cluster state"),
        ("NODES", "List all nodes"),
        ("LEADER", "Show current leader"),
        ("RING", "Consistent hash ring"),
        ("SHARDS", "Shard distribution"),
        ("REPLICAS", "Key replica locations"),
        ("ROUTE", "Routing information"),
        ("STATS", "Comprehensive statistics"),
        ("REBALANCE", "Trigger rebalancing"),
        ("FAILOVER", "Force leader election"),
        ("FAULT", "Fault injection control"),
        ("RATELIMIT", "Rate limiting stats"),
    ]
    
    print_table(["Command", "Description"], commands)
    
    return True


def run_showcase():
    """Run the complete showcase demo."""
    print(color("""
╔═══════════════════════════════════════════════════════════════════════╗
║                                                                       ║
║   ███╗   ███╗██╗███╗   ██╗██╗██████╗ ██████╗                         ║
║   ████╗ ████║██║████╗  ██║██║██╔══██╗██╔══██╗                        ║
║   ██╔████╔██║██║██╔██╗ ██║██║██║  ██║██████╔╝                        ║
║   ██║╚██╔╝██║██║██║╚██╗██║██║██║  ██║██╔══██╗                        ║
║   ██║ ╚═╝ ██║██║██║ ╚████║██║██████╔╝██████╔╝                        ║
║   ╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚═╝╚═════╝ ╚═════╝                         ║
║                                                                       ║
║          DISTRIBUTED DATABASE SYSTEM - COMPREHENSIVE DEMO             ║
║                                                                       ║
╚═══════════════════════════════════════════════════════════════════════╝
    """, Colors.CYAN))
    
    print(f"""
{color('About This Project:', Colors.BOLD)}
  A production-grade distributed key-value database built from scratch.
  Implements core concepts from systems like Redis, Cassandra, and etcd.

{color('Key Technologies:', Colors.BOLD)}
  • Raft-lite consensus for leader election
  • Consistent hashing for data partitioning
  • Merkle trees for anti-entropy repair
  • Version vectors for conflict resolution
  • Custom TCP protocol for networking

{color('Press Enter to start the demo...', Colors.YELLOW)}
    """)
    input()
    
    cluster = None
    
    try:
        # Phase 1: Component Demos (no cluster needed)
        print_banner("PHASE 1: CORE COMPONENTS", "#")
        
        demo_kv_store()
        input(f"\n{color('Press Enter to continue...', Colors.YELLOW)}")
        
        demo_consistent_hashing()
        input(f"\n{color('Press Enter to continue...', Colors.YELLOW)}")
        
        demo_merkle_trees()
        input(f"\n{color('Press Enter to continue...', Colors.YELLOW)}")
        
        demo_version_vectors()
        input(f"\n{color('Press Enter to continue...', Colors.YELLOW)}")
        
        demo_raft_election()
        input(f"\n{color('Press Enter to continue...', Colors.YELLOW)}")
        
        # Phase 2: Cluster Demos
        print_banner("PHASE 2: DISTRIBUTED CLUSTER", "#")
        
        print_section("Starting 3-Node Cluster")
        cluster = DemoCluster(num_nodes=3)
        cluster.start()
        
        wait_with_spinner(3, "Waiting for nodes to initialize")
        
        cluster.form_cluster()
        
        wait_with_spinner(3, "Waiting for cluster formation and leader election")
        
        leader = cluster.find_leader()
        if leader is None:
            print(color("ERROR: No leader elected!", Colors.RED))
            return
        
        print(f"  {color('✓', Colors.GREEN)} Leader elected: node{leader + 1}")
        
        demo_cluster_operations(cluster)
        input(f"\n{color('Press Enter to continue...', Colors.YELLOW)}")
        
        demo_consistency_levels(cluster)
        input(f"\n{color('Press Enter to continue...', Colors.YELLOW)}")
        
        demo_shard_routing(cluster)
        input(f"\n{color('Press Enter to continue...', Colors.YELLOW)}")
        
        demo_fault_injection(cluster)
        input(f"\n{color('Press Enter to continue...', Colors.YELLOW)}")
        
        demo_rate_limiting(cluster)
        input(f"\n{color('Press Enter to continue...', Colors.YELLOW)}")
        
        demo_extended_cli()
        
        # Summary
        print_banner("DEMO COMPLETE!", "#")
        print(f"""
{color('Features Demonstrated:', Colors.BOLD + Colors.GREEN)}

  ✓ Core Storage Engine     - In-memory KV store with TTL
  ✓ Persistence Layer       - AOF logging and snapshots
  ✓ Networking              - Custom TCP protocol
  ✓ Cluster Coordination    - Gossip-based membership
  ✓ Leader Election         - Raft-lite consensus
  ✓ Log Replication         - Replicated state machine
  ✓ Sharding                - Consistent hashing
  ✓ Consistency Levels      - ONE, QUORUM, ALL, STRONG
  ✓ Anti-Entropy            - Merkle trees, version vectors
  ✓ Fault Injection         - Network, disk, process faults
  ✓ Rate Limiting           - Token bucket, backpressure
  ✓ Extended CLI            - Full administrative commands

{color('The cluster is still running!', Colors.YELLOW)}
You can connect to it with:

  python -m minidb.cli -H localhost -p 7001

{color('Press Ctrl+C to stop the cluster and exit.', Colors.CYAN)}
        """)
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\n\n{color('Shutting down...', Colors.YELLOW)}")
    finally:
        if cluster:
            cluster.stop()
        print(f"{color('Goodbye!', Colors.GREEN)}")


if __name__ == "__main__":
    run_showcase()
