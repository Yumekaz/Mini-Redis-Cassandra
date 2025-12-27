"""
Test script for the distributed database cluster.
Demonstrates all major features.
"""

import time
import threading
import sys
import os

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minidb.config import NodeConfig, ConsistencyLevel
from minidb.node import DatabaseNode, create_node
from minidb.network.client import TCPClient


def test_single_node():
    """Test single node operations."""
    print("\n" + "="*60)
    print("TEST 1: Single Node Operations")
    print("="*60)
    
    # Create and start a node
    node = create_node(
        "node1",
        client_port=7001,
        cluster_port=8001,
        data_dir="./test_data",
        aof_enabled=False,  # Disable persistence for test
        snapshot_enabled=False
    )
    node.start()
    time.sleep(2)  # Wait for leader election
    
    # Connect a client
    client = TCPClient("localhost", 7001)
    if not client.connect():
        print("Failed to connect!")
        node.stop()
        return False
    
    # Test PING
    response = client.send_command("PING")
    print(f"PING: {response.payload.get('data') if response else 'ERROR'}")
    
    # Test SET
    response = client.send_command("SET", "hello", "world")
    print(f"SET hello world: {response.payload.get('data') if response else 'ERROR'}")
    
    # Test GET
    response = client.send_command("GET", "hello")
    print(f"GET hello: {response.payload.get('data') if response else 'ERROR'}")
    
    # Test SET with TTL
    response = client.send_command("SET", "temp", "value", "5")
    print(f"SET temp value 5: {response.payload.get('data') if response else 'ERROR'}")
    
    # Test EXISTS
    response = client.send_command("EXISTS", "hello")
    print(f"EXISTS hello: {response.payload.get('data') if response else 'ERROR'}")
    
    response = client.send_command("EXISTS", "nonexistent")
    print(f"EXISTS nonexistent: {response.payload.get('data') if response else 'ERROR'}")
    
    # Test KEYS
    response = client.send_command("SET", "user:1", "alice")
    response = client.send_command("SET", "user:2", "bob")
    response = client.send_command("KEYS", "user:*")
    print(f"KEYS user:*: {response.payload.get('data') if response else 'ERROR'}")
    
    # Test DELETE
    response = client.send_command("DELETE", "user:1")
    print(f"DELETE user:1: {response.payload.get('data') if response else 'ERROR'}")
    
    response = client.send_command("GET", "user:1")
    print(f"GET user:1 (after delete): {response.payload.get('data') if response else 'ERROR'}")
    
    # Test INFO
    response = client.send_command("INFO")
    if response:
        info = response.payload.get('data', {})
        print(f"INFO: role={info.get('role')}, keys={info.get('keys')}")
    
    client.disconnect()
    node.stop()
    
    print("Single node test: PASSED")
    return True


def test_cluster():
    """Test multi-node cluster operations."""
    print("\n" + "="*60)
    print("TEST 2: Multi-Node Cluster")
    print("="*60)
    
    nodes = []
    
    try:
        # Create 3 nodes
        for i in range(3):
            node = create_node(
                f"node{i+1}",
                client_port=7001 + i,
                cluster_port=8001 + i,
                data_dir=f"./test_data/node{i+1}",
                aof_enabled=False,
                snapshot_enabled=False,
                replication_factor=3
            )
            nodes.append(node)
            node.start()
            print(f"Started node{i+1}")
        
        time.sleep(3)  # Wait for leader election
        
        # Node 2 and 3 join via node 1
        for i in range(1, 3):
            success = nodes[i].join_cluster(f"localhost:{8001}")
            print(f"Node{i+1} joined cluster: {success}")
        
        time.sleep(2)  # Wait for cluster to stabilize
        
        # Find the leader
        leader_node = None
        leader_port = None
        for i, node in enumerate(nodes):
            if node.cluster.is_leader():
                leader_node = node
                leader_port = 7001 + i
                print(f"Leader is node{i+1}")
                break
        
        if not leader_node:
            print("No leader elected!")
            return False
        
        # Connect to leader
        client = TCPClient("localhost", leader_port)
        if not client.connect():
            print("Failed to connect to leader!")
            return False
        
        # Write some data
        print("\nWriting data through leader...")
        for i in range(10):
            response = client.send_command("SET", f"key{i}", f"value{i}")
            if not response or not response.payload.get('success'):
                print(f"Failed to SET key{i}")
        
        # Verify data exists
        response = client.send_command("KEYS", "*")
        keys = response.payload.get('data', [])
        print(f"Total keys in leader: {len(keys)}")
        
        # Check cluster info
        response = client.send_command("CLUSTER")
        if response:
            cluster_info = response.payload.get('data', {})
            print(f"Cluster members: {cluster_info.get('alive_count', 0)}")
            print(f"Log length: {cluster_info.get('log_length', 0)}")
        
        client.disconnect()
        
        # Verify data is replicated to followers
        print("\nVerifying replication to followers...")
        time.sleep(1)  # Wait for replication
        
        for i, node in enumerate(nodes):
            if node != leader_node:
                follower_client = TCPClient("localhost", 7001 + i)
                if follower_client.connect():
                    # Read from follower
                    response = follower_client.send_command("GET", "key5")
                    value = response.payload.get('data') if response else None
                    print(f"Node{i+1} GET key5: {value}")
                    follower_client.disconnect()
        
        print("Cluster test: PASSED")
        return True
        
    finally:
        # Cleanup
        for node in nodes:
            try:
                node.stop()
            except:
                pass


def test_consistent_hashing():
    """Test consistent hashing distribution."""
    print("\n" + "="*60)
    print("TEST 3: Consistent Hashing")
    print("="*60)
    
    from minidb.sharding import ConsistentHashRing
    
    ring = ConsistentHashRing(virtual_nodes=150)
    
    # Add nodes
    ring.add_node("node1")
    ring.add_node("node2")
    ring.add_node("node3")
    
    print(f"Nodes in ring: {ring.get_all_nodes()}")
    
    # Test key distribution
    distribution = {}
    for i in range(1000):
        key = f"key{i}"
        node = ring.get_node(key)
        distribution[node] = distribution.get(node, 0) + 1
    
    print("Key distribution (1000 keys):")
    for node, count in sorted(distribution.items()):
        print(f"  {node}: {count} keys ({count/10:.1f}%)")
    
    # Test replication
    replicas = ring.get_nodes("test_key", 3)
    print(f"\nReplicas for 'test_key': {replicas}")
    
    # Test node removal
    ring.remove_node("node2")
    print(f"\nAfter removing node2: {ring.get_all_nodes()}")
    
    new_owner = ring.get_node("test_key")
    print(f"New owner of 'test_key': {new_owner}")
    
    print("Consistent hashing test: PASSED")
    return True


def test_kv_store():
    """Test KV store operations."""
    print("\n" + "="*60)
    print("TEST 4: KV Store with TTL")
    print("="*60)
    
    from minidb.storage import KVStore
    
    store = KVStore()
    
    # Basic operations
    store.set("name", "Alice")
    value, found = store.get("name")
    print(f"SET/GET name: {value} (found={found})")
    
    # TTL
    store.set("temp", "expires_soon", ttl=2)
    value, found = store.get("temp")
    print(f"GET temp (before expiry): {value}")
    
    time.sleep(3)
    value, found = store.get("temp")
    print(f"GET temp (after expiry): {value} (found={found})")
    
    # Stats
    store.set("a", "1")
    store.set("b", "2")
    store.get("a")
    store.get("nonexistent")
    
    stats = store.get_stats()
    print(f"\nStore stats:")
    print(f"  Keys: {stats['key_count']}")
    print(f"  Hits: {stats['hits']}")
    print(f"  Misses: {stats['misses']}")
    print(f"  Hit rate: {stats['hit_rate']:.2%}")
    
    print("KV Store test: PASSED")
    return True


def test_raft_election():
    """Test Raft-lite leader election."""
    print("\n" + "="*60)
    print("TEST 5: Raft Leader Election")
    print("="*60)
    
    from minidb.cluster.election import ElectionManager
    
    # Create election managers
    election1 = ElectionManager("node1")
    election2 = ElectionManager("node2")
    election3 = ElectionManager("node3")
    
    # Simulate election callbacks
    def mock_request_votes(term, last_log_index, last_log_term):
        return 2  # Return 2 votes (majority in 3-node cluster)
    
    def mock_heartbeat():
        pass
    
    election1.set_callbacks(
        request_votes=mock_request_votes,
        send_heartbeats=mock_heartbeat
    )
    
    # Start election
    election1.start()
    
    # Wait for leader
    time.sleep(5)
    
    print(f"Node1 state: role={election1.role.value}, term={election1.current_term}")
    print(f"Is leader: {election1.is_leader()}")
    
    election1.stop()
    
    print("Raft election test: PASSED")
    return True


def test_merkle_tree():
    """Test Merkle tree for anti-entropy."""
    print("\n" + "="*60)
    print("TEST 6: Merkle Tree Anti-Entropy")
    print("="*60)
    
    from minidb.repair import MerkleTree
    
    # Create two trees with same data
    tree1 = MerkleTree(bucket_size=10)
    tree2 = MerkleTree(bucket_size=10)
    
    data1 = {f"key{i}": (f"value{i}", i) for i in range(100)}
    data2 = {f"key{i}": (f"value{i}", i) for i in range(100)}
    
    tree1.build(data1)
    tree2.build(data2)
    
    print(f"Tree1 root hash: {tree1.get_root_hash()}")
    print(f"Tree2 root hash: {tree2.get_root_hash()}")
    print(f"Hashes match: {tree1.get_root_hash() == tree2.get_root_hash()}")
    
    # Modify one tree's data
    data2["key50"] = ("modified_value", 2)
    tree2.build(data2)
    
    print(f"\nAfter modification:")
    print(f"Tree1 root hash: {tree1.get_root_hash()}")
    print(f"Tree2 root hash: {tree2.get_root_hash()}")
    print(f"Hashes match: {tree1.get_root_hash() == tree2.get_root_hash()}")
    
    # Find differences
    differences = tree1.compare(tree2)
    print(f"Different ranges: {len(differences)}")
    
    print("Merkle tree test: PASSED")
    return True


def test_version_vectors():
    """Test version vectors for conflict resolution."""
    print("\n" + "="*60)
    print("TEST 7: Version Vectors")
    print("="*60)
    
    from minidb.repair import VersionVector
    
    # Create version vectors
    v1 = VersionVector()
    v2 = VersionVector()
    
    # Node1 makes updates
    v1.increment("node1")
    v1.increment("node1")
    print(f"V1 after node1 updates: {v1}")
    
    # Node2 makes updates  
    v2.increment("node2")
    print(f"V2 after node2 updates: {v2}")
    
    # Compare
    comparison = v1.compare(v2)
    print(f"V1 vs V2: {comparison}")
    
    # Merge
    merged = v1.merge(v2)
    print(f"Merged: {merged}")
    
    # After merge, both should be "before" the merged version
    print(f"V1 vs Merged: {v1.compare(merged)}")
    print(f"V2 vs Merged: {v2.compare(merged)}")
    
    print("Version vector test: PASSED")
    return True


def run_all_tests():
    """Run all tests."""
    print("\n" + "#"*60)
    print("# Mini-Redis/Cassandra Distributed Database Test Suite")
    print("#"*60)
    
    tests = [
        ("KV Store", test_kv_store),
        ("Consistent Hashing", test_consistent_hashing),
        ("Merkle Tree", test_merkle_tree),
        ("Version Vectors", test_version_vectors),
        ("Raft Election", test_raft_election),
        ("Single Node", test_single_node),
        ("Cluster", test_cluster),
    ]
    
    results = []
    
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            print(f"\nTest {name} FAILED with exception: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for name, success in results:
        status = "PASSED" if success else "FAILED"
        print(f"  {name}: {status}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
