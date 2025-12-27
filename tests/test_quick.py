#!/usr/bin/env python3
"""
Quick validation test to ensure all components work correctly.
Run this before the showcase to verify the system is functional.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_imports():
    """Test all imports work."""
    print("Testing imports...")
    
    from minidb.config import NodeConfig, ConsistencyLevel, NodeRole
    from minidb.storage import KVStore, AOFPersistence, SnapshotPersistence
    from minidb.network import TCPServer, TCPClient, Protocol, Message, MessageType
    from minidb.cluster import (
        ClusterCoordinator, NodeInfo, ElectionManager, 
        ReplicationManager, MembershipManager, ReadCoordinator
    )
    from minidb.sharding import (
        ConsistentHashRing, PartitionManager, 
        ShardMigrationManager, ShardRouter
    )
    from minidb.repair import MerkleTree, VersionVector, AntiEntropyManager
    from minidb.chaos import (
        FaultInjector, FaultType, Fault,
        TokenBucketLimiter, SlidingWindowLimiter, 
        AdaptiveRateLimiter, BackpressureManager
    )
    from minidb.node import DatabaseNode, create_node
    from minidb.cli import DatabaseCLI
    
    print("  All imports OK!")
    return True


def test_kv_store():
    """Test KV store operations."""
    print("Testing KV store...")
    
    from minidb.storage import KVStore
    
    store = KVStore()
    
    # SET
    assert store.set("key1", "value1")
    
    # GET
    value, found = store.get("key1")
    assert found
    assert value == "value1"
    
    # DELETE
    assert store.delete("key1")
    _, found = store.get("key1")
    assert not found
    
    # TTL
    import time
    store.set("temp", "value", ttl=1)
    _, found = store.get("temp")
    assert found
    time.sleep(1.5)
    _, found = store.get("temp")
    assert not found
    
    print("  KV store OK!")
    return True


def test_consistent_hashing():
    """Test consistent hashing."""
    print("Testing consistent hashing...")
    
    from minidb.sharding import ConsistentHashRing
    
    ring = ConsistentHashRing(virtual_nodes=10)
    ring.add_node("node1")
    ring.add_node("node2")
    ring.add_node("node3")
    
    # Should distribute keys
    nodes_used = set()
    for i in range(100):
        node = ring.get_node(f"key{i}")
        nodes_used.add(node)
    
    assert len(nodes_used) >= 2  # Should use at least 2 nodes
    
    # Replicas should be different nodes
    replicas = ring.get_nodes("test", 3)
    assert len(replicas) == 3
    assert len(set(replicas)) == 3
    
    print("  Consistent hashing OK!")
    return True


def test_merkle_tree():
    """Test Merkle tree."""
    print("Testing Merkle tree...")
    
    from minidb.repair import MerkleTree
    
    tree1 = MerkleTree()
    tree2 = MerkleTree()
    
    data = {f"key{i}": (f"value{i}", i) for i in range(100)}
    
    tree1.build(data)
    tree2.build(data)
    
    # Same data should have same hash
    assert tree1.get_root_hash() == tree2.get_root_hash()
    
    # Different data should have different hash
    data["key50"] = ("modified", 999)
    tree2.build(data)
    assert tree1.get_root_hash() != tree2.get_root_hash()
    
    print("  Merkle tree OK!")
    return True


def test_version_vectors():
    """Test version vectors."""
    print("Testing version vectors...")
    
    from minidb.repair import VersionVector
    
    v1 = VersionVector()
    v2 = VersionVector()
    
    v1.increment("node1")
    v2.increment("node2")
    
    # Concurrent updates
    assert v1.compare(v2) == "concurrent"
    
    # Merge resolves conflict
    merged = v1.merge(v2)
    assert v1.compare(merged) == "before"
    assert v2.compare(merged) == "before"
    
    print("  Version vectors OK!")
    return True


def test_rate_limiter():
    """Test rate limiting."""
    print("Testing rate limiter...")
    
    from minidb.chaos import TokenBucketLimiter
    
    limiter = TokenBucketLimiter(rate=10, burst=5)
    
    # Should allow burst
    allowed = sum(1 for _ in range(10) if limiter.try_acquire())
    assert allowed == 5  # Only burst amount
    
    print("  Rate limiter OK!")
    return True


def run_tests():
    """Run all tests."""
    print("=" * 50)
    print("Mini-Redis/Cassandra Quick Validation Tests")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_kv_store,
        test_consistent_hashing,
        test_merkle_tree,
        test_version_vectors,
        test_rate_limiter,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  FAILED: {e}")
            failed += 1
    
    print("=" * 50)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 50)
    
    if failed == 0:
        print("\n[PASS] All tests passed! Ready for showcase demo.")
        return True
    else:
        print("\n[FAIL] Some tests failed. Please check the errors.")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
