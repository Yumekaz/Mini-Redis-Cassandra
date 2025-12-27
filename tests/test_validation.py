#!/usr/bin/env python3
"""
Mini-Redis/Cassandra Project Validator
Checks all imports, module loading, and basic functionality.
Run this to verify the project is correctly set up.
"""

import sys
import os
import traceback

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_section(name):
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print('='*60)

def success(msg):
    print(f"  [OK] {msg}")

def fail(msg, error=None):
    print(f"  [FAIL] {msg}")
    if error:
        print(f"        Error: {error}")
    return False

def validate_imports():
    """Test all module imports"""
    test_section("Module Imports")
    all_passed = True
    
    # Core modules
    modules = [
        ("minidb", "Main package"),
        ("minidb.config", "Configuration"),
        ("minidb.node", "Node"),
        ("minidb.cli", "CLI"),
        ("minidb.main", "Main entry"),
    ]
    
    # Storage modules
    modules += [
        ("minidb.storage", "Storage package"),
        ("minidb.storage.kv_store", "KV Store"),
        ("minidb.storage.aof", "AOF Persistence"),
        ("minidb.storage.snapshot", "Snapshot"),
    ]
    
    # Network modules
    modules += [
        ("minidb.network", "Network package"),
        ("minidb.network.protocol", "Protocol"),
        ("minidb.network.server", "Server"),
        ("minidb.network.client", "Client"),
    ]
    
    # Cluster modules
    modules += [
        ("minidb.cluster", "Cluster package"),
        ("minidb.cluster.membership", "Membership"),
        ("minidb.cluster.election", "Election"),
        ("minidb.cluster.replication", "Replication"),
        ("minidb.cluster.coordinator", "Coordinator"),
        ("minidb.cluster.read_coordinator", "Read Coordinator"),
    ]
    
    # Sharding modules
    modules += [
        ("minidb.sharding", "Sharding package"),
        ("minidb.sharding.consistent_hash", "Consistent Hash"),
        ("minidb.sharding.migration", "Migration"),
    ]
    
    # Repair modules
    modules += [
        ("minidb.repair", "Repair package"),
        ("minidb.repair.anti_entropy", "Anti-Entropy"),
        ("minidb.repair.merkle", "Merkle Tree"),
        ("minidb.repair.version_vector", "Version Vector"),
    ]
    
    # Testing modules
    modules += [
        ("minidb.chaos", "Testing package"),
        ("minidb.chaos.fault_injection", "Fault Injection"),
        ("minidb.chaos.rate_limiter", "Rate Limiter"),
    ]
    
    for module_name, description in modules:
        try:
            __import__(module_name)
            success(f"{description} ({module_name})")
        except Exception as e:
            all_passed = fail(f"{description} ({module_name})", str(e))
    
    return all_passed

def validate_classes():
    """Test that key classes can be instantiated"""
    test_section("Class Instantiation")
    all_passed = True
    
    # Test KVStore
    try:
        from minidb.storage.kv_store import KVStore
        store = KVStore()
        store.set("test_key", "test_value")
        val, found = store.get("test_key")
        assert found is True, "Key not found"
        assert val == "test_value", f"Expected 'test_value', got '{val}'"
        store.delete("test_key")
        success("KVStore - set/get/delete working")
    except Exception as e:
        all_passed = fail("KVStore", str(e))
    
    # Test ConsistentHash
    try:
        from minidb.sharding.consistent_hash import ConsistentHashRing
        ch = ConsistentHashRing(virtual_nodes=3)
        ch.add_node("node1")
        ch.add_node("node2")
        node = ch.get_node("test_key")
        assert node in ["node1", "node2"], f"Unexpected node: {node}"
        success("ConsistentHashRing - add_node/get_node working")
    except Exception as e:
        all_passed = fail("ConsistentHashRing", str(e))
    
    # Test MerkleTree
    try:
        from minidb.repair.merkle import MerkleTree
        mt = MerkleTree()
        data = {
            "key1": ("value1", 1),
            "key2": ("value2", 1)
        }
        mt.build(data)
        root_hash = mt.get_root_hash()
        assert root_hash is not None, "Root hash is None"
        success("MerkleTree - insert/get_root_hash working")
    except Exception as e:
        all_passed = fail("MerkleTree", str(e))
    
    # Test FaultInjector
    try:
        from minidb.chaos.fault_injection import FaultInjector
        fi = FaultInjector("test_node")
        fi.enable()  # Enable injection
        success("FaultInjector - initialization working")
    except Exception as e:
        all_passed = fail("FaultInjector", str(e))
    
    # Test RateLimiter
    try:
        from minidb.chaos.rate_limiter import SlidingWindowLimiter, BackpressureManager
        rl = SlidingWindowLimiter(max_requests=100, window_seconds=1)
        allowed = rl.try_acquire()
        assert allowed == True, "Rate limiter should allow first request"
        success("RateLimiter - initialization working")
        
        bp = BackpressureManager(rate_limiter=rl)
        success("BackpressureManager - initialization working")
    except Exception as e:
        all_passed = fail("RateLimiter/BackpressureManager", str(e))
    
    # Test Election (with dataclass fix)
    try:
        from minidb.cluster.election import ElectionManager, LogEntry
        entry = LogEntry(term=1, index=0, command="SET x 1", key="x")
        assert entry.term == 1, "LogEntry term incorrect"
        success("Election LogEntry dataclass working")
    except Exception as e:
        all_passed = fail("Election LogEntry", str(e))
    
    return all_passed

def validate_config():
    """Test configuration loading"""
    test_section("Configuration")
    all_passed = True
    
    try:
        from minidb.config import NodeConfig
        config = NodeConfig(node_id="test")
        assert hasattr(config, 'host') or hasattr(config, 'HOST'), "Config missing host"
        success("NodeConfig loads correctly")
    except Exception as e:
        all_passed = fail("NodeConfig", str(e))
    
    return all_passed

def validate_protocol():
    """Test protocol encoding/decoding"""
    test_section("Protocol")
    all_passed = True
    
    try:
        from minidb.network.protocol import Protocol, Message, MessageType
        
        # Test encode/decode
        original_payload = {"cmd": "SET", "args": ["test", "123"]}
        msg = Message(msg_type=MessageType.COMMAND, payload=original_payload)
        encoded = msg.encode()
        decoded = Message.decode(encoded)
        
        assert decoded.payload == original_payload, f"Decode mismatch: {decoded.payload} != {original_payload}"
        assert decoded.msg_type == MessageType.COMMAND, "MessageType mismatch"
        success("Protocol encode/decode working")
    except Exception as e:
        all_passed = fail("Protocol", str(e))
    
    return all_passed

def main():
    print("\n" + "="*60)
    print("   Mini-Redis/Cassandra Distributed Database - Project Validator")
    print("="*60)
    
    results = []
    
    results.append(("Imports", validate_imports()))
    results.append(("Classes", validate_classes()))
    results.append(("Config", validate_config()))
    results.append(("Protocol", validate_protocol()))
    
    # Summary
    test_section("SUMMARY")
    all_passed = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "="*60)
    if all_passed:
        print("  ALL TESTS PASSED - Project is ready!")
        print("  ")
        print("  Next steps:")
        print("    python demo_cluster.py      # Run basic demo")
        print("    python failure_demo.py      # Run full demo")
    else:
        print("  SOME TESTS FAILED - Please fix the issues above")
    print("="*60 + "\n")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
