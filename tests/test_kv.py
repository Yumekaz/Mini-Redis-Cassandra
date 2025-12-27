"""
Basic Key-Value Store Tests
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minidb.storage import KVStore


def test_basic_operations():
    """Test basic SET/GET/DELETE operations."""
    store = KVStore()
    
    # Test SET and GET
    store.set("key1", "value1")
    value, found = store.get("key1")
    assert found, "Key should be found"
    assert value == "value1", "Value should match"
    
    # Test overwrite
    store.set("key1", "value2")
    value, found = store.get("key1")
    assert value == "value2", "Value should be updated"
    
    # Test DELETE
    store.delete("key1")
    value, found = store.get("key1")
    assert not found, "Key should be deleted"
    
    print("[OK] Basic operations test passed")


def test_exists():
    """Test EXISTS command."""
    store = KVStore()
    
    assert not store.exists("missing"), "Missing key should not exist"
    
    store.set("present", "value")
    assert store.exists("present"), "Present key should exist"
    
    print("[OK] EXISTS test passed")


def test_keys_pattern():
    """Test KEYS pattern matching."""
    store = KVStore()
    
    store.set("user:1", "Alice")
    store.set("user:2", "Bob")
    store.set("product:1", "Widget")
    
    all_keys = store.keys("*")
    assert len(all_keys) == 3, "Should have 3 keys"
    
    user_keys = store.keys("user:*")
    assert len(user_keys) == 2, "Should have 2 user keys"
    
    print("[OK] KEYS pattern test passed")


def test_ttl():
    """Test TTL expiration."""
    import time
    
    store = KVStore()
    
    # Set key with 1 second TTL
    store.set("temp", "value", ttl=1)
    
    # Should exist immediately
    value, found = store.get("temp")
    assert found, "Key should exist initially"
    
    # Wait for expiration
    time.sleep(1.5)
    store.cleanup_expired_keys()
    
    # Should be gone
    value, found = store.get("temp")
    assert not found, "Key should be expired"
    
    print("[OK] TTL test passed")


def test_stats():
    """Test statistics tracking."""
    store = KVStore()
    
    store.set("key", "value")
    store.get("key")  # Hit
    store.get("key")  # Hit
    store.get("missing")  # Miss
    
    stats = store.get_stats()
    assert stats["hits"] == 2, "Should have 2 hits"
    assert stats["misses"] == 1, "Should have 1 miss"
    
    print("[OK] Stats test passed")


if __name__ == "__main__":
    test_basic_operations()
    test_exists()
    test_keys_pattern()
    test_ttl()
    test_stats()
    print("\n=== All KV tests passed! ===")
