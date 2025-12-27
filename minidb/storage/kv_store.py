"""
In-memory Key-Value Store with TTL support.
Thread-safe, O(1) operations.
"""

import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import json


@dataclass
class KeyMetadata:
    """Metadata for a stored key."""
    value: Any
    created_at: float
    updated_at: float
    expires_at: Optional[float] = None
    version: int = 1
    

@dataclass
class StoreStats:
    """Statistics for the KV store."""
    hits: int = 0
    misses: int = 0
    sets: int = 0
    deletes: int = 0
    expired: int = 0
    start_time: float = field(default_factory=time.time)
    
    def uptime(self) -> float:
        return time.time() - self.start_time
    
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0
    
    def to_dict(self) -> Dict:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "sets": self.sets,
            "deletes": self.deletes,
            "expired": self.expired,
            "uptime_seconds": self.uptime(),
            "hit_rate": self.hit_rate()
        }


class KVStore:
    """
    Thread-safe in-memory key-value store with TTL support.
    Provides O(1) operations for SET, GET, DELETE, EXISTS.
    """
    
    def __init__(self):
        self._store: Dict[str, KeyMetadata] = {}
        self._lock = threading.RLock()
        self._stats = StoreStats()
        self._ttl_index: Dict[float, List[str]] = defaultdict(list)  # expiry_time -> keys
        self._running = True
        
    def set(self, key: str, value: Any, ttl: Optional[int] = None, 
            version: Optional[int] = None) -> bool:
        """
        Set a key-value pair with optional TTL.
        
        Args:
            key: The key to set
            value: The value to store
            ttl: Time-to-live in seconds (optional)
            version: Version number (for conflict resolution)
            
        Returns:
            True if set successfully
        """
        with self._lock:
            now = time.time()
            expires_at = now + ttl if ttl else None
            
            existing = self._store.get(key)
            new_version = version if version else (existing.version + 1 if existing else 1)
            
            self._store[key] = KeyMetadata(
                value=value,
                created_at=existing.created_at if existing else now,
                updated_at=now,
                expires_at=expires_at,
                version=new_version
            )
            
            if expires_at:
                self._ttl_index[expires_at].append(key)
            
            self._stats.sets += 1
            return True
    
    def get(self, key: str) -> Tuple[Optional[Any], bool]:
        """
        Get a value by key.
        
        Returns:
            Tuple of (value, found). Value is None if not found or expired.
        """
        with self._lock:
            metadata = self._store.get(key)
            
            if metadata is None:
                self._stats.misses += 1
                return None, False
            
            # Check TTL
            if metadata.expires_at and time.time() > metadata.expires_at:
                self._delete_internal(key)
                self._stats.misses += 1
                self._stats.expired += 1
                return None, False
            
            self._stats.hits += 1
            return metadata.value, True
    
    def get_with_metadata(self, key: str) -> Optional[KeyMetadata]:
        """Get value with full metadata."""
        with self._lock:
            metadata = self._store.get(key)
            if metadata is None:
                return None
            if metadata.expires_at and time.time() > metadata.expires_at:
                self._delete_internal(key)
                return None
            return metadata
    
    def delete(self, key: str) -> bool:
        """
        Delete a key.
        
        Returns:
            True if the key existed and was deleted
        """
        with self._lock:
            if key in self._store:
                self._delete_internal(key)
                self._stats.deletes += 1
                return True
            return False
    
    def _delete_internal(self, key: str):
        """Internal delete without lock."""
        if key in self._store:
            del self._store[key]
    
    def exists(self, key: str) -> bool:
        """Check if a key exists (and is not expired)."""
        with self._lock:
            metadata = self._store.get(key)
            if metadata is None:
                return False
            if metadata.expires_at and time.time() > metadata.expires_at:
                self._delete_internal(key)
                return False
            return True
    
    def keys(self, pattern: str = "*") -> List[str]:
        """
        Get all keys matching a pattern.
        Simple pattern matching: * matches everything, prefix* matches prefix.
        """
        with self._lock:
            self._cleanup_expired()
            
            if pattern == "*":
                return list(self._store.keys())
            
            if pattern.endswith("*"):
                prefix = pattern[:-1]
                return [k for k in self._store.keys() if k.startswith(prefix)]
            
            return [k for k in self._store.keys() if k == pattern]
    
    def _cleanup_expired(self):
        """Remove expired keys."""
        now = time.time()
        expired_keys = []
        
        for key, metadata in self._store.items():
            if metadata.expires_at and metadata.expires_at < now:
                expired_keys.append(key)
        
        for key in expired_keys:
            self._delete_internal(key)
            self._stats.expired += 1
    
    def cleanup_expired_keys(self):
        """Public method to cleanup expired keys."""
        with self._lock:
            self._cleanup_expired()
    
    def get_stats(self) -> Dict:
        """Get store statistics."""
        with self._lock:
            stats = self._stats.to_dict()
            stats["key_count"] = len(self._store)
            stats["memory_keys"] = sum(len(k) for k in self._store.keys())
            return stats
    
    def get_all_data(self) -> Dict[str, Tuple[Any, Optional[float], int]]:
        """Get all data for snapshot. Returns {key: (value, expires_at, version)}"""
        with self._lock:
            self._cleanup_expired()
            return {
                key: (meta.value, meta.expires_at, meta.version)
                for key, meta in self._store.items()
            }
    
    def load_data(self, data: Dict[str, Tuple[Any, Optional[float], int]]):
        """Load data from snapshot."""
        with self._lock:
            now = time.time()
            for key, (value, expires_at, version) in data.items():
                # Skip expired keys
                if expires_at and expires_at < now:
                    continue
                self._store[key] = KeyMetadata(
                    value=value,
                    created_at=now,
                    updated_at=now,
                    expires_at=expires_at,
                    version=version
                )
    
    def clear(self):
        """Clear all data."""
        with self._lock:
            self._store.clear()
            self._ttl_index.clear()
    
    def size(self) -> int:
        """Get number of keys."""
        with self._lock:
            return len(self._store)
    
    def get_version(self, key: str) -> Optional[int]:
        """Get version of a key."""
        with self._lock:
            metadata = self._store.get(key)
            return metadata.version if metadata else None
    
    def get_keys_for_shard(self, shard_keys: set) -> Dict[str, KeyMetadata]:
        """Get all keys belonging to a specific shard."""
        with self._lock:
            return {k: v for k, v in self._store.items() if k in shard_keys}
