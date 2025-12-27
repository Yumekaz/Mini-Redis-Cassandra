"""
Snapshot persistence for periodic full database dumps.
"""

import os
import json
import gzip
import time
import threading
import shutil
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass


@dataclass
class SnapshotMetadata:
    """Metadata about a snapshot."""
    timestamp: float
    key_count: int
    compressed_size: int
    checksum: str


class SnapshotPersistence:
    """
    Snapshot persistence manager.
    
    Features:
    - Periodic full snapshots
    - Compressed storage
    - Corruption-safe writes
    - Recovery with AOF tail replay
    """
    
    def __init__(self, data_dir: str, interval: float = 300.0):
        self.data_dir = data_dir
        self.interval = interval
        self.snapshot_path = os.path.join(data_dir, "snapshot.db.gz")
        self.snapshot_tmp_path = os.path.join(data_dir, "snapshot.db.tmp.gz")
        self.metadata_path = os.path.join(data_dir, "snapshot.meta.json")
        
        self._lock = threading.Lock()
        self._last_snapshot = 0.0
        self._running = True
        
        os.makedirs(data_dir, exist_ok=True)
    
    def save(self, data: Dict[str, tuple]) -> bool:
        """
        Save a snapshot of the database.
        
        Args:
            data: Dict of {key: (value, expires_at, version)}
            
        Returns:
            True if snapshot was saved successfully
        """
        with self._lock:
            try:
                # Serialize data
                snapshot_data = {
                    "version": 1,
                    "timestamp": time.time(),
                    "data": {}
                }
                
                for key, (value, expires_at, version) in data.items():
                    snapshot_data["data"][key] = {
                        "v": value,
                        "e": expires_at,
                        "ver": version
                    }
                
                # Write to temp file (compressed)
                json_bytes = json.dumps(snapshot_data).encode("utf-8")
                
                with gzip.open(self.snapshot_tmp_path, "wb") as f:
                    f.write(json_bytes)
                
                # Atomic rename
                if os.path.exists(self.snapshot_path):
                    backup_path = self.snapshot_path + ".bak"
                    if os.path.exists(backup_path):
                        os.remove(backup_path)
                    os.rename(self.snapshot_path, backup_path)
                
                os.rename(self.snapshot_tmp_path, self.snapshot_path)
                
                # Save metadata
                metadata = {
                    "timestamp": snapshot_data["timestamp"],
                    "key_count": len(data),
                    "compressed_size": os.path.getsize(self.snapshot_path)
                }
                
                with open(self.metadata_path, "w") as f:
                    json.dump(metadata, f)
                
                self._last_snapshot = time.time()
                return True
                
            except Exception as e:
                print(f"Snapshot save error: {e}")
                # Clean up temp file
                if os.path.exists(self.snapshot_tmp_path):
                    os.remove(self.snapshot_tmp_path)
                return False
    
    def load(self) -> Optional[Dict[str, tuple]]:
        """
        Load the latest snapshot.
        
        Returns:
            Dict of {key: (value, expires_at, version)} or None if no snapshot
        """
        if not os.path.exists(self.snapshot_path):
            return None
        
        try:
            with gzip.open(self.snapshot_path, "rb") as f:
                json_bytes = f.read()
            
            snapshot_data = json.loads(json_bytes.decode("utf-8"))
            
            # Convert back to tuple format
            result = {}
            now = time.time()
            
            for key, item in snapshot_data["data"].items():
                expires_at = item.get("e")
                # Skip expired keys
                if expires_at and expires_at < now:
                    continue
                result[key] = (item["v"], expires_at, item.get("ver", 1))
            
            return result
            
        except Exception as e:
            print(f"Snapshot load error: {e}")
            # Try backup
            backup_path = self.snapshot_path + ".bak"
            if os.path.exists(backup_path):
                try:
                    shutil.copy(backup_path, self.snapshot_path)
                    return self.load()
                except Exception:
                    pass
            return None
    
    def get_metadata(self) -> Optional[SnapshotMetadata]:
        """Get metadata about the latest snapshot."""
        if not os.path.exists(self.metadata_path):
            return None
        
        try:
            with open(self.metadata_path, "r") as f:
                data = json.load(f)
            return SnapshotMetadata(
                timestamp=data["timestamp"],
                key_count=data["key_count"],
                compressed_size=data["compressed_size"],
                checksum=""
            )
        except Exception:
            return None
    
    def should_snapshot(self) -> bool:
        """Check if it's time to create a new snapshot."""
        return time.time() - self._last_snapshot >= self.interval
    
    def get_snapshot_time(self) -> float:
        """Get timestamp of last snapshot."""
        metadata = self.get_metadata()
        return metadata.timestamp if metadata else 0.0
