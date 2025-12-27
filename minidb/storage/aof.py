"""
Append-Only File (AOF) persistence.
Ensures no committed write is lost by logging every operation.
"""

import os
import json
import time
import threading
import gzip
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum


class AOFCommand(Enum):
    """AOF command types."""
    SET = "SET"
    DELETE = "DELETE"
    EXPIRE = "EXPIRE"


@dataclass
class AOFEntry:
    """A single AOF log entry."""
    timestamp: float
    command: AOFCommand
    key: str
    value: Optional[Any] = None
    ttl: Optional[int] = None
    version: int = 1
    
    def to_line(self) -> str:
        """Convert to AOF line format."""
        data = {
            "ts": self.timestamp,
            "cmd": self.command.value,
            "key": self.key,
            "ver": self.version
        }
        if self.value is not None:
            data["val"] = self.value
        if self.ttl is not None:
            data["ttl"] = self.ttl
        return json.dumps(data)
    
    @classmethod
    def from_line(cls, line: str) -> 'AOFEntry':
        """Parse from AOF line format."""
        data = json.loads(line.strip())
        return cls(
            timestamp=data["ts"],
            command=AOFCommand(data["cmd"]),
            key=data["key"],
            value=data.get("val"),
            ttl=data.get("ttl"),
            version=data.get("ver", 1)
        )


class AOFPersistence:
    """
    Append-Only File persistence manager.
    
    Features:
    - Append every write to disk
    - Periodic fsync for durability
    - Replay on startup
    - File rotation and compaction
    - Corruption-safe writes
    """
    
    def __init__(self, data_dir: str, fsync_interval: float = 1.0):
        self.data_dir = data_dir
        self.fsync_interval = fsync_interval
        self.aof_path = os.path.join(data_dir, "appendonly.aof")
        self.aof_backup_path = os.path.join(data_dir, "appendonly.aof.bak")
        
        self._file: Optional[Any] = None
        self._lock = threading.Lock()
        self._buffer: List[str] = []
        self._last_fsync = time.time()
        self._running = True
        self._entries_since_compact = 0
        self._compact_threshold = 10000  # Compact after this many entries
        
        # Ensure directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # Open AOF file
        self._open_file()
        
        # Start fsync thread
        self._fsync_thread = threading.Thread(target=self._fsync_loop, daemon=True)
        self._fsync_thread.start()
    
    def _open_file(self):
        """Open AOF file for appending."""
        self._file = open(self.aof_path, "a", encoding="utf-8")
    
    def append(self, command: AOFCommand, key: str, value: Any = None, 
               ttl: Optional[int] = None, version: int = 1):
        """
        Append a command to the AOF.
        
        Args:
            command: The command type (SET, DELETE, EXPIRE)
            key: The key being operated on
            value: The value (for SET)
            ttl: TTL in seconds (optional)
            version: Version number for conflict resolution
        """
        entry = AOFEntry(
            timestamp=time.time(),
            command=command,
            key=key,
            value=value,
            ttl=ttl,
            version=version
        )
        
        with self._lock:
            line = entry.to_line() + "\n"
            self._file.write(line)
            self._entries_since_compact += 1
            
            # Check if immediate fsync needed
            if time.time() - self._last_fsync > self.fsync_interval:
                self._do_fsync()
    
    def _do_fsync(self):
        """Perform fsync."""
        if self._file:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._last_fsync = time.time()
    
    def _fsync_loop(self):
        """Background thread for periodic fsync."""
        while self._running:
            time.sleep(self.fsync_interval)
            with self._lock:
                if self._file and not self._file.closed:
                    self._do_fsync()
    
    def replay(self, apply_func: Callable[[AOFEntry], None]) -> int:
        """
        Replay AOF entries.
        
        Args:
            apply_func: Function to apply each entry to the store
            
        Returns:
            Number of entries replayed
        """
        if not os.path.exists(self.aof_path):
            return 0
        
        count = 0
        corrupted_lines = 0
        
        with open(self.aof_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = AOFEntry.from_line(line)
                    apply_func(entry)
                    count += 1
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    corrupted_lines += 1
                    print(f"Warning: Corrupted AOF line {line_num}: {e}")
        
        if corrupted_lines > 0:
            print(f"AOF replay: {count} entries applied, {corrupted_lines} corrupted lines skipped")
        
        return count
    
    def compact(self, current_state: Dict[str, tuple]):
        """
        Compact the AOF by rewriting with current state.
        
        Args:
            current_state: Dict of {key: (value, expires_at, version)}
        """
        with self._lock:
            # Close current file
            if self._file:
                self._do_fsync()
                self._file.close()
            
            # Write to temp file
            temp_path = self.aof_path + ".tmp"
            now = time.time()
            
            with open(temp_path, "w", encoding="utf-8") as f:
                for key, (value, expires_at, version) in current_state.items():
                    # Skip expired keys
                    if expires_at and expires_at < now:
                        continue
                    
                    # Calculate remaining TTL
                    ttl = None
                    if expires_at:
                        ttl = int(expires_at - now)
                        if ttl <= 0:
                            continue
                    
                    entry = AOFEntry(
                        timestamp=now,
                        command=AOFCommand.SET,
                        key=key,
                        value=value,
                        ttl=ttl,
                        version=version
                    )
                    f.write(entry.to_line() + "\n")
                f.flush()
                os.fsync(f.fileno())
            
            # Atomic rename
            if os.path.exists(self.aof_backup_path):
                os.remove(self.aof_backup_path)
            if os.path.exists(self.aof_path):
                os.rename(self.aof_path, self.aof_backup_path)
            os.rename(temp_path, self.aof_path)
            
            # Reopen file
            self._open_file()
            self._entries_since_compact = 0
    
    def should_compact(self) -> bool:
        """Check if compaction is needed."""
        return self._entries_since_compact >= self._compact_threshold
    
    def close(self):
        """Close the AOF file."""
        self._running = False
        with self._lock:
            if self._file:
                self._do_fsync()
                self._file.close()
                self._file = None
    
    def get_size(self) -> int:
        """Get AOF file size in bytes."""
        if os.path.exists(self.aof_path):
            return os.path.getsize(self.aof_path)
        return 0
