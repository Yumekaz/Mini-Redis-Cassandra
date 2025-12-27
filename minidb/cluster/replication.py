"""
Replication manager for sync and async replication.
"""

import time
import threading
import queue
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum

from ..config import ConsistencyLevel
from .election import LogEntry


@dataclass
class ReplicationRequest:
    """A pending replication request."""
    entry: LogEntry
    consistency: ConsistencyLevel
    required_acks: int
    received_acks: int = 0
    success: bool = False
    event: threading.Event = None
    
    def __post_init__(self):
        if self.event is None:
            self.event = threading.Event()


class ReplicationManager:
    """
    Manages replication of log entries to followers.
    
    Features:
    - Synchronous replication (QUORUM, ALL, STRONG)
    - Asynchronous replication (ONE, ANY)
    - ACK tracking
    - Timeout handling
    """
    
    def __init__(self, node_id: str, replication_factor: int = 3,
                 default_consistency: ConsistencyLevel = ConsistencyLevel.QUORUM,
                 replication_timeout: float = 5.0):
        self.node_id = node_id
        self.replication_factor = replication_factor
        self.default_consistency = default_consistency
        self.replication_timeout = replication_timeout
        
        self._pending: Dict[int, ReplicationRequest] = {}  # log_index -> request
        self._async_queue: queue.Queue = queue.Queue()
        self._lock = threading.Lock()
        self._running = False
        self._async_thread: Optional[threading.Thread] = None
        
        # Callbacks
        self._send_replicate: Optional[Callable[[str, LogEntry], bool]] = None
        self._get_followers: Optional[Callable[[], List[str]]] = None
    
    def set_callbacks(self, send_replicate=None, get_followers=None):
        """Set replication callbacks."""
        self._send_replicate = send_replicate
        self._get_followers = get_followers
    
    def start(self):
        """Start the replication manager."""
        self._running = True
        self._async_thread = threading.Thread(target=self._async_replication_loop, daemon=True)
        self._async_thread.start()
    
    def stop(self):
        """Stop the replication manager."""
        self._running = False
        if self._async_thread:
            self._async_thread.join(timeout=2.0)
    
    def _async_replication_loop(self):
        """Process async replication queue."""
        while self._running:
            try:
                entry = self._async_queue.get(timeout=1.0)
                self._replicate_to_followers(entry, async_mode=True)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Async replication error: {e}")
    
    def replicate(self, entry: LogEntry, 
                  consistency: Optional[ConsistencyLevel] = None) -> bool:
        """
        Replicate a log entry according to consistency level.
        
        Args:
            entry: The log entry to replicate
            consistency: Consistency level (uses default if None)
            
        Returns:
            True if replication succeeded according to consistency level
        """
        consistency = consistency or self.default_consistency
        
        if not self._get_followers:
            return True
        
        followers = self._get_followers()
        
        if not followers:
            # No followers, always succeed
            return True
        
        # Calculate required acknowledgments
        cluster_size = len(followers) + 1  # +1 for leader
        
        if consistency == ConsistencyLevel.ONE or consistency == ConsistencyLevel.ANY:
            # Async replication
            self._async_queue.put(entry)
            return True
            
        elif consistency == ConsistencyLevel.QUORUM:
            required_acks = cluster_size // 2  # Leader counts as one
            
        elif consistency == ConsistencyLevel.ALL:
            required_acks = len(followers)
            
        elif consistency == ConsistencyLevel.STRONG:
            required_acks = len(followers)
            
        else:
            required_acks = 1
        
        # Create pending request
        request = ReplicationRequest(
            entry=entry,
            consistency=consistency,
            required_acks=required_acks,
            received_acks=0
        )
        
        with self._lock:
            self._pending[entry.index] = request
        
        # Send to followers
        self._replicate_to_followers(entry, async_mode=False)
        
        # Wait for acknowledgments
        success = request.event.wait(timeout=self.replication_timeout)
        
        with self._lock:
            if entry.index in self._pending:
                del self._pending[entry.index]
        
        return request.success or (request.received_acks >= required_acks)
    
    def _replicate_to_followers(self, entry: LogEntry, async_mode: bool):
        """Send replication to all followers."""
        if not self._send_replicate or not self._get_followers:
            return
        
        followers = self._get_followers()
        
        for follower_id in followers:
            try:
                success = self._send_replicate(follower_id, entry)
                if success and not async_mode:
                    self.acknowledge(entry.index, follower_id)
            except Exception as e:
                print(f"Replication to {follower_id} failed: {e}")
    
    def acknowledge(self, log_index: int, from_node: str):
        """
        Acknowledge a replication.
        
        Args:
            log_index: The log entry index being acknowledged
            from_node: The node sending the acknowledgment
        """
        with self._lock:
            if log_index not in self._pending:
                return
            
            request = self._pending[log_index]
            request.received_acks += 1
            
            if request.received_acks >= request.required_acks:
                request.success = True
                request.event.set()
    
    def get_pending_count(self) -> int:
        """Get number of pending replication requests."""
        with self._lock:
            return len(self._pending)
    
    def get_async_queue_size(self) -> int:
        """Get size of async replication queue."""
        return self._async_queue.qsize()
