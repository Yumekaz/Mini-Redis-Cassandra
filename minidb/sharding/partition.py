"""
Partition management for sharding.
"""

import threading
from typing import Dict, List, Optional, Set, Callable
from dataclasses import dataclass, field

from .consistent_hash import ConsistentHashRing


@dataclass
class Partition:
    """A partition of keys."""
    partition_id: int
    owner: str
    replicas: List[str] = field(default_factory=list)
    key_count: int = 0


class PartitionManager:
    """
    Manages partitions and key ownership.
    
    Features:
    - Partition tracking
    - Ownership updates on membership changes
    - Rebalancing coordination
    """
    
    def __init__(self, local_node_id: str, ring: ConsistentHashRing,
                 replication_factor: int = 3):
        self.local_node_id = local_node_id
        self.ring = ring
        self.replication_factor = replication_factor
        
        self._lock = threading.RLock()
        
        # Callbacks
        self._on_ownership_change: Optional[Callable[[str, str, str], None]] = None
    
    def set_ownership_callback(self, callback: Callable[[str, str, str], None]):
        """Set callback for ownership changes: (key_prefix, old_owner, new_owner)"""
        self._on_ownership_change = callback
    
    def get_owner(self, key: str) -> Optional[str]:
        """Get the owner node for a key."""
        return self.ring.get_node(key)
    
    def get_replicas(self, key: str) -> List[str]:
        """Get all replica nodes for a key."""
        return self.ring.get_nodes(key, self.replication_factor)
    
    def is_local_key(self, key: str) -> bool:
        """Check if this node owns or replicates this key."""
        replicas = self.get_replicas(key)
        return self.local_node_id in replicas
    
    def is_primary_owner(self, key: str) -> bool:
        """Check if this node is the primary owner of a key."""
        return self.get_owner(key) == self.local_node_id
    
    def add_node(self, node_id: str):
        """Handle a new node joining."""
        with self._lock:
            self.ring.add_node(node_id)
    
    def remove_node(self, node_id: str):
        """Handle a node leaving."""
        with self._lock:
            self.ring.remove_node(node_id)
    
    def get_local_key_range(self) -> List[str]:
        """Get the key prefixes this node is responsible for."""
        # This is simplified - in a real system you'd track actual key ranges
        return []
    
    def get_partition_info(self) -> Dict:
        """Get partition information."""
        return {
            "local_node": self.local_node_id,
            "replication_factor": self.replication_factor,
            "nodes": self.ring.get_all_nodes(),
            "ring_sample": self.ring.get_ring_state()
        }
