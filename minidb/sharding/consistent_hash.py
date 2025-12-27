"""
Consistent hashing ring for key distribution.
"""

import hashlib
import bisect
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
import threading


@dataclass
class VirtualNode:
    """A virtual node on the hash ring."""
    physical_node: str
    virtual_id: int
    hash_value: int


class ConsistentHashRing:
    """
    Consistent hashing ring for distributing keys across nodes.
    
    Features:
    - Virtual nodes for better distribution
    - O(log n) lookups
    - Minimal key movement on node changes
    """
    
    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        
        self._ring: List[Tuple[int, str]] = []  # (hash, node_id)
        self._nodes: Set[str] = set()
        self._lock = threading.RLock()
    
    def _hash(self, key: str) -> int:
        """Generate hash for a key."""
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)
    
    def add_node(self, node_id: str) -> List[str]:
        """
        Add a node to the ring.
        
        Args:
            node_id: The node identifier
            
        Returns:
            List of keys that should be moved to this node
        """
        with self._lock:
            if node_id in self._nodes:
                return []
            
            self._nodes.add(node_id)
            
            # Add virtual nodes
            for i in range(self.virtual_nodes):
                vnode_key = f"{node_id}:{i}"
                hash_val = self._hash(vnode_key)
                bisect.insort(self._ring, (hash_val, node_id))
            
            return []  # Key movement handled by partition manager
    
    def remove_node(self, node_id: str) -> List[Tuple[str, str]]:
        """
        Remove a node from the ring.
        
        Args:
            node_id: The node identifier
            
        Returns:
            List of (key_hash_range, new_owner) tuples
        """
        with self._lock:
            if node_id not in self._nodes:
                return []
            
            self._nodes.discard(node_id)
            
            # Remove virtual nodes
            self._ring = [(h, n) for h, n in self._ring if n != node_id]
            
            return []  # Rebalancing handled by partition manager
    
    def get_node(self, key: str) -> Optional[str]:
        """
        Get the node responsible for a key.
        
        Args:
            key: The key to look up
            
        Returns:
            Node ID or None if ring is empty
        """
        with self._lock:
            if not self._ring:
                return None
            
            key_hash = self._hash(key)
            
            # Find the first node with hash >= key_hash
            idx = bisect.bisect_left(self._ring, (key_hash, ""))
            
            if idx == len(self._ring):
                idx = 0  # Wrap around
            
            return self._ring[idx][1]
    
    def get_nodes(self, key: str, count: int) -> List[str]:
        """
        Get multiple nodes for a key (for replication).
        
        Args:
            key: The key to look up
            count: Number of nodes to return
            
        Returns:
            List of node IDs
        """
        with self._lock:
            if not self._ring:
                return []
            
            key_hash = self._hash(key)
            idx = bisect.bisect_left(self._ring, (key_hash, ""))
            
            nodes = []
            seen = set()
            
            for i in range(len(self._ring)):
                ring_idx = (idx + i) % len(self._ring)
                node_id = self._ring[ring_idx][1]
                
                if node_id not in seen:
                    nodes.append(node_id)
                    seen.add(node_id)
                    
                    if len(nodes) >= count:
                        break
            
            return nodes
    
    def get_all_nodes(self) -> List[str]:
        """Get all physical nodes."""
        with self._lock:
            return list(self._nodes)
    
    def get_ring_state(self) -> List[Dict]:
        """Get current ring state for debugging."""
        with self._lock:
            return [
                {"hash": h, "node": n}
                for h, n in self._ring[:20]  # First 20 for brevity
            ]
    
    def get_node_count(self) -> int:
        """Get number of physical nodes."""
        with self._lock:
            return len(self._nodes)
    
    def get_key_distribution(self, sample_keys: List[str]) -> Dict[str, int]:
        """
        Get distribution of keys across nodes.
        
        Args:
            sample_keys: List of keys to check
            
        Returns:
            Dict of node_id -> key count
        """
        distribution = {}
        for key in sample_keys:
            node = self.get_node(key)
            if node:
                distribution[node] = distribution.get(node, 0) + 1
        return distribution
