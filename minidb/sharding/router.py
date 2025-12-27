"""
Shard-aware client router for directing requests to correct nodes.
"""

import random
import threading
from typing import Dict, List, Optional, Tuple, Any, Callable

from .consistent_hash import ConsistentHashRing
from ..config import ConsistencyLevel


class ShardRouter:
    """
    Routes client requests to appropriate nodes based on key ownership.
    
    Features:
    - Key-to-node routing via consistent hashing
    - Replica selection for reads
    - Write routing to primary owner
    - Fallback handling
    """
    
    def __init__(self, local_node_id: str, ring: ConsistentHashRing,
                 replication_factor: int = 3):
        self.local_node_id = local_node_id
        self.ring = ring
        self.replication_factor = replication_factor
        
        self._lock = threading.RLock()
        self._node_addresses: Dict[str, str] = {}  # node_id -> "host:port"
        self._node_health: Dict[str, bool] = {}  # node_id -> is_healthy
        
        # Callbacks
        self._get_node_address: Optional[Callable[[str], Optional[str]]] = None
        self._is_node_healthy: Optional[Callable[[str], bool]] = None
    
    def set_callbacks(self, get_node_address=None, is_node_healthy=None):
        """Set router callbacks."""
        self._get_node_address = get_node_address
        self._is_node_healthy = is_node_healthy
    
    def update_node_address(self, node_id: str, address: str):
        """Update address for a node."""
        with self._lock:
            self._node_addresses[node_id] = address
    
    def remove_node(self, node_id: str):
        """Remove a node from routing."""
        with self._lock:
            self._node_addresses.pop(node_id, None)
            self._node_health.pop(node_id, None)
    
    def set_node_health(self, node_id: str, is_healthy: bool):
        """Update health status for a node."""
        with self._lock:
            self._node_health[node_id] = is_healthy
    
    def _is_healthy(self, node_id: str) -> bool:
        """Check if a node is healthy."""
        if self._is_node_healthy:
            return self._is_node_healthy(node_id)
        with self._lock:
            return self._node_health.get(node_id, True)
    
    def _get_address(self, node_id: str) -> Optional[str]:
        """Get address for a node."""
        if self._get_node_address:
            return self._get_node_address(node_id)
        with self._lock:
            return self._node_addresses.get(node_id)
    
    def route_write(self, key: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Route a write request to the primary owner.
        
        Args:
            key: The key being written
            
        Returns:
            Tuple of (node_id, address) or (None, None) if no route
        """
        owner = self.ring.get_node(key)
        if not owner:
            return None, None
        
        # Check if owner is healthy
        if self._is_healthy(owner):
            address = self._get_address(owner)
            if address:
                return owner, address
        
        # Fallback to first healthy replica
        replicas = self.ring.get_nodes(key, self.replication_factor)
        for replica in replicas:
            if self._is_healthy(replica):
                address = self._get_address(replica)
                if address:
                    return replica, address
        
        return None, None
    
    def route_read(self, key: str, 
                   consistency: ConsistencyLevel = ConsistencyLevel.ANY,
                   prefer_local: bool = True) -> List[Tuple[str, str]]:
        """
        Route a read request based on consistency level.
        
        Args:
            key: The key being read
            consistency: Read consistency level
            prefer_local: Prefer local node if it's a replica
            
        Returns:
            List of (node_id, address) tuples to query
        """
        replicas = self.ring.get_nodes(key, self.replication_factor)
        routes = []
        
        # For STRONG/LEADER consistency, only return the primary
        if consistency == ConsistencyLevel.STRONG:
            owner = self.ring.get_node(key)
            if owner and self._is_healthy(owner):
                address = self._get_address(owner)
                if address:
                    return [(owner, address)]
            return []
        
        # For ANY/ONE, prefer local node
        if consistency in (ConsistencyLevel.ANY, ConsistencyLevel.ONE):
            if prefer_local and self.local_node_id in replicas:
                return [(self.local_node_id, "local")]
            
            # Pick any healthy replica
            for replica in replicas:
                if self._is_healthy(replica):
                    address = self._get_address(replica)
                    if address:
                        return [(replica, address)]
            return []
        
        # For QUORUM/ALL, return multiple nodes
        healthy_replicas = []
        for replica in replicas:
            if self._is_healthy(replica):
                address = self._get_address(replica) if replica != self.local_node_id else "local"
                if address:
                    healthy_replicas.append((replica, address))
        
        # Put local first if available
        if prefer_local:
            healthy_replicas.sort(key=lambda x: 0 if x[0] == self.local_node_id else 1)
        
        if consistency == ConsistencyLevel.QUORUM:
            quorum_size = len(replicas) // 2 + 1
            return healthy_replicas[:quorum_size]
        
        if consistency == ConsistencyLevel.ALL:
            return healthy_replicas
        
        return healthy_replicas[:1] if healthy_replicas else []
    
    def should_handle_locally(self, key: str) -> bool:
        """
        Check if this node should handle a key locally.
        
        Args:
            key: The key to check
            
        Returns:
            True if local node is a replica for this key
        """
        replicas = self.ring.get_nodes(key, self.replication_factor)
        return self.local_node_id in replicas
    
    def is_primary_owner(self, key: str) -> bool:
        """Check if this node is the primary owner of a key."""
        return self.ring.get_node(key) == self.local_node_id
    
    def get_routing_info(self, key: str) -> Dict:
        """Get routing information for a key."""
        owner = self.ring.get_node(key)
        replicas = self.ring.get_nodes(key, self.replication_factor)
        
        return {
            "key": key,
            "primary_owner": owner,
            "replicas": replicas,
            "is_local": self.local_node_id in replicas,
            "is_primary": owner == self.local_node_id,
            "healthy_replicas": [r for r in replicas if self._is_healthy(r)]
        }
    
    def get_all_routes(self) -> Dict[str, str]:
        """Get all known node routes."""
        with self._lock:
            return dict(self._node_addresses)
