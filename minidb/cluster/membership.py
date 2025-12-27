"""
SWIM-style gossip membership protocol for failure detection and cluster membership.
"""

import time
import random
import threading
from typing import Dict, List, Optional, Set, Callable
from dataclasses import dataclass, field
from enum import Enum

from ..config import NodeState, NodeRole


@dataclass
class NodeInfo:
    """Information about a cluster node."""
    node_id: str
    host: str
    client_port: int
    cluster_port: int
    state: NodeState = NodeState.ALIVE
    role: NodeRole = NodeRole.FOLLOWER
    last_heartbeat: float = field(default_factory=time.time)
    incarnation: int = 0  # For handling outdated gossip
    joined_at: float = field(default_factory=time.time)
    
    def address(self) -> str:
        return f"{self.host}:{self.cluster_port}"
    
    def client_address(self) -> str:
        return f"{self.host}:{self.client_port}"
    
    def to_dict(self) -> Dict:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "client_port": self.client_port,
            "cluster_port": self.cluster_port,
            "state": self.state.value,
            "role": self.role.value,
            "last_heartbeat": self.last_heartbeat,
            "incarnation": self.incarnation
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'NodeInfo':
        return cls(
            node_id=data["node_id"],
            host=data["host"],
            client_port=data["client_port"],
            cluster_port=data["cluster_port"],
            state=NodeState(data.get("state", "ALIVE")),
            role=NodeRole(data.get("role", "FOLLOWER")),
            last_heartbeat=data.get("last_heartbeat", time.time()),
            incarnation=data.get("incarnation", 0)
        )


class MembershipManager:
    """
    SWIM-style membership manager with gossip protocol.
    
    Features:
    - Gossip heartbeats
    - Failure suspicion thresholds
    - Node join/leave announcements
    - Distributed cluster state propagation
    """
    
    def __init__(self, local_node: NodeInfo, 
                 failure_timeout: float = 5.0,
                 dead_timeout: float = 15.0,
                 gossip_interval: float = 1.0):
        self.local_node = local_node
        self.failure_timeout = failure_timeout
        self.dead_timeout = dead_timeout
        self.gossip_interval = gossip_interval
        
        self._members: Dict[str, NodeInfo] = {local_node.node_id: local_node}
        self._lock = threading.RLock()
        self._running = False
        self._gossip_thread: Optional[threading.Thread] = None
        
        # Callbacks
        self._on_node_join: Optional[Callable[[NodeInfo], None]] = None
        self._on_node_leave: Optional[Callable[[NodeInfo], None]] = None
        self._on_node_suspect: Optional[Callable[[NodeInfo], None]] = None
        self._send_gossip: Optional[Callable[[NodeInfo, List[Dict]], None]] = None
    
    def set_callbacks(self, on_join=None, on_leave=None, on_suspect=None, send_gossip=None):
        """Set event callbacks."""
        self._on_node_join = on_join
        self._on_node_leave = on_leave
        self._on_node_suspect = on_suspect
        self._send_gossip = send_gossip
    
    def start(self):
        """Start the membership manager."""
        self._running = True
        self._gossip_thread = threading.Thread(target=self._gossip_loop, daemon=True)
        self._gossip_thread.start()
    
    def stop(self):
        """Stop the membership manager."""
        self._running = False
        if self._gossip_thread:
            self._gossip_thread.join(timeout=2.0)
    
    def _gossip_loop(self):
        """Main gossip loop."""
        while self._running:
            try:
                self._check_node_health()
                self._send_gossip_to_random_nodes()
            except Exception as e:
                print(f"Gossip error: {e}")
            
            time.sleep(self.gossip_interval)
    
    def _check_node_health(self):
        """Check health of all known nodes."""
        now = time.time()
        
        with self._lock:
            for node_id, node in list(self._members.items()):
                if node_id == self.local_node.node_id:
                    node.last_heartbeat = now
                    continue
                
                time_since_heartbeat = now - node.last_heartbeat
                
                if node.state == NodeState.ALIVE:
                    if time_since_heartbeat > self.failure_timeout:
                        node.state = NodeState.SUSPECT
                        if self._on_node_suspect:
                            self._on_node_suspect(node)
                
                elif node.state == NodeState.SUSPECT:
                    if time_since_heartbeat > self.dead_timeout:
                        node.state = NodeState.DEAD
                        if self._on_node_leave:
                            self._on_node_leave(node)
    
    def _send_gossip_to_random_nodes(self):
        """Send gossip to random subset of nodes."""
        with self._lock:
            alive_nodes = [
                n for n in self._members.values()
                if n.node_id != self.local_node.node_id and n.state != NodeState.DEAD
            ]
        
        if not alive_nodes or not self._send_gossip:
            return
        
        # Gossip to random subset (fanout of 3)
        targets = random.sample(alive_nodes, min(3, len(alive_nodes)))
        members_data = self.get_members_data()
        
        for target in targets:
            try:
                self._send_gossip(target, members_data)
            except Exception:
                pass
    
    def add_node(self, node: NodeInfo) -> bool:
        """Add a new node to the membership."""
        with self._lock:
            if node.node_id in self._members:
                existing = self._members[node.node_id]
                # Update if newer incarnation
                if node.incarnation > existing.incarnation:
                    self._members[node.node_id] = node
                    return True
                return False
            
            self._members[node.node_id] = node
            
        if self._on_node_join:
            self._on_node_join(node)
        
        return True
    
    def remove_node(self, node_id: str):
        """Remove a node from the membership."""
        with self._lock:
            if node_id in self._members and node_id != self.local_node.node_id:
                node = self._members[node_id]
                del self._members[node_id]
                
                if self._on_node_leave:
                    self._on_node_leave(node)
    
    def update_heartbeat(self, node_id: str):
        """Update heartbeat timestamp for a node."""
        with self._lock:
            if node_id in self._members:
                node = self._members[node_id]
                node.last_heartbeat = time.time()
                if node.state == NodeState.SUSPECT:
                    node.state = NodeState.ALIVE
    
    def handle_gossip(self, members_data: List[Dict]):
        """Handle incoming gossip message."""
        with self._lock:
            for member_data in members_data:
                node_id = member_data["node_id"]
                
                if node_id == self.local_node.node_id:
                    continue
                
                incoming = NodeInfo.from_dict(member_data)
                
                if node_id not in self._members:
                    self._members[node_id] = incoming
                    if self._on_node_join:
                        threading.Thread(
                            target=self._on_node_join, 
                            args=(incoming,),
                            daemon=True
                        ).start()
                else:
                    existing = self._members[node_id]
                    # Update if newer incarnation or more recent heartbeat
                    if (incoming.incarnation > existing.incarnation or
                        incoming.last_heartbeat > existing.last_heartbeat):
                        self._members[node_id] = incoming
                        
                        # Revive if was dead
                        if existing.state == NodeState.DEAD and incoming.state == NodeState.ALIVE:
                            if self._on_node_join:
                                threading.Thread(
                                    target=self._on_node_join,
                                    args=(incoming,),
                                    daemon=True
                                ).start()
    
    def get_alive_nodes(self) -> List[NodeInfo]:
        """Get all alive nodes."""
        with self._lock:
            return [n for n in self._members.values() if n.state == NodeState.ALIVE]
    
    def get_all_nodes(self) -> List[NodeInfo]:
        """Get all known nodes."""
        with self._lock:
            return list(self._members.values())
    
    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Get a specific node."""
        with self._lock:
            return self._members.get(node_id)
    
    def get_members_data(self) -> List[Dict]:
        """Get membership data for gossip."""
        with self._lock:
            return [n.to_dict() for n in self._members.values()]
    
    def set_node_role(self, node_id: str, role: NodeRole):
        """Set the role of a node."""
        with self._lock:
            if node_id in self._members:
                self._members[node_id].role = role
    
    def get_leader(self) -> Optional[NodeInfo]:
        """Get the current leader node."""
        with self._lock:
            for node in self._members.values():
                if node.role == NodeRole.LEADER and node.state == NodeState.ALIVE:
                    return node
            return None
    
    def node_count(self) -> int:
        """Get count of alive nodes."""
        return len(self.get_alive_nodes())
    
    def increment_incarnation(self):
        """Increment local node incarnation (used when suspected)."""
        with self._lock:
            self.local_node.incarnation += 1
