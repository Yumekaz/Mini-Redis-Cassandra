"""
Cluster coordinator that ties together membership, election, and replication.
"""

import time
import threading
from typing import Dict, List, Optional, Callable, Any

from ..config import NodeConfig, ConsistencyLevel, NodeRole, NodeState
from ..network.client import TCPClient, ConnectionPool
from ..network.protocol import Protocol, Message, MessageType
from .membership import MembershipManager, NodeInfo
from .election import ElectionManager, LogEntry
from .replication import ReplicationManager


class ClusterCoordinator:
    """
    Coordinates all cluster operations.
    
    Combines:
    - SWIM membership management
    - Raft-lite leader election
    - Log replication
    - Cluster state propagation
    """
    
    def __init__(self, config: NodeConfig, fault_injector: Any = None):
        self.config = config
        self.node_id = config.node_id
        self.fault_injector = fault_injector
        
        # Create local node info
        self.local_node = NodeInfo(
            node_id=config.node_id,
            host=config.host,
            client_port=config.client_port,
            cluster_port=config.cluster_port
        )
        
        # Initialize components
        self.membership = MembershipManager(
            self.local_node,
            failure_timeout=config.failure_timeout,
            dead_timeout=config.dead_timeout,
            gossip_interval=config.heartbeat_interval
        )
        
        self.election = ElectionManager(
            config.node_id,
            election_timeout_min=config.election_timeout_min,
            election_timeout_max=config.election_timeout_max,
            heartbeat_interval=config.heartbeat_interval
        )
        
        self.replication = ReplicationManager(
            config.node_id,
            replication_factor=config.replication_factor,
            default_consistency=config.default_consistency
        )
        
        # Connection pool for inter-node communication
        self._connection_pool = ConnectionPool(timeout=5.0, node_id=config.node_id)
        
        # Set up callbacks
        self._setup_callbacks()
        
        # Callbacks to node
        self._on_apply_entry: Optional[Callable[[LogEntry], None]] = None
        self._running = False
    
    def _setup_callbacks(self):
        """Wire up all component callbacks."""
        # Membership callbacks
        self.membership.set_callbacks(
            on_join=self._on_node_join,
            on_leave=self._on_node_leave,
            on_suspect=self._on_node_suspect,
            send_gossip=self._send_gossip
        )
        
        # Election callbacks
        self.election.set_callbacks(
            on_become_leader=self._on_become_leader,
            on_become_follower=self._on_become_follower,
            request_votes=self._request_votes,
            send_heartbeats=self._send_heartbeats,
            apply_entry=self._on_apply_entry_internal
        )
        
        # Replication callbacks
        self.replication.set_callbacks(
            send_replicate=self._send_replicate,
            get_followers=self._get_followers
        )
    
    def set_apply_callback(self, callback: Callable[[LogEntry], None]):
        """Set callback for applying log entries."""
        self._on_apply_entry = callback
    
    def start(self):
        """Start the cluster coordinator."""
        self._running = True
        self.membership.start()
        self.election.start()
        self.replication.start()
        print(f"[{self.node_id}] Cluster coordinator started")
    
    def stop(self):
        """Stop the cluster coordinator."""
        self._running = False
        self.replication.stop()
        self.election.stop()
        self.membership.stop()
        self._connection_pool.close_all()
        print(f"[{self.node_id}] Cluster coordinator stopped")
    
    def join_cluster(self, seed_address: str) -> bool:
        """
        Join an existing cluster via a seed node.
        
        Args:
            seed_address: Address of seed node (host:port)
            
        Returns:
            True if joined successfully
        """
        try:
            host, port = seed_address.split(":")
            port = int(port)
            
            client = self._connection_pool.get_connection(host, port)
            
            # Send join message
            join_msg = Message(
                msg_type=MessageType.JOIN,
                payload=self.local_node.to_dict(),
                sender_id=self.node_id
            )
            
            response = client.send_message(join_msg)
            
            if response and response.msg_type == MessageType.RESPONSE:
                # Process membership data from response
                members = response.payload.get("members", [])
                self.membership.handle_gossip(members)
                print(f"[{self.node_id}] Joined cluster via {seed_address}")
                return True
            
            return False
            
        except Exception as e:
            print(f"[{self.node_id}] Failed to join cluster: {e}")
            return False
    
    # ============ Membership Callbacks ============
    
    def _on_node_join(self, node: NodeInfo):
        """Handle new node joining."""
        print(f"[{self.node_id}] Node joined: {node.node_id}")
    
    def _on_node_leave(self, node: NodeInfo):
        """Handle node leaving."""
        print(f"[{self.node_id}] Node left: {node.node_id}")
    
    def _on_node_suspect(self, node: NodeInfo):
        """Handle suspected node."""
        print(f"[{self.node_id}] Node suspected: {node.node_id}")
    
    def _send_gossip(self, target: NodeInfo, members: List[Dict]):
        """Send gossip to a target node."""
        if self.fault_injector and self.fault_injector.should_fail_network(target.node_id):
            return

        try:
            client = self._connection_pool.get_connection(target.host, target.cluster_port)
            
            msg = Protocol.create_gossip(self.node_id, members)
            msg.term = self.election.get_term()
            
            # Include heartbeat if we're leader
            if self.election.is_leader():
                msg.payload["leader_id"] = self.node_id
            
            client.send_message_no_response(msg)
            
        except Exception:
            pass
    
    # ============ Election Callbacks ============
    
    def _on_become_leader(self):
        """Handle becoming leader."""
        self.membership.set_node_role(self.node_id, NodeRole.LEADER)
        self.local_node.role = NodeRole.LEADER
    
    def _on_become_follower(self, leader_id: str):
        """Handle becoming follower."""
        self.membership.set_node_role(self.node_id, NodeRole.FOLLOWER)
        self.local_node.role = NodeRole.FOLLOWER
    
    def _request_votes(self, term: int, last_log_index: int, last_log_term: int) -> int:
        """Request votes from other nodes."""
        votes = 1  # Vote for self
        
        for node in self.membership.get_alive_nodes():
            if node.node_id == self.node_id:
                continue
            
            if self.fault_injector and self.fault_injector.should_fail_network(node.node_id):
                continue
            
            try:
                client = self._connection_pool.get_connection(node.host, node.cluster_port)
                
                msg = Protocol.create_vote_request(
                    self.node_id, term, last_log_index, last_log_term
                )
                
                response = client.send_message(msg)
                
                if response and response.payload.get("vote_granted"):
                    votes += 1
                    
            except Exception:
                pass
        
        return votes
    
    def _send_heartbeats(self):
        """Send heartbeats to all nodes."""
        members_data = self.membership.get_members_data()
        
        for node in self.membership.get_alive_nodes():
            if node.node_id == self.node_id:
                continue
            
            if self.fault_injector and self.fault_injector.should_fail_network(node.node_id):
                continue
            
            try:
                client = self._connection_pool.get_connection(node.host, node.cluster_port)
                
                msg = Protocol.create_heartbeat(
                    self.node_id,
                    self.election.current_term,
                    self.node_id,
                    members_data
                )
                
                client.send_message_no_response(msg)
                
            except Exception:
                pass
    
    def _on_apply_entry_internal(self, entry: LogEntry):
        """Apply a committed log entry."""
        if self._on_apply_entry:
            self._on_apply_entry(entry)
    
    # ============ Replication Callbacks ============
    
    def _send_replicate(self, follower_id: str, entry: LogEntry) -> bool:
        """Send replication to a follower."""
        node = self.membership.get_node(follower_id)
        if not node:
            return False
        
        if self.fault_injector and self.fault_injector.should_fail_network(follower_id):
            return False

        try:
            client = self._connection_pool.get_connection(node.host, node.cluster_port)
            
            msg = Protocol.create_replicate(
                self.node_id,
                self.election.current_term,
                entry.to_dict()
            )
            
            response = client.send_message(msg)
            
            if response and response.payload.get("success"):
                return True
            
            return False
            
        except Exception:
            return False
    
    def _get_followers(self) -> List[str]:
        """Get list of follower node IDs."""
        return [
            n.node_id for n in self.membership.get_alive_nodes()
            if n.node_id != self.node_id
        ]
    
    # ============ Message Handling ============
    
    def handle_cluster_message(self, message: Message) -> Optional[Message]:
        """
        Handle incoming cluster message.
        
        Args:
            message: The incoming message
            
        Returns:
            Response message if needed
        """
        msg_type = message.msg_type
        sender_id = message.sender_id
        
        if msg_type == MessageType.HEARTBEAT:
            leader_id = message.payload.get("leader_id")
            if leader_id:
                self.election.receive_heartbeat(leader_id, message.term)
            
            members = message.payload.get("members", [])
            if members:
                self.membership.handle_gossip(members)
            
            self.membership.update_heartbeat(sender_id)
            return None
        
        elif msg_type == MessageType.VOTE_REQUEST:
            term, granted = self.election.handle_vote_request(
                sender_id,
                message.term,
                message.payload.get("last_log_index", 0),
                message.payload.get("last_log_term", 0)
            )
            return Protocol.create_vote_response(self.node_id, term, granted)
        
        elif msg_type == MessageType.GOSSIP:
            members = message.payload.get("members", [])
            self.membership.handle_gossip(members)
            self.membership.update_heartbeat(sender_id)
            return None
        
        elif msg_type == MessageType.JOIN:
            # New node joining
            node_data = message.payload
            new_node = NodeInfo.from_dict(node_data)
            self.membership.add_node(new_node)
            
            return Protocol.create_response(
                True,
                data={"members": self.membership.get_members_data()}
            )
        
        elif msg_type == MessageType.REPLICATE:
            entry_data = message.payload.get("entry")
            if entry_data:
                entry = LogEntry.from_dict(entry_data)
                # Apply the entry
                if self._on_apply_entry:
                    self._on_apply_entry(entry)
                
                return Protocol.create_response(True, data={"success": True})
            
            return Protocol.create_response(False, error="No entry in payload")
        
        elif msg_type == MessageType.APPEND_ENTRIES:
            term, success, match_index = self.election.handle_append_entries(
                sender_id,
                message.term,
                message.payload.get("prev_log_index", 0),
                message.payload.get("prev_log_term", 0),
                message.payload.get("entries", []),
                message.payload.get("leader_commit", 0)
            )
            
            return Message(
                msg_type=MessageType.APPEND_RESPONSE,
                payload={"success": success, "match_index": match_index},
                sender_id=self.node_id,
                term=term
            )
        
        return None
    
    # ============ Public API ============
    
    def is_leader(self) -> bool:
        """Check if this node is the leader."""
        return self.election.is_leader()
    
    def get_leader_id(self) -> Optional[str]:
        """Get the current leader ID."""
        return self.election.leader_id
    
    def get_leader_address(self) -> Optional[str]:
        """Get the current leader's client address."""
        leader = self.membership.get_leader()
        return leader.client_address() if leader else None
    
    def append_and_replicate(self, command: str, key: str, value: Any = None,
                            ttl: Optional[int] = None,
                            consistency: ConsistencyLevel = None) -> bool:
        """
        Append entry to log and replicate.
        
        Args:
            command: The command (SET, DELETE)
            key: The key
            value: The value (for SET)
            ttl: TTL in seconds
            consistency: Consistency level
            
        Returns:
            True if successful
        """
        if not self.is_leader():
            return False
        
        entry = self.election.append_entry(command, key, value, ttl)
        if not entry:
            return False
        
        # Replicate based on consistency
        consistency = consistency or self.config.default_consistency
        success = self.replication.replicate(entry, consistency)
        
        # Apply locally if successful
        if success and self._on_apply_entry:
            self._on_apply_entry(entry)
        
        return success
    
    def get_cluster_info(self) -> Dict:
        """Get comprehensive cluster information."""
        return {
            "node_id": self.node_id,
            "role": self.election.role.value,
            "term": self.election.current_term,
            "leader_id": self.election.leader_id,
            "members": [n.to_dict() for n in self.membership.get_all_nodes()],
            "alive_count": self.membership.node_count(),
            "replication": {
                "pending": self.replication.get_pending_count(),
                "async_queue": self.replication.get_async_queue_size()
            },
            "log_length": len(self.election.log),
            "commit_index": self.election.commit_index
        }
