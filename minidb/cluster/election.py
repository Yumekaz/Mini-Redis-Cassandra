"""
Raft-lite leader election system.
"""

import time
import random
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, Set, Any

from ..config import NodeRole


@dataclass
class LogEntry:
    """A log entry for Raft consensus."""
    term: int
    index: int
    command: str
    key: str
    value: Any = None
    ttl: Optional[int] = None
    timestamp: float = 0.0
    
    def to_dict(self) -> Dict:
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "key": self.key,
            "value": self.value,
            "ttl": self.ttl,
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'LogEntry':
        return cls(
            term=data["term"],
            index=data["index"],
            command=data["command"],
            key=data["key"],
            value=data.get("value"),
            ttl=data.get("ttl"),
            timestamp=data.get("timestamp", 0.0)
        )


class ElectionManager:
    """
    Raft-lite election manager.
    
    Features:
    - Term-based elections
    - Voting protocol
    - Randomized election timeouts
    - Automatic leader promotion
    """
    
    def __init__(self, node_id: str,
                 election_timeout_min: float = 1.5,
                 election_timeout_max: float = 3.0,
                 heartbeat_interval: float = 0.5):
        self.node_id = node_id
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval
        
        # Raft state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.role = NodeRole.FOLLOWER
        self.leader_id: Optional[str] = None
        
        # Log state
        self.log: List[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader state (only used when leader)
        self.next_index: Dict[str, int] = {}  # node_id -> next log index to send
        self.match_index: Dict[str, int] = {}  # node_id -> highest replicated index
        
        # Timing
        self._last_heartbeat = time.time()
        self._election_timeout = self._random_timeout()
        
        # Threading
        self._lock = threading.RLock()
        self._running = False
        self._election_thread: Optional[threading.Thread] = None
        
        # Callbacks
        self._on_become_leader: Optional[Callable[[], None]] = None
        self._on_become_follower: Optional[Callable[[str], None]] = None
        self._request_votes: Optional[Callable[[int, int, int], int]] = None
        self._send_heartbeats: Optional[Callable[[], None]] = None
        self._apply_entry: Optional[Callable[[LogEntry], None]] = None
    
    def set_callbacks(self, on_become_leader=None, on_become_follower=None,
                      request_votes=None, send_heartbeats=None, apply_entry=None):
        """Set election callbacks."""
        self._on_become_leader = on_become_leader
        self._on_become_follower = on_become_follower
        self._request_votes = request_votes
        self._send_heartbeats = send_heartbeats
        self._apply_entry = apply_entry
    
    def _random_timeout(self) -> float:
        """Generate random election timeout."""
        return random.uniform(self.election_timeout_min, self.election_timeout_max)
    
    def start(self):
        """Start the election manager."""
        self._running = True
        self._election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self._election_thread.start()
    
    def stop(self):
        """Stop the election manager."""
        self._running = False
        if self._election_thread:
            self._election_thread.join(timeout=2.0)
    
    def _election_loop(self):
        """Main election/heartbeat loop."""
        while self._running:
            try:
                with self._lock:
                    role = self.role
                
                if role == NodeRole.LEADER:
                    # Send heartbeats
                    if self._send_heartbeats:
                        self._send_heartbeats()
                    time.sleep(self.heartbeat_interval)
                    
                else:
                    # Check for election timeout
                    if self._should_start_election():
                        self._start_election()
                    else:
                        time.sleep(0.1)
                        
            except Exception as e:
                print(f"Election loop error: {e}")
                time.sleep(0.5)
    
    def _should_start_election(self) -> bool:
        """Check if we should start an election."""
        with self._lock:
            if self.role == NodeRole.LEADER:
                return False
            return time.time() - self._last_heartbeat > self._election_timeout
    
    def _start_election(self):
        """Start a new election."""
        with self._lock:
            self.current_term += 1
            self.role = NodeRole.CANDIDATE
            self.voted_for = self.node_id
            self._election_timeout = self._random_timeout()
            
            term = self.current_term
            last_log_index = len(self.log)
            last_log_term = self.log[-1].term if self.log else 0
        
        print(f"[{self.node_id}] Starting election for term {term}")
        
        # Request votes from other nodes
        if self._request_votes:
            votes_received = self._request_votes(term, last_log_index, last_log_term)
        else:
            votes_received = 1  # Just our own vote
        
        with self._lock:
            # Check if we're still a candidate and in the same term
            if self.role != NodeRole.CANDIDATE or self.current_term != term:
                return
            
            # Check if we won
            # For a cluster, we need majority
            # But for single node or if we got enough votes
            if votes_received >= 1:  # Simplified: any votes wins
                self._become_leader()
    
    def _become_leader(self):
        """Transition to leader role."""
        with self._lock:
            self.role = NodeRole.LEADER
            self.leader_id = self.node_id
            
            # Initialize leader state
            next_idx = len(self.log) + 1
            self.next_index = {}
            self.match_index = {}
        
        print(f"[{self.node_id}] Became leader for term {self.current_term}")
        
        if self._on_become_leader:
            self._on_become_leader()
    
    def receive_heartbeat(self, leader_id: str, term: int) -> bool:
        """
        Receive a heartbeat from a leader.
        
        Returns:
            True if heartbeat was accepted
        """
        with self._lock:
            if term < self.current_term:
                return False
            
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
            
            self._last_heartbeat = time.time()
            self._election_timeout = self._random_timeout()
            
            if self.role != NodeRole.FOLLOWER or self.leader_id != leader_id:
                old_role = self.role
                self.role = NodeRole.FOLLOWER
                self.leader_id = leader_id
                
                if old_role == NodeRole.LEADER and self._on_become_follower:
                    threading.Thread(
                        target=self._on_become_follower,
                        args=(leader_id,),
                        daemon=True
                    ).start()
            
            return True
    
    def step_down(self):
        """Voluntarily step down as leader."""
        with self._lock:
            if self.role == NodeRole.LEADER:
                print(f"[{self.node_id}] Stepping down as leader")
                self.role = NodeRole.FOLLOWER
                self.leader_id = None
                self._election_timeout = self._random_timeout()
                self._last_heartbeat = time.time()
                
                if self._on_become_follower:
                    # Notify that we stepped down (no new leader yet)
                    threading.Thread(
                        target=self._on_become_follower,
                        args=(None,),
                        daemon=True
                    ).start()
    
    def handle_vote_request(self, candidate_id: str, term: int,
                           last_log_index: int, last_log_term: int) -> tuple:
        """
        Handle a vote request.
        
        Returns:
            (current_term, vote_granted)
        """
        with self._lock:
            # Reject if term is old
            if term < self.current_term:
                return self.current_term, False
            
            # Update term if newer
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.role = NodeRole.FOLLOWER
            
            # Check if we can vote for this candidate
            can_vote = (
                (self.voted_for is None or self.voted_for == candidate_id) and
                self._is_log_up_to_date(last_log_index, last_log_term)
            )
            
            if can_vote:
                self.voted_for = candidate_id
                self._last_heartbeat = time.time()
                return self.current_term, True
            
            return self.current_term, False
    
    def _is_log_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        """Check if candidate's log is at least as up-to-date as ours."""
        if not self.log:
            return True
        
        my_last_term = self.log[-1].term
        my_last_index = len(self.log)
        
        if last_log_term != my_last_term:
            return last_log_term > my_last_term
        return last_log_index >= my_last_index
    
    def append_entry(self, command: str, key: str, value: Any = None,
                    ttl: Optional[int] = None) -> Optional[LogEntry]:
        """
        Append a new entry to the log (leader only).
        
        Returns:
            The log entry if successful, None otherwise
        """
        with self._lock:
            if self.role != NodeRole.LEADER:
                return None
            
            entry = LogEntry(
                term=self.current_term,
                index=len(self.log) + 1,
                command=command,
                key=key,
                value=value,
                ttl=ttl,
                timestamp=time.time()
            )
            
            self.log.append(entry)
            return entry
    
    def handle_append_entries(self, leader_id: str, term: int,
                             prev_log_index: int, prev_log_term: int,
                             entries: List[Dict], leader_commit: int) -> tuple:
        """
        Handle append entries RPC.
        
        Returns:
            (term, success, match_index)
        """
        with self._lock:
            # Reject if term is old
            if term < self.current_term:
                return self.current_term, False, 0
            
            # Accept leader
            self.receive_heartbeat(leader_id, term)
            
            # Check log consistency
            if prev_log_index > 0:
                if len(self.log) < prev_log_index:
                    return self.current_term, False, len(self.log)
                if self.log[prev_log_index - 1].term != prev_log_term:
                    # Delete conflicting entries
                    self.log = self.log[:prev_log_index - 1]
                    return self.current_term, False, len(self.log)
            
            # Append new entries
            for entry_data in entries:
                entry = LogEntry.from_dict(entry_data)
                if entry.index <= len(self.log):
                    # Already have this entry, check for conflict
                    if self.log[entry.index - 1].term != entry.term:
                        self.log = self.log[:entry.index - 1]
                        self.log.append(entry)
                else:
                    self.log.append(entry)
            
            # Update commit index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log))
                self._apply_committed_entries()
            
            return self.current_term, True, len(self.log)
    
    def _apply_committed_entries(self):
        """Apply committed entries to state machine."""
        with self._lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied - 1]
                
                if self._apply_entry:
                    try:
                        self._apply_entry(entry)
                    except Exception as e:
                        print(f"Error applying entry: {e}")
    
    def commit_entries(self, match_indices: Dict[str, int], cluster_size: int):
        """
        Update commit index based on match indices (leader only).
        Called after receiving replication acknowledgments.
        """
        with self._lock:
            if self.role != NodeRole.LEADER:
                return
            
            # Find the highest index replicated on a majority
            all_indices = list(match_indices.values()) + [len(self.log)]
            all_indices.sort(reverse=True)
            
            majority = cluster_size // 2 + 1
            
            for idx in all_indices:
                if idx <= self.commit_index:
                    break
                    
                # Count nodes with this index or higher
                count = sum(1 for i in all_indices if i >= idx)
                
                if count >= majority:
                    # Check that entry at idx is from current term
                    if idx > 0 and idx <= len(self.log):
                        if self.log[idx - 1].term == self.current_term:
                            self.commit_index = idx
                            self._apply_committed_entries()
                    break
    
    def get_state(self) -> Dict:
        """Get current election state."""
        with self._lock:
            return {
                "node_id": self.node_id,
                "role": self.role.value,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "voted_for": self.voted_for,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied
            }
    
    def is_leader(self) -> bool:
        """Check if this node is the leader."""
        with self._lock:
            return self.role == NodeRole.LEADER
    
    def get_term(self) -> int:
        """Get current term."""
        with self._lock:
            return self.current_term
