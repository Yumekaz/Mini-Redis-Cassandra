"""
Read coordinator for consistency-aware distributed reads.
"""

import time
import threading
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

from ..config import ConsistencyLevel


@dataclass
class ReadResult:
    """Result of a read operation."""
    value: Any
    version: int
    node_id: str
    is_stale: bool = False
    read_time: float = 0.0


@dataclass
class QuorumReadResult:
    """Result of a quorum read."""
    value: Any
    version: int
    responses: List[ReadResult]
    achieved_consistency: ConsistencyLevel
    is_consistent: bool = True


class ReadCoordinator:
    """
    Coordinates distributed reads with tunable consistency.
    
    Features:
    - Leader-only reads (STRONG)
    - Quorum reads (QUORUM)
    - Any replica reads (ONE, ANY)
    - Stale read detection
    - Read repair on inconsistency
    """
    
    def __init__(self, local_node_id: str, replication_factor: int = 3,
                 read_timeout: float = 5.0):
        self.local_node_id = local_node_id
        self.replication_factor = replication_factor
        self.read_timeout = read_timeout
        
        self._executor = ThreadPoolExecutor(max_workers=10)
        self._lock = threading.RLock()
        
        # Stats
        self._stats = {
            "total_reads": 0,
            "local_reads": 0,
            "remote_reads": 0,
            "quorum_reads": 0,
            "leader_reads": 0,
            "stale_reads_detected": 0,
            "read_repairs": 0
        }
        
        # Callbacks
        self._get_local_value: Optional[Callable[[str], Tuple[Any, int, bool]]] = None
        self._get_remote_value: Optional[Callable[[str, str], Tuple[Any, int, bool]]] = None
        self._get_replicas: Optional[Callable[[str], List[str]]] = None
        self._get_leader: Optional[Callable[[], Optional[str]]] = None
        self._is_leader: Optional[Callable[[], bool]] = None
        self._repair_value: Optional[Callable[[str, Any, int], None]] = None
    
    def set_callbacks(self, get_local_value=None, get_remote_value=None,
                      get_replicas=None, get_leader=None, is_leader=None,
                      repair_value=None):
        """Set read coordinator callbacks."""
        self._get_local_value = get_local_value
        self._get_remote_value = get_remote_value
        self._get_replicas = get_replicas
        self._get_leader = get_leader
        self._is_leader = is_leader
        self._repair_value = repair_value
    
    def read(self, key: str, consistency: ConsistencyLevel = ConsistencyLevel.ANY,
             allow_stale: bool = False) -> Tuple[Any, bool, Dict]:
        """
        Read a value with specified consistency level.
        
        Args:
            key: The key to read
            consistency: Desired consistency level
            allow_stale: Whether to return potentially stale data
            
        Returns:
            Tuple of (value, found, metadata)
        """
        self._stats["total_reads"] += 1
        start_time = time.time()
        
        if consistency == ConsistencyLevel.STRONG:
            result = self._read_strong(key)
        elif consistency == ConsistencyLevel.QUORUM:
            result = self._read_quorum(key)
        elif consistency == ConsistencyLevel.ALL:
            result = self._read_all(key)
        else:  # ONE or ANY
            result = self._read_any(key, allow_stale)
        
        read_time = time.time() - start_time
        
        metadata = {
            "consistency": consistency.value,
            "read_time_ms": read_time * 1000,
            "node": self.local_node_id
        }
        
        if result:
            metadata["version"] = result.version
            metadata["from_node"] = result.node_id
            metadata["is_stale"] = result.is_stale
            return result.value, True, metadata
        
        return None, False, metadata
    
    def _read_strong(self, key: str) -> Optional[ReadResult]:
        """
        Read from leader only (strong consistency).
        """
        self._stats["leader_reads"] += 1
        
        # If we're the leader, read locally
        if self._is_leader and self._is_leader():
            return self._read_local(key)
        
        # Otherwise, read from leader
        if self._get_leader:
            leader_id = self._get_leader()
            if leader_id and self._get_remote_value:
                value, version, found = self._get_remote_value(key, leader_id)
                if found:
                    return ReadResult(
                        value=value,
                        version=version,
                        node_id=leader_id,
                        is_stale=False
                    )
        
        return None
    
    def _read_quorum(self, key: str) -> Optional[ReadResult]:
        """
        Read from quorum of replicas.
        """
        self._stats["quorum_reads"] += 1
        
        if not self._get_replicas:
            return self._read_local(key)
        
        replicas = self._get_replicas(key)
        quorum_size = len(replicas) // 2 + 1
        
        # Collect responses from replicas
        results = self._read_from_replicas(key, replicas, quorum_size)
        
        if len(results) < quorum_size:
            return None
        
        # Find the most recent version
        best_result = max(results, key=lambda r: r.version)
        
        # Check for inconsistency and trigger read repair
        versions = set(r.version for r in results)
        if len(versions) > 1:
            self._stats["stale_reads_detected"] += 1
            self._trigger_read_repair(key, best_result, results)
        
        return best_result
    
    def _read_all(self, key: str) -> Optional[ReadResult]:
        """
        Read from all replicas.
        """
        if not self._get_replicas:
            return self._read_local(key)
        
        replicas = self._get_replicas(key)
        results = self._read_from_replicas(key, replicas, len(replicas))
        
        if len(results) < len(replicas):
            return None  # Not all replicas responded
        
        # Find most recent and check consistency
        best_result = max(results, key=lambda r: r.version)
        
        versions = set(r.version for r in results)
        if len(versions) > 1:
            self._stats["stale_reads_detected"] += 1
            self._trigger_read_repair(key, best_result, results)
        
        return best_result
    
    def _read_any(self, key: str, allow_stale: bool = False) -> Optional[ReadResult]:
        """
        Read from any available replica (prefer local).
        """
        # Try local first
        local_result = self._read_local(key)
        if local_result:
            self._stats["local_reads"] += 1
            return local_result
        
        # Try remote replicas
        if self._get_replicas:
            replicas = self._get_replicas(key)
            for replica in replicas:
                if replica != self.local_node_id:
                    result = self._read_remote(key, replica)
                    if result:
                        self._stats["remote_reads"] += 1
                        return result
        
        return None
    
    def _read_local(self, key: str) -> Optional[ReadResult]:
        """Read from local store."""
        if not self._get_local_value:
            return None
        
        value, version, found = self._get_local_value(key)
        if found:
            return ReadResult(
                value=value,
                version=version,
                node_id=self.local_node_id,
                is_stale=False
            )
        return None
    
    def _read_remote(self, key: str, node_id: str) -> Optional[ReadResult]:
        """Read from a remote node."""
        if not self._get_remote_value:
            return None
        
        try:
            value, version, found = self._get_remote_value(key, node_id)
            if found:
                return ReadResult(
                    value=value,
                    version=version,
                    node_id=node_id,
                    is_stale=False
                )
        except Exception:
            pass
        return None
    
    def _read_from_replicas(self, key: str, replicas: List[str], 
                           min_responses: int) -> List[ReadResult]:
        """
        Read from multiple replicas in parallel.
        """
        results = []
        futures: Dict[Future, str] = {}
        
        for replica in replicas:
            if replica == self.local_node_id:
                # Read local immediately
                result = self._read_local(key)
                if result:
                    results.append(result)
            else:
                # Submit remote reads
                future = self._executor.submit(self._read_remote, key, replica)
                futures[future] = replica
        
        # Wait for remote results
        try:
            for future in as_completed(futures.keys(), timeout=self.read_timeout):
                result = future.result()
                if result:
                    results.append(result)
                    if len(results) >= min_responses:
                        break
        except Exception:
            pass
        
        return results
    
    def _trigger_read_repair(self, key: str, best_result: ReadResult,
                            all_results: List[ReadResult]):
        """
        Trigger read repair for inconsistent replicas.
        """
        if not self._repair_value:
            return
        
        self._stats["read_repairs"] += 1
        
        # Find stale replicas
        for result in all_results:
            if result.version < best_result.version:
                # Repair this replica
                try:
                    self._repair_value(key, best_result.value, best_result.version)
                except Exception:
                    pass
    
    def detect_stale_read(self, key: str, local_version: int) -> bool:
        """
        Detect if local data is stale by checking other replicas.
        
        Args:
            key: The key to check
            local_version: Current local version
            
        Returns:
            True if local data is stale
        """
        if not self._get_replicas or not self._get_remote_value:
            return False
        
        replicas = self._get_replicas(key)
        
        for replica in replicas:
            if replica != self.local_node_id:
                try:
                    _, remote_version, found = self._get_remote_value(key, replica)
                    if found and remote_version > local_version:
                        self._stats["stale_reads_detected"] += 1
                        return True
                except Exception:
                    pass
        
        return False
    
    def get_stats(self) -> Dict:
        """Get read coordinator statistics."""
        return dict(self._stats)
    
    def shutdown(self):
        """Shutdown the executor."""
        self._executor.shutdown(wait=False)
