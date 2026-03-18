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
    updated_at: float = 0.0
    created_at: float = 0.0
    expires_at: Optional[float] = None
    coordinator_id: str = ""


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
    - Primary-owner reads (STRONG)
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
        self._get_local_value: Optional[Callable[[str], Tuple[Any, int, bool, Dict[str, Any]]]] = None
        self._get_remote_value: Optional[Callable[[str, str], Tuple[Any, int, bool, Dict[str, Any]]]] = None
        self._get_replicas: Optional[Callable[[str], List[str]]] = None
        self._get_primary_owner: Optional[Callable[[str], Optional[str]]] = None
        self._repair_value: Optional[Callable[[str, Any, int, Optional[str], Dict[str, Any]], None]] = None
    
    def set_callbacks(self, get_local_value=None, get_remote_value=None,
                      get_replicas=None, get_primary_owner=None,
                      repair_value=None):
        """Set read coordinator callbacks."""
        self._get_local_value = get_local_value
        self._get_remote_value = get_remote_value
        self._get_replicas = get_replicas
        self._get_primary_owner = get_primary_owner
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
            metadata["updated_at"] = result.updated_at
            metadata["coordinator_id"] = result.coordinator_id
            return result.value, True, metadata
        
        return None, False, metadata
    
    def _read_strong(self, key: str) -> Optional[ReadResult]:
        """
        Read from the primary owner only.
        """
        self._stats["leader_reads"] += 1
        
        primary_owner = self._get_primary_owner(key) if self._get_primary_owner else self.local_node_id
        if not primary_owner:
            return None

        if primary_owner == self.local_node_id:
            return self._read_local(key)

        result = self._read_remote(key, primary_owner)
        if result:
            return result
        
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
        
        # Find the most recent version deterministically
        best_result = max(results, key=self._result_sort_key)
        if best_result.version == 0 and best_result.value is None:
            return None
        
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
        best_result = max(results, key=self._result_sort_key)
        if best_result.version == 0 and best_result.value is None:
            return None
        
        versions = set(r.version for r in results)
        if len(versions) > 1:
            self._stats["stale_reads_detected"] += 1
            self._trigger_read_repair(key, best_result, results)
        
        return best_result
    
    def _read_any(self, key: str, allow_stale: bool = False) -> Optional[ReadResult]:
        """
        Read from any available replica (prefer local).
        """
        replicas = self._get_replicas(key) if self._get_replicas else [self.local_node_id]

        # Try local first only when this node is an actual replica
        if self.local_node_id in replicas:
            local_result = self._read_local(key)
            if local_result:
                self._stats["local_reads"] += 1
                return local_result
        
        # Try remote replicas
        for replica in replicas:
            if replica != self.local_node_id:
                result = self._read_remote(key, replica)
                if result:
                    self._stats["remote_reads"] += 1
                    return result
        
        return None
    
    def _read_local(self, key: str, include_missing: bool = False) -> Optional[ReadResult]:
        """Read from local store."""
        if not self._get_local_value:
            return None
        
        value, version, found, metadata = self._get_local_value(key)
        if found:
            return ReadResult(
                value=value,
                version=version,
                node_id=self.local_node_id,
                is_stale=False,
                updated_at=metadata.get("updated_at", 0.0),
                created_at=metadata.get("created_at", 0.0),
                expires_at=metadata.get("expires_at"),
                coordinator_id=metadata.get("coordinator_id", "")
            )
        if include_missing:
            return ReadResult(
                value=None,
                version=version,
                node_id=self.local_node_id,
                is_stale=True,
                updated_at=metadata.get("updated_at", 0.0),
                created_at=metadata.get("created_at", 0.0),
                expires_at=metadata.get("expires_at"),
                coordinator_id=metadata.get("coordinator_id", "")
            )
        return None
    
    def _read_remote(self, key: str, node_id: str, include_missing: bool = False) -> Optional[ReadResult]:
        """Read from a remote node."""
        if not self._get_remote_value:
            return None
        
        try:
            value, version, found, metadata = self._get_remote_value(key, node_id)
            if found:
                return ReadResult(
                    value=value,
                    version=version,
                    node_id=node_id,
                    is_stale=False,
                    updated_at=metadata.get("updated_at", 0.0),
                    created_at=metadata.get("created_at", 0.0),
                    expires_at=metadata.get("expires_at"),
                    coordinator_id=metadata.get("coordinator_id", "")
                )
            if include_missing:
                return ReadResult(
                    value=None,
                    version=version,
                    node_id=node_id,
                    is_stale=True,
                    updated_at=metadata.get("updated_at", 0.0),
                    created_at=metadata.get("created_at", 0.0),
                    expires_at=metadata.get("expires_at"),
                    coordinator_id=metadata.get("coordinator_id", "")
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
                result = self._read_local(key, include_missing=True)
                if result:
                    results.append(result)
            else:
                # Submit remote reads
                future = self._executor.submit(self._read_remote, key, replica, True)
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
            if self._result_sort_key(result) < self._result_sort_key(best_result):
                # Repair this replica
                try:
                    self._repair_value(
                        key,
                        best_result.value,
                        best_result.version,
                        result.node_id,
                        {
                            "updated_at": best_result.updated_at,
                            "created_at": best_result.created_at,
                            "expires_at": best_result.expires_at,
                            "coordinator_id": best_result.coordinator_id
                        }
                    )
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
                    _, remote_version, found, _ = self._get_remote_value(key, replica)
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

    @staticmethod
    def _result_sort_key(result: ReadResult) -> Tuple[int, float, str]:
        """Order results deterministically by version, time, and coordinator."""
        return (result.version, result.updated_at, result.coordinator_id or result.node_id)
