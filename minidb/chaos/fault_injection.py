"""
Fault injection framework for testing distributed system resilience.
"""

import time
import random
import threading
from typing import Dict, List, Optional, Set, Callable, Any
from dataclasses import dataclass, field
from enum import Enum


class FaultType(Enum):
    """Types of faults that can be injected."""
    NETWORK_DELAY = "NETWORK_DELAY"         # Add latency to network calls
    NETWORK_DROP = "NETWORK_DROP"           # Drop network packets
    NETWORK_PARTITION = "NETWORK_PARTITION" # Isolate node from others
    DISK_SLOW = "DISK_SLOW"                 # Slow down disk I/O
    DISK_FAIL = "DISK_FAIL"                 # Fail disk operations
    PROCESS_PAUSE = "PROCESS_PAUSE"         # Pause processing
    MEMORY_PRESSURE = "MEMORY_PRESSURE"     # Simulate memory pressure
    CLOCK_SKEW = "CLOCK_SKEW"               # Skew system time
    LEADER_KILL = "LEADER_KILL"             # Kill leader node
    RANDOM_CRASH = "RANDOM_CRASH"           # Random node crash


@dataclass
class Fault:
    """A fault configuration."""
    fault_id: str
    fault_type: FaultType
    target_nodes: List[str] = field(default_factory=list)  # Empty = all nodes
    probability: float = 1.0  # 0.0 to 1.0
    duration_seconds: float = 0  # 0 = until cleared
    parameters: Dict[str, Any] = field(default_factory=dict)
    start_time: Optional[float] = None
    active: bool = True


@dataclass
class FaultStats:
    """Statistics for fault injection."""
    total_faults_injected: int = 0
    network_delays_injected: int = 0
    packets_dropped: int = 0
    disk_failures_injected: int = 0
    partitions_created: int = 0
    leader_kills: int = 0


class FaultInjector:
    """
    Fault injection system for testing distributed database resilience.
    
    Features:
    - Network fault simulation (delay, drops, partitions)
    - Disk fault simulation
    - Process faults (pause, crash)
    - Configurable probability and duration
    - Per-node targeting
    """
    
    def __init__(self, node_id: str, enabled: bool = False):
        self.node_id = node_id
        self.enabled = enabled
        
        self._lock = threading.RLock()
        self._faults: Dict[str, Fault] = {}
        self._partitioned_from: Set[str] = set()
        self._stats = FaultStats()
        
        # Callbacks for applying faults
        self._on_leader_kill: Optional[Callable[[], None]] = None
        self._on_pause: Optional[Callable[[float], None]] = None
        self._on_crash: Optional[Callable[[], None]] = None
    
    def set_callbacks(self, on_leader_kill=None, on_pause=None, on_crash=None):
        """Set fault callbacks."""
        self._on_leader_kill = on_leader_kill
        self._on_pause = on_pause
        self._on_crash = on_crash
    
    def enable(self):
        """Enable fault injection."""
        self.enabled = True
    
    def disable(self):
        """Disable fault injection."""
        self.enabled = False
    
    def inject_fault(self, fault: Fault) -> bool:
        """
        Inject a fault.
        
        Args:
            fault: The fault to inject
            
        Returns:
            True if fault was injected
        """
        if not self.enabled:
            return False
        
        with self._lock:
            fault.start_time = time.time()
            self._faults[fault.fault_id] = fault
            self._stats.total_faults_injected += 1
            
            # Apply immediate effects
            if fault.fault_type == FaultType.NETWORK_PARTITION:
                target = fault.parameters.get("partition_from", [])
                self._partitioned_from.update(target)
                self._stats.partitions_created += 1
            
            elif fault.fault_type == FaultType.LEADER_KILL:
                if self._on_leader_kill:
                    self._on_leader_kill()
                self._stats.leader_kills += 1
            
            elif fault.fault_type == FaultType.PROCESS_PAUSE:
                duration = fault.parameters.get("pause_duration", 5.0)
                if self._on_pause:
                    threading.Thread(
                        target=self._on_pause, 
                        args=(duration,),
                        daemon=True
                    ).start()
            
            elif fault.fault_type == FaultType.RANDOM_CRASH:
                if random.random() < fault.probability:
                    if self._on_crash:
                        self._on_crash()
        
        return True
    
    def clear_fault(self, fault_id: str) -> bool:
        """
        Clear a specific fault.
        
        Args:
            fault_id: The fault to clear
            
        Returns:
            True if fault was cleared
        """
        with self._lock:
            if fault_id in self._faults:
                fault = self._faults[fault_id]
                
                # Remove effects
                if fault.fault_type == FaultType.NETWORK_PARTITION:
                    target = fault.parameters.get("partition_from", [])
                    self._partitioned_from.difference_update(target)
                
                del self._faults[fault_id]
                return True
        return False
    
    def clear_all_faults(self):
        """Clear all faults."""
        with self._lock:
            self._faults.clear()
            self._partitioned_from.clear()
    
    def should_fail_network(self, target_node: str) -> bool:
        """
        Check if network to target should fail.
        
        Args:
            target_node: The target node
            
        Returns:
            True if network call should fail
        """
        if not self.enabled:
            return False
        
        with self._lock:
            # Check partition
            if target_node in self._partitioned_from:
                return True
            
            # Check network drop faults
            for fault in self._faults.values():
                if not fault.active:
                    continue
                if self._is_expired(fault):
                    continue
                    
                if fault.fault_type == FaultType.NETWORK_DROP:
                    if self._targets_node(fault, target_node):
                        if random.random() < fault.probability:
                            self._stats.packets_dropped += 1
                            return True
        
        return False
    
    def get_network_delay(self, target_node: str) -> float:
        """
        Get network delay to add for target.
        
        Args:
            target_node: The target node
            
        Returns:
            Delay in seconds
        """
        if not self.enabled:
            return 0.0
        
        total_delay = 0.0
        
        with self._lock:
            for fault in self._faults.values():
                if not fault.active:
                    continue
                if self._is_expired(fault):
                    continue
                    
                if fault.fault_type == FaultType.NETWORK_DELAY:
                    if self._targets_node(fault, target_node):
                        if random.random() < fault.probability:
                            delay = fault.parameters.get("delay_ms", 100) / 1000.0
                            jitter = fault.parameters.get("jitter_ms", 0) / 1000.0
                            total_delay += delay + random.uniform(-jitter, jitter)
                            self._stats.network_delays_injected += 1
        
        return max(0, total_delay)
    
    def should_fail_disk(self) -> bool:
        """
        Check if disk operation should fail.
        
        Returns:
            True if disk operation should fail
        """
        if not self.enabled:
            return False
        
        with self._lock:
            for fault in self._faults.values():
                if not fault.active:
                    continue
                if self._is_expired(fault):
                    continue
                    
                if fault.fault_type == FaultType.DISK_FAIL:
                    if random.random() < fault.probability:
                        self._stats.disk_failures_injected += 1
                        return True
        
        return False
    
    def get_disk_delay(self) -> float:
        """
        Get disk I/O delay.
        
        Returns:
            Delay in seconds
        """
        if not self.enabled:
            return 0.0
        
        with self._lock:
            for fault in self._faults.values():
                if not fault.active:
                    continue
                if self._is_expired(fault):
                    continue
                    
                if fault.fault_type == FaultType.DISK_SLOW:
                    if random.random() < fault.probability:
                        return fault.parameters.get("delay_ms", 50) / 1000.0
        
        return 0.0
    
    def get_clock_skew(self) -> float:
        """
        Get clock skew offset.
        
        Returns:
            Time offset in seconds
        """
        if not self.enabled:
            return 0.0
        
        with self._lock:
            for fault in self._faults.values():
                if not fault.active:
                    continue
                if self._is_expired(fault):
                    continue
                    
                if fault.fault_type == FaultType.CLOCK_SKEW:
                    return fault.parameters.get("skew_seconds", 0.0)
        
        return 0.0
    
    def _is_expired(self, fault: Fault) -> bool:
        """Check if a fault has expired."""
        if fault.duration_seconds <= 0:
            return False
        if fault.start_time is None:
            return False
        return time.time() - fault.start_time > fault.duration_seconds
    
    def _targets_node(self, fault: Fault, node_id: str) -> bool:
        """Check if fault targets a specific node."""
        if not fault.target_nodes:
            return True  # Targets all nodes
        return node_id in fault.target_nodes
    
    def get_active_faults(self) -> List[Dict]:
        """Get list of active faults."""
        with self._lock:
            return [
                {
                    "fault_id": f.fault_id,
                    "type": f.fault_type.value,
                    "targets": f.target_nodes,
                    "probability": f.probability,
                    "active": f.active and not self._is_expired(f),
                    "parameters": f.parameters
                }
                for f in self._faults.values()
            ]
    
    def get_stats(self) -> Dict:
        """Get fault injection statistics."""
        return {
            "enabled": self.enabled,
            "total_faults": self._stats.total_faults_injected,
            "network_delays": self._stats.network_delays_injected,
            "packets_dropped": self._stats.packets_dropped,
            "disk_failures": self._stats.disk_failures_injected,
            "partitions": self._stats.partitions_created,
            "leader_kills": self._stats.leader_kills,
            "active_faults": len(self._faults)
        }


# Convenience functions for creating faults
def create_network_delay(delay_ms: int = 100, jitter_ms: int = 20,
                         probability: float = 1.0,
                         targets: List[str] = None) -> Fault:
    """Create a network delay fault."""
    return Fault(
        fault_id=f"net-delay-{int(time.time()*1000)}",
        fault_type=FaultType.NETWORK_DELAY,
        target_nodes=targets or [],
        probability=probability,
        parameters={"delay_ms": delay_ms, "jitter_ms": jitter_ms}
    )


def create_network_partition(partition_from: List[str],
                            duration: float = 0) -> Fault:
    """Create a network partition fault."""
    return Fault(
        fault_id=f"partition-{int(time.time()*1000)}",
        fault_type=FaultType.NETWORK_PARTITION,
        duration_seconds=duration,
        parameters={"partition_from": partition_from}
    )


def create_packet_drop(probability: float = 0.1,
                      targets: List[str] = None) -> Fault:
    """Create a packet drop fault."""
    return Fault(
        fault_id=f"drop-{int(time.time()*1000)}",
        fault_type=FaultType.NETWORK_DROP,
        target_nodes=targets or [],
        probability=probability
    )


def create_disk_failure(probability: float = 1.0) -> Fault:
    """Create a disk failure fault."""
    return Fault(
        fault_id=f"disk-fail-{int(time.time()*1000)}",
        fault_type=FaultType.DISK_FAIL,
        probability=probability
    )


def create_leader_kill() -> Fault:
    """Create a leader kill fault."""
    return Fault(
        fault_id=f"leader-kill-{int(time.time()*1000)}",
        fault_type=FaultType.LEADER_KILL
    )
