"""
Configuration management for the distributed database.
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class ConsistencyLevel(Enum):
    """Tunable consistency levels."""
    ONE = "ONE"           # Single node acknowledgment
    QUORUM = "QUORUM"     # Majority acknowledgment
    ALL = "ALL"           # All replicas acknowledge
    ANY = "ANY"           # Fire and forget
    STRONG = "STRONG"     # Leader-only strict consistency


class NodeRole(Enum):
    """Node roles in the cluster."""
    LEADER = "LEADER"
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"


class NodeState(Enum):
    """Node health states."""
    ALIVE = "ALIVE"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"


@dataclass
class NodeConfig:
    """Configuration for a single node."""
    node_id: str
    host: str = "localhost"
    client_port: int = 7001      # Port for client connections
    cluster_port: int = 8001     # Port for inter-node communication
    data_dir: str = "./data"
    
    # Persistence settings
    aof_enabled: bool = True
    aof_fsync_interval: float = 1.0  # seconds
    snapshot_enabled: bool = True
    snapshot_interval: float = 300.0  # 5 minutes
    
    # Cluster settings
    heartbeat_interval: float = 1.0  # seconds
    failure_timeout: float = 5.0     # seconds before marking node suspect
    dead_timeout: float = 15.0       # seconds before marking node dead
    
    # Election settings
    election_timeout_min: float = 1.5  # seconds
    election_timeout_max: float = 3.0  # seconds
    
    # Replication settings
    replication_factor: int = 3
    default_consistency: ConsistencyLevel = ConsistencyLevel.QUORUM
    
    # Sharding settings
    virtual_nodes: int = 150  # Virtual nodes per physical node
    
    # TTL settings
    ttl_check_interval: float = 1.0  # seconds
    
    # Anti-entropy settings
    repair_interval: float = 60.0  # seconds
    
    def __post_init__(self):
        """Create data directory if it doesn't exist."""
        node_data_dir = os.path.join(self.data_dir, self.node_id)
        os.makedirs(node_data_dir, exist_ok=True)
        self.node_data_dir = node_data_dir


@dataclass
class ClusterConfig:
    """Configuration for the entire cluster."""
    seed_nodes: List[str] = field(default_factory=list)  # ["host:port", ...]
    min_nodes_for_quorum: int = 2
    
    def add_seed_node(self, address: str):
        """Add a seed node address."""
        if address not in self.seed_nodes:
            self.seed_nodes.append(address)


def get_default_config(node_id: str, port: int = 7001, cluster_port: int = 8001) -> NodeConfig:
    """Get default configuration for a node."""
    return NodeConfig(
        node_id=node_id,
        client_port=port,
        cluster_port=cluster_port,
        data_dir=f"./data_{node_id}"
    )
