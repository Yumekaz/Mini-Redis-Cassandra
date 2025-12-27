"""
Mini-Distributed Database
A production-grade distributed, fault-tolerant, in-memory key-value database.
"""

__version__ = "1.0.0"
__author__ = "The Mini-Redis Cassandra Contributors"

from .config import NodeConfig, ConsistencyLevel, NodeRole, NodeState
from .node import DatabaseNode, create_node
from .cli import DatabaseCLI, run_cli

__all__ = [
    # Config
    'NodeConfig',
    'ConsistencyLevel',
    'NodeRole',
    'NodeState',
    # Node
    'DatabaseNode',
    'create_node',
    # CLI
    'DatabaseCLI',
    'run_cli',
]
