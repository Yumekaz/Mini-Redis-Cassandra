"""
Mini-Distributed Database
A production-grade distributed, fault-tolerant, in-memory key-value database.
"""

from importlib import import_module

__version__ = "1.0.0"
__author__ = "The Mini-Redis Cassandra Contributors"

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

_EXPORTS = {
    'NodeConfig': ('minidb.config', 'NodeConfig'),
    'ConsistencyLevel': ('minidb.config', 'ConsistencyLevel'),
    'NodeRole': ('minidb.config', 'NodeRole'),
    'NodeState': ('minidb.config', 'NodeState'),
    'DatabaseNode': ('minidb.node', 'DatabaseNode'),
    'create_node': ('minidb.node', 'create_node'),
    'DatabaseCLI': ('minidb.cli', 'DatabaseCLI'),
    'run_cli': ('minidb.cli', 'run_cli'),
}


def __getattr__(name):
    """Lazily expose package-level symbols without pre-importing submodules."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = _EXPORTS[name]
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__():
    """Include lazily exported names in interactive discovery."""
    return sorted(set(globals()) | set(__all__))
