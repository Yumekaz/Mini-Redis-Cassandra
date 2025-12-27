"""
Shard migration manager for handling key movement during rebalancing.
"""

import time
import threading
from typing import Dict, List, Optional, Set, Callable, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

from .consistent_hash import ConsistentHashRing


class MigrationState(Enum):
    """State of a migration task."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class MigrationTask:
    """A single key migration task."""
    task_id: str
    source_node: str
    target_node: str
    keys: List[str]
    state: MigrationState = MigrationState.PENDING
    progress: int = 0  # Number of keys migrated
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None


@dataclass
class MigrationStats:
    """Statistics for migration operations."""
    total_migrations: int = 0
    successful_migrations: int = 0
    failed_migrations: int = 0
    keys_migrated: int = 0
    bytes_transferred: int = 0
    last_migration_time: Optional[float] = None


class ShardMigrationManager:
    """
    Manages shard migration during node joins/leaves.
    
    Features:
    - Identify keys that need migration on topology change
    - Batch key transfer between nodes
    - Progress tracking and resumability
    - Conflict resolution during migration
    """
    
    def __init__(self, local_node_id: str, ring: ConsistentHashRing,
                 replication_factor: int = 3):
        self.local_node_id = local_node_id
        self.ring = ring
        self.replication_factor = replication_factor
        
        self._lock = threading.RLock()
        self._active_migrations: Dict[str, MigrationTask] = {}
        self._migration_history: List[MigrationTask] = []
        self._stats = MigrationStats()
        
        self._running = False
        self._migration_thread: Optional[threading.Thread] = None
        
        # Callbacks
        self._get_local_keys: Optional[Callable[[], Dict[str, Any]]] = None
        self._send_keys: Optional[Callable[[str, Dict[str, Any]], bool]] = None
        self._receive_keys: Optional[Callable[[Dict[str, Any]], bool]] = None
        self._delete_keys: Optional[Callable[[List[str]], None]] = None
    
    def set_callbacks(self, get_local_keys=None, send_keys=None, 
                      receive_keys=None, delete_keys=None):
        """Set migration callbacks."""
        self._get_local_keys = get_local_keys
        self._send_keys = send_keys
        self._receive_keys = receive_keys
        self._delete_keys = delete_keys
    
    def start(self):
        """Start the migration manager."""
        self._running = True
        self._migration_thread = threading.Thread(
            target=self._migration_loop, daemon=True
        )
        self._migration_thread.start()
    
    def stop(self):
        """Stop the migration manager."""
        self._running = False
        if self._migration_thread:
            self._migration_thread.join(timeout=5.0)
    
    def _migration_loop(self):
        """Process pending migrations."""
        while self._running:
            try:
                with self._lock:
                    pending = [
                        t for t in self._active_migrations.values()
                        if t.state == MigrationState.PENDING
                    ]
                
                for task in pending:
                    self._execute_migration(task)
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"[{self.local_node_id}] Migration loop error: {e}")
                time.sleep(1)
    
    def on_node_join(self, new_node_id: str) -> Optional[MigrationTask]:
        """
        Handle a new node joining - identify keys to migrate.
        
        Returns:
            MigrationTask if keys need to be sent to new node
        """
        if not self._get_local_keys:
            return None
        
        local_keys = self._get_local_keys()
        keys_to_migrate = []
        
        # Find keys that should now be owned by the new node
        for key in local_keys.keys():
            new_owner = self.ring.get_node(key)
            if new_owner == new_node_id:
                keys_to_migrate.append(key)
        
        if not keys_to_migrate:
            return None
        
        task_id = f"migrate-{self.local_node_id}-to-{new_node_id}-{int(time.time())}"
        task = MigrationTask(
            task_id=task_id,
            source_node=self.local_node_id,
            target_node=new_node_id,
            keys=keys_to_migrate
        )
        
        with self._lock:
            self._active_migrations[task_id] = task
        
        return task
    
    def on_node_leave(self, leaving_node_id: str) -> List[MigrationTask]:
        """
        Handle a node leaving - identify keys we need to receive.
        
        Returns:
            List of MigrationTasks for keys we should now own
        """
        tasks = []
        
        if not self._get_local_keys:
            return tasks
        
        # Keys that were on the leaving node and should come to us
        # are handled by anti-entropy - this just triggers rebalancing
        
        return tasks
    
    def calculate_rebalance_migrations(self) -> List[MigrationTask]:
        """
        Calculate all migrations needed to rebalance the cluster.
        
        Returns:
            List of MigrationTasks to execute
        """
        if not self._get_local_keys:
            return []
        
        local_keys = self._get_local_keys()
        migrations: Dict[str, List[str]] = {}  # target_node -> keys
        
        for key in local_keys.keys():
            owner = self.ring.get_node(key)
            replicas = self.ring.get_nodes(key, self.replication_factor)
            
            # Check if we should still have this key
            if self.local_node_id not in replicas:
                if owner and owner != self.local_node_id:
                    if owner not in migrations:
                        migrations[owner] = []
                    migrations[owner].append(key)
        
        tasks = []
        for target_node, keys in migrations.items():
            task_id = f"rebalance-{self.local_node_id}-to-{target_node}-{int(time.time())}"
            task = MigrationTask(
                task_id=task_id,
                source_node=self.local_node_id,
                target_node=target_node,
                keys=keys
            )
            tasks.append(task)
            
            with self._lock:
                self._active_migrations[task_id] = task
        
        return tasks
    
    def _execute_migration(self, task: MigrationTask):
        """Execute a migration task."""
        if not self._get_local_keys or not self._send_keys:
            task.state = MigrationState.FAILED
            task.error = "Missing callbacks"
            return
        
        task.state = MigrationState.IN_PROGRESS
        task.started_at = time.time()
        
        try:
            local_keys = self._get_local_keys()
            
            # Batch keys for transfer
            batch_size = 100
            keys_data = {}
            
            for key in task.keys:
                if key in local_keys:
                    keys_data[key] = local_keys[key]
                    
                    if len(keys_data) >= batch_size:
                        if not self._send_keys(task.target_node, keys_data):
                            raise Exception(f"Failed to send batch to {task.target_node}")
                        task.progress += len(keys_data)
                        self._stats.keys_migrated += len(keys_data)
                        keys_data = {}
            
            # Send remaining keys
            if keys_data:
                if not self._send_keys(task.target_node, keys_data):
                    raise Exception(f"Failed to send final batch to {task.target_node}")
                task.progress += len(keys_data)
                self._stats.keys_migrated += len(keys_data)
            
            # Delete migrated keys locally (only if we're not a replica)
            if self._delete_keys:
                non_replica_keys = []
                for key in task.keys:
                    replicas = self.ring.get_nodes(key, self.replication_factor)
                    if self.local_node_id not in replicas:
                        non_replica_keys.append(key)
                if non_replica_keys:
                    self._delete_keys(non_replica_keys)
            
            task.state = MigrationState.COMPLETED
            task.completed_at = time.time()
            self._stats.successful_migrations += 1
            
        except Exception as e:
            task.state = MigrationState.FAILED
            task.error = str(e)
            task.completed_at = time.time()
            self._stats.failed_migrations += 1
        
        self._stats.total_migrations += 1
        self._stats.last_migration_time = time.time()
        
        # Move to history
        with self._lock:
            if task.task_id in self._active_migrations:
                del self._active_migrations[task.task_id]
            self._migration_history.append(task)
            # Keep only last 100 in history
            if len(self._migration_history) > 100:
                self._migration_history = self._migration_history[-100:]
    
    def receive_migration_data(self, keys_data: Dict[str, Any]) -> bool:
        """
        Receive migrated keys from another node.
        
        Args:
            keys_data: Dict of key -> value data
            
        Returns:
            True if received successfully
        """
        if self._receive_keys:
            return self._receive_keys(keys_data)
        return False
    
    def get_migration_status(self) -> Dict:
        """Get current migration status."""
        with self._lock:
            active = [
                {
                    "task_id": t.task_id,
                    "source": t.source_node,
                    "target": t.target_node,
                    "state": t.state.value,
                    "progress": t.progress,
                    "total_keys": len(t.keys)
                }
                for t in self._active_migrations.values()
            ]
            
            return {
                "active_migrations": active,
                "stats": {
                    "total_migrations": self._stats.total_migrations,
                    "successful": self._stats.successful_migrations,
                    "failed": self._stats.failed_migrations,
                    "keys_migrated": self._stats.keys_migrated
                }
            }
    
    def cancel_migration(self, task_id: str) -> bool:
        """Cancel an active migration."""
        with self._lock:
            if task_id in self._active_migrations:
                task = self._active_migrations[task_id]
                if task.state in (MigrationState.PENDING, MigrationState.IN_PROGRESS):
                    task.state = MigrationState.CANCELLED
                    return True
        return False


class RebalanceCoordinator:
    """
    Coordinates cluster-wide rebalancing operations.
    
    Features:
    - Orchestrate multi-node migrations
    - Handle concurrent rebalancing
    - Progress tracking
    """
    
    def __init__(self, local_node_id: str, migration_manager: ShardMigrationManager):
        self.local_node_id = local_node_id
        self.migration_manager = migration_manager
        
        self._rebalancing = False
        self._lock = threading.Lock()
    
    def start_rebalance(self) -> bool:
        """
        Start a cluster rebalance operation.
        
        Returns:
            True if rebalance started
        """
        with self._lock:
            if self._rebalancing:
                return False
            self._rebalancing = True
        
        try:
            tasks = self.migration_manager.calculate_rebalance_migrations()
            print(f"[{self.local_node_id}] Starting rebalance: {len(tasks)} migrations")
            return True
        except Exception as e:
            print(f"[{self.local_node_id}] Rebalance failed: {e}")
            with self._lock:
                self._rebalancing = False
            return False
    
    def is_rebalancing(self) -> bool:
        """Check if rebalancing is in progress."""
        with self._lock:
            return self._rebalancing
    
    def get_rebalance_progress(self) -> Dict:
        """Get rebalance progress."""
        status = self.migration_manager.get_migration_status()
        active = status.get("active_migrations", [])
        
        total_keys = sum(m.get("total_keys", 0) for m in active)
        migrated_keys = sum(m.get("progress", 0) for m in active)
        
        return {
            "is_rebalancing": self._rebalancing,
            "active_migrations": len(active),
            "total_keys": total_keys,
            "migrated_keys": migrated_keys,
            "progress_percent": (migrated_keys / total_keys * 100) if total_keys > 0 else 100
        }
