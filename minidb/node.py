"""
Main database node implementation.
"""

import time
import threading
import socket
from typing import Dict, List, Optional, Any, Tuple

from .config import NodeConfig, ConsistencyLevel, NodeRole
from .storage import KVStore, AOFPersistence, SnapshotPersistence
from .storage.aof import AOFCommand, AOFEntry
from .network import TCPServer, Protocol, Message, MessageType
from .cluster import ClusterCoordinator, NodeInfo, ReadCoordinator
from .cluster.election import LogEntry
from .sharding import ConsistentHashRing, PartitionManager, ShardMigrationManager, ShardRouter
from .repair import AntiEntropyManager
from .testing import (
    FaultInjector, RateLimiter, TokenBucketLimiter, BackpressureManager,
    create_network_delay, create_network_partition, create_packet_drop
)


class DatabaseNode:
    """
    A complete database node with all components.
    
    Features:
    - In-memory KV store with TTL
    - AOF and snapshot persistence
    - Cluster coordination
    - Consistent hashing
    - Anti-entropy repair
    - Custom TCP protocol
    - Fault injection (testing)
    - Rate limiting & backpressure
    - Consistency-aware reads
    - Shard migration
    """
    
    def __init__(self, config: NodeConfig):
        self.config = config
        self.node_id = config.node_id
        
        # Core storage
        self.store = KVStore()
        
        # Persistence
        if config.aof_enabled:
            self.aof = AOFPersistence(
                config.node_data_dir,
                fsync_interval=config.aof_fsync_interval
            )
        else:
            self.aof = None
        
        if config.snapshot_enabled:
            self.snapshot = SnapshotPersistence(
                config.node_data_dir,
                interval=config.snapshot_interval
            )
        else:
            self.snapshot = None
        
        # Sharding
        self.ring = ConsistentHashRing(virtual_nodes=config.virtual_nodes)
        self.partitions = PartitionManager(
            config.node_id,
            self.ring,
            replication_factor=config.replication_factor
        )
        
        self.router = ShardRouter(
            config.node_id,
            self.ring,
            replication_factor=config.replication_factor
        )
        
        self.migration = ShardMigrationManager(
            config.node_id,
            self.ring,
            replication_factor=config.replication_factor
        )
        self._setup_migration_callbacks()
        
        
        self.fault_injector = FaultInjector(config.node_id, enabled=False)
        self._setup_fault_callbacks()
        
        # Cluster coordination
        self.cluster = ClusterCoordinator(config, self.fault_injector)
        self.cluster.set_apply_callback(self._apply_log_entry)
        
        self.read_coordinator = ReadCoordinator(
            config.node_id,
            replication_factor=config.replication_factor
        )
        self._setup_read_callbacks()
        
        # Anti-entropy
        self.repair = AntiEntropyManager(
            config.node_id,
            repair_interval=config.repair_interval
        )
        self._setup_repair_callbacks()
        

        
        self.rate_limiter = TokenBucketLimiter(rate=1000.0, burst=100)
        self.backpressure = BackpressureManager(self.rate_limiter)
        
        # TCP servers
        self.client_server = TCPServer(
            config.host,
            config.client_port,
            self._handle_client_message
        )
        
        self.cluster_server = TCPServer(
            config.host,
            config.cluster_port,
            self._handle_cluster_message
        )
        
        # Background tasks
        self._running = False
        self._maintenance_thread: Optional[threading.Thread] = None
        
        # Add self to ring
        self.ring.add_node(config.node_id)
    
    def _setup_migration_callbacks(self):
        """Set up shard migration callbacks."""
        self.migration.set_callbacks(
            get_local_keys=lambda: self.store.get_all_data(),
            send_keys=self._send_migration_keys,
            receive_keys=self._receive_migration_keys,
            delete_keys=self._delete_migrated_keys
        )
    
    def _setup_read_callbacks(self):
        """Set up read coordinator callbacks."""
        self.read_coordinator.set_callbacks(
            get_local_value=self._get_local_value_for_read,
            get_remote_value=self._get_remote_value_for_read,
            get_replicas=lambda key: self.ring.get_nodes(key, self.config.replication_factor),
            get_leader=lambda: self.cluster.get_leader_id(),
            is_leader=lambda: self.cluster.is_leader(),
            repair_value=self._repair_value
        )
    
    def _setup_repair_callbacks(self):
        """Set up anti-entropy callbacks."""
        self.repair.set_callbacks(
            get_local_data=self._get_data_for_repair,
            get_peers=lambda: [n.node_id for n in self.cluster.membership.get_alive_nodes()],
            apply_repair_data=self._apply_repair_data
        )
    
    def _setup_fault_callbacks(self):
        """Set up fault injection callbacks."""
        self.fault_injector.set_callbacks(
            on_leader_kill=self._handle_leader_kill,
            on_pause=self._handle_pause,
            on_crash=self._handle_crash
        )
    
    def start(self):
        """Start the database node."""
        print(f"[{self.node_id}] Starting database node...")
        
        # Load data from persistence
        self._recover_data()
        
        # Start servers
        self.client_server.start()
        self.cluster_server.start()
        
        # Start cluster coordinator
        self.cluster.start()
        
        # Start anti-entropy
        self.repair.start()
        
        self.migration.start()
        
        # Start maintenance thread
        self._running = True
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            daemon=True
        )
        self._maintenance_thread.start()
        
        print(f"[{self.node_id}] Database node started")
        print(f"[{self.node_id}] Client port: {self.config.client_port}")
        print(f"[{self.node_id}] Cluster port: {self.config.cluster_port}")
    
    def stop(self):
        """Stop the database node."""
        print(f"[{self.node_id}] Stopping database node...")
        
        self._running = False
        
        self.migration.stop()
        
        self.read_coordinator.shutdown()
        
        # Stop anti-entropy
        self.repair.stop()
        
        # Stop cluster coordinator
        self.cluster.stop()
        
        # Stop servers
        self.client_server.stop()
        self.cluster_server.stop()
        
        # Final snapshot
        if self.snapshot:
            self.snapshot.save(self.store.get_all_data())
        
        # Close AOF
        if self.aof:
            self.aof.close()
        
        print(f"[{self.node_id}] Database node stopped")
    
    def _recover_data(self):
        """Recover data from persistence on startup."""
        # First, try loading snapshot
        if self.snapshot:
            snapshot_data = self.snapshot.load()
            if snapshot_data:
                self.store.load_data(snapshot_data)
                print(f"[{self.node_id}] Loaded {len(snapshot_data)} keys from snapshot")
        
        # Then replay AOF for any changes since snapshot
        if self.aof:
            def apply_aof_entry(entry: AOFEntry):
                if entry.command == AOFCommand.SET:
                    ttl = entry.ttl
                    if entry.ttl and hasattr(entry, 'timestamp'):
                        # Adjust TTL based on time elapsed
                        elapsed = time.time() - entry.timestamp
                        ttl = max(0, entry.ttl - int(elapsed))
                        if ttl <= 0:
                            return  # Already expired
                    self.store.set(entry.key, entry.value, ttl, entry.version)
                elif entry.command == AOFCommand.DELETE:
                    self.store.delete(entry.key)
            
            count = self.aof.replay(apply_aof_entry)
            print(f"[{self.node_id}] Replayed {count} AOF entries")
    
    def _maintenance_loop(self):
        """Background maintenance tasks."""
        while self._running:
            try:
                # TTL cleanup
                self.store.cleanup_expired_keys()
                
                # Periodic snapshot
                if self.snapshot and self.snapshot.should_snapshot():
                    data = self.store.get_all_data()
                    if self.snapshot.save(data):
                        print(f"[{self.node_id}] Snapshot saved: {len(data)} keys")
                        
                        # Compact AOF after snapshot
                        if self.aof and self.aof.should_compact():
                            self.aof.compact(data)
                            print(f"[{self.node_id}] AOF compacted")
                
                self.backpressure.update_queue_depth(
                    "pending_writes", 
                    self.cluster.replication.get_pending_count()
                )
                
                time.sleep(self.config.ttl_check_interval)
                
            except Exception as e:
                print(f"[{self.node_id}] Maintenance error: {e}")
                time.sleep(1)
    
    def _handle_client_message(self, message: Message, client_socket: socket.socket) -> Message:
        """Handle incoming client message."""
        if not self.backpressure.should_accept_request(message.sender_id or "default"):
            return Protocol.create_response(False, error="Rate limited - try again later")
        
        cmd, args = Protocol.parse_command(message.payload)
        
        try:
            if cmd == "SET":
                return self._handle_set(args)
            elif cmd == "SETEX":
                return self._handle_setex(args)
            elif cmd == "GET":
                return self._handle_get(args)
            elif cmd == "DELETE" or cmd == "DEL":
                return self._handle_delete(args)
            elif cmd == "EXISTS":
                return self._handle_exists(args)
            elif cmd == "KEYS":
                return self._handle_keys(args)
            elif cmd == "INFO":
                return self._handle_info()
            elif cmd == "CLUSTER":
                return self._handle_cluster_info()
            elif cmd == "PING":
                return Protocol.create_response(True, data="PONG")
            elif cmd == "NODES":
                return self._handle_nodes()
            elif cmd == "LEADER":
                return self._handle_leader()
            elif cmd == "RING":
                return self._handle_ring(args)
            elif cmd == "SHARDS":
                return self._handle_shards()
            elif cmd == "REPLICAS":
                return self._handle_replicas(args)
            elif cmd == "ROUTE":
                return self._handle_route(args)
            elif cmd == "STATS":
                return self._handle_stats()
            elif cmd == "REBALANCE":
                return self._handle_rebalance()
            elif cmd == "MIGRATE":
                return self._handle_migrate(args)
            elif cmd == "FAILOVER":
                return self._handle_failover()
            elif cmd == "FAULT":
                return self._handle_fault(args)
            elif cmd == "RATELIMIT":
                return self._handle_ratelimit(args)
            else:
                return Protocol.create_response(False, error=f"Unknown command: {cmd}")
                
        except Exception as e:
            return Protocol.create_response(False, error=str(e))
    
    def _handle_set(self, args: List[str]) -> Message:
        """Handle SET command."""
        if len(args) < 2:
            return Protocol.create_response(False, error="SET requires key and value")
        
        key = args[0]
        value = args[1]
        ttl = int(args[2]) if len(args) > 2 else None
        
        # Check if we're the leader (for writes)
        if not self.cluster.is_leader():
            leader_addr = self.cluster.get_leader_address()
            if leader_addr:
                return Protocol.create_response(
                    False, 
                    error=f"Not leader. Redirect to {leader_addr}"
                )
            return Protocol.create_response(False, error="No leader available")
        
        # Replicate and apply
        success = self.cluster.append_and_replicate(
            "SET", key, value, ttl, self.config.default_consistency
        )
        
        if success:
            return Protocol.create_response(True, data="OK")
        return Protocol.create_response(False, error="Replication failed")
    
    def _handle_setex(self, args: List[str]) -> Message:
        """Handle SETEX command."""
        if len(args) < 3:
            return Protocol.create_response(False, error="SETEX requires key, ttl, and value")
        
        key = args[0]
        try:
            ttl = int(args[1])
        except ValueError:
            return Protocol.create_response(False, error="TTL must be an integer")
        value = args[2]
        
        # Check if we're the leader
        if not self.cluster.is_leader():
            leader_addr = self.cluster.get_leader_address()
            if leader_addr:
                return Protocol.create_response(
                    False, 
                    error=f"Not leader. Redirect to {leader_addr}"
                )
            return Protocol.create_response(False, error="No leader available")
        
        # Replicate and apply
        success = self.cluster.append_and_replicate(
            "SET", key, value, ttl, self.config.default_consistency
        )
        
        if success:
            return Protocol.create_response(True, data="OK")
        return Protocol.create_response(False, error="Replication failed")
    
    def _handle_get(self, args: List[str]) -> Message:
        """Handle GET command with consistency levels."""
        if len(args) < 1:
            return Protocol.create_response(False, error="GET requires key")
        
        key = args[0]
        
        # Parse consistency level if provided
        consistency = ConsistencyLevel.ANY
        if len(args) > 1:
            try:
                consistency = ConsistencyLevel(args[1].upper())
            except ValueError:
                pass
        
        # Use read coordinator for consistency-aware reads
        value, found, metadata = self.read_coordinator.read(key, consistency)
        
        if found:
            return Protocol.create_response(True, data=value)
        return Protocol.create_response(True, data=None)
    
    def _handle_delete(self, args: List[str]) -> Message:
        """Handle DELETE command."""
        if len(args) < 1:
            return Protocol.create_response(False, error="DELETE requires key")
        
        key = args[0]
        
        if not self.cluster.is_leader():
            leader_addr = self.cluster.get_leader_address()
            if leader_addr:
                return Protocol.create_response(
                    False,
                    error=f"Not leader. Redirect to {leader_addr}"
                )
            return Protocol.create_response(False, error="No leader available")
        
        success = self.cluster.append_and_replicate(
            "DELETE", key, None, None, self.config.default_consistency
        )
        
        if success:
            return Protocol.create_response(True, data="OK")
        return Protocol.create_response(False, error="Replication failed")
    
    def _handle_exists(self, args: List[str]) -> Message:
        """Handle EXISTS command."""
        if len(args) < 1:
            return Protocol.create_response(False, error="EXISTS requires key")
        
        key = args[0]
        exists = self.store.exists(key)
        return Protocol.create_response(True, data=1 if exists else 0)
    
    def _handle_keys(self, args: List[str]) -> Message:
        """Handle KEYS command."""
        pattern = args[0] if args else "*"
        keys = self.store.keys(pattern)
        return Protocol.create_response(True, data=keys)
    
    def _handle_info(self) -> Message:
        """Handle INFO command."""
        info = {
            "node_id": self.node_id,
            "role": self.cluster.election.role.value,
            "leader_id": self.cluster.get_leader_id(),
            "term": self.cluster.election.current_term,
            "keys": self.store.size(),
            "stats": self.store.get_stats(),
            "cluster_size": self.cluster.membership.node_count(),
            "uptime": self.store.get_stats().get("uptime_seconds", 0)
        }
        return Protocol.create_response(True, data=info)
    
    def _handle_cluster_info(self) -> Message:
        """Handle CLUSTER command."""
        return Protocol.create_response(True, data=self.cluster.get_cluster_info())
    
    def _handle_nodes(self) -> Message:
        """Handle NODES command."""
        nodes = [n.to_dict() for n in self.cluster.membership.get_all_nodes()]
        return Protocol.create_response(True, data={"nodes": nodes})
    
    def _handle_leader(self) -> Message:
        """Handle LEADER command."""
        leader_id = self.cluster.get_leader_id()
        leader_addr = self.cluster.get_leader_address()
        return Protocol.create_response(True, data={
            "leader_id": leader_id,
            "leader_address": leader_addr,
            "is_self": leader_id == self.node_id
        })
    
    def _handle_ring(self, args: List[str]) -> Message:
        """Handle RING command."""
        samples = int(args[0]) if args else 20
        ring_state = self.ring.get_ring_state()[:samples]
        return Protocol.create_response(True, data={
            "ring": ring_state,
            "node_count": self.ring.get_node_count(),
            "virtual_nodes": self.config.virtual_nodes
        })
    
    def _handle_shards(self) -> Message:
        """Handle SHARDS command."""
        # Get key distribution across nodes
        all_keys = list(self.store.keys("*"))
        distribution = self.ring.get_key_distribution(all_keys)
        
        return Protocol.create_response(True, data={
            "total_keys": len(all_keys),
            "distribution": distribution,
            "local_keys": len([k for k in all_keys if self.partitions.is_local_key(k)]),
            "primary_keys": len([k for k in all_keys if self.partitions.is_primary_owner(k)])
        })
    
    def _handle_replicas(self, args: List[str]) -> Message:
        """Handle REPLICAS command."""
        if not args:
            # Return general replication status
            return Protocol.create_response(True, data={
                "replication_factor": self.config.replication_factor,
                "default_consistency": self.config.default_consistency.name,
                "pending_requests": self.cluster.replication.get_pending_count(),
                "async_queue_size": self.cluster.replication.get_async_queue_size()
            })
        
        key = args[0]
        replicas = self.ring.get_nodes(key, self.config.replication_factor)
        owner = self.ring.get_node(key)
        
        return Protocol.create_response(True, data={
            "key": key,
            "primary": owner,
            "replicas": replicas,
            "is_local_replica": self.node_id in replicas
        })
    
    def _handle_route(self, args: List[str]) -> Message:
        """Handle ROUTE command."""
        if not args:
            return Protocol.create_response(False, error="ROUTE requires key")
        
        key = args[0]
        routing_info = self.router.get_routing_info(key)
        return Protocol.create_response(True, data=routing_info)
    
    def _handle_stats(self) -> Message:
        """Handle STATS command."""
        stats = {
            "storage": self.store.get_stats(),
            "cluster": {
                "members": self.cluster.membership.node_count(),
                "leader": self.cluster.get_leader_id(),
                "term": self.cluster.election.current_term
            },
            "replication": {
                "pending": self.cluster.replication.get_pending_count(),
                "async_queue": self.cluster.replication.get_async_queue_size()
            },
            "reads": self.read_coordinator.get_stats(),
            "rate_limit": self.rate_limiter.get_stats(),
            "backpressure": {
                "level": self.backpressure.get_backpressure_level()
            },
            "migration": self.migration.get_migration_status()
        }
        return Protocol.create_response(True, data=stats)
    
    def _handle_rebalance(self) -> Message:
        """Handle REBALANCE command."""
        tasks = self.migration.calculate_rebalance_migrations()
        return Protocol.create_response(True, data={
            "migrations_started": len(tasks),
            "status": self.migration.get_migration_status()
        })
    
    def _handle_migrate(self, args: List[str]) -> Message:
        """Handle MIGRATE command."""
        return Protocol.create_response(True, data=self.migration.get_migration_status())
    
    def _handle_failover(self) -> Message:
        """Handle FAILOVER command - force election."""
        if not self.cluster.is_leader():
            # If not leader, forward to leader or error
            leader_addr = self.cluster.get_leader_address()
            if leader_addr:
                return Protocol.create_response(False, error=f"Not leader. Redirect to {leader_addr}")
            return Protocol.create_response(False, error="No leader available")
        
        # We are leader, so step down to force election
        print(f"[{self.node_id}] Received FAILOVER command - stepping down")
        self.cluster.election.step_down()
        
        return Protocol.create_response(True, data={
            "message": "Stepping down as leader",
            "old_term": self.cluster.election.current_term
        })
    
    def _handle_fault(self, args: List[str]) -> Message:
        """Handle FAULT command for fault injection."""
        if not args:
            return Protocol.create_response(True, data=self.fault_injector.get_stats())
        
        action = args[0].upper()
        
        if action == "ENABLE":
            self.fault_injector.enable()
            return Protocol.create_response(True, data="Fault injection enabled")
        elif action == "DISABLE":
            self.fault_injector.disable()
            return Protocol.create_response(True, data="Fault injection disabled")
        elif action == "CLEAR":
            self.fault_injector.clear_all_faults()
            return Protocol.create_response(True, data="All faults cleared")
        elif action == "PARTITION":
            # FAULT PARTITION <node_id>
            if len(args) < 2:
                return Protocol.create_response(False, error="Usage: FAULT PARTITION <node_id>")
            target = args[1]
            fault = create_network_partition(partition_from=[target])
            self.fault_injector.inject_fault(fault)
            return Protocol.create_response(True, data=f"Partitioned from {target}")
            
        elif action == "LIST":
            return Protocol.create_response(True, data=self.fault_injector.get_active_faults())
        
        elif action == "DELAY":
            # FAULT DELAY <ms> [jitter]
            if len(args) < 2:
                return Protocol.create_response(False, error="Usage: FAULT DELAY <ms> [jitter]")
            try:
                ms = int(args[1])
                jitter = int(args[2]) if len(args) > 2 else 0
                fault = create_network_delay(delay_ms=ms, jitter_ms=jitter)
                self.fault_injector.inject_fault(fault)
                return Protocol.create_response(True, data=f"Injected {ms}ms delay")
            except ValueError:
                return Protocol.create_response(False, error="Invalid delay value")
                
        elif action == "DROP":
            # FAULT DROP <percentage 0-100>
            if len(args) < 2:
                return Protocol.create_response(False, error="Usage: FAULT DROP <percentage>")
            try:
                pct = float(args[1])
                if pct > 1.0: pct /= 100.0  # Handle 50 as 0.5
                fault = create_packet_drop(probability=pct)
                self.fault_injector.inject_fault(fault)
                return Protocol.create_response(True, data=f"Injected {pct*100}% packet drop")
            except ValueError:
                return Protocol.create_response(False, error="Invalid percentage")
        
        return Protocol.create_response(False, error=f"Unknown fault action: {action}")
    
    def _handle_ratelimit(self, args: List[str]) -> Message:
        """Handle RATELIMIT command."""
        return Protocol.create_response(True, data=self.backpressure.get_stats())
    
    
    def _handle_cluster_message(self, message: Message, client_socket: socket.socket) -> Optional[Message]:
        """Handle incoming cluster message."""
        if self.fault_injector.should_fail_network(message.sender_id):
            return None  # Drop message
        
        # Add network delay if configured
        delay = self.fault_injector.get_network_delay(message.sender_id)
        if delay > 0:
            time.sleep(delay)
        
        return self.cluster.handle_cluster_message(message)
    
    def _apply_log_entry(self, entry: LogEntry):
        """Apply a committed log entry to the store."""
        if entry.command == "SET":
            self.store.set(entry.key, entry.value, entry.ttl)
            if self.aof:
                self.aof.append(AOFCommand.SET, entry.key, entry.value, entry.ttl)
        elif entry.command == "DELETE":
            self.store.delete(entry.key)
            if self.aof:
                self.aof.append(AOFCommand.DELETE, entry.key)
    
    def _get_data_for_repair(self) -> Dict[str, Tuple]:
        """Get data for anti-entropy repair."""
        data = self.store.get_all_data()
        return {k: (v[0], v[2]) for k, v in data.items()}  # key -> (value, version)
    
    def _apply_repair_data(self, key: str, value: Any, version: int):
        """Apply repaired data from another node."""
        current_version = self.store.get_version(key)
        if current_version is None or version > current_version:
            self.store.set(key, value, version=version)
            if self.aof:
                self.aof.append(AOFCommand.SET, key, value, version=version)
    
    def _get_local_value_for_read(self, key: str) -> Tuple[Any, int, bool]:
        """Get local value for read coordinator."""
        value, found = self.store.get(key)
        version = self.store.get_version(key) or 0
        return value, version, found
    
    def _get_remote_value_for_read(self, key: str, node_id: str) -> Tuple[Any, int, bool]:
        """Get remote value for read coordinator."""
        node = self.cluster.membership.get_node(node_id)
        if not node:
            return None, 0, False
        
        try:
            from .network.client import TCPClient
            client = TCPClient(node.host, node.client_port, timeout=5.0)
            if client.connect():
                response = client.send_command("GET", key)
                client.disconnect()
                if response and response.payload.get("success"):
                    value = response.payload.get("data")
                    return value, 0, value is not None
        except Exception:
            pass
        
        return None, 0, False
    
    def _repair_value(self, key: str, value: Any, version: int):
        """Repair a stale value."""
        self._apply_repair_data(key, value, version)
    
    def _send_migration_keys(self, target_node: str, keys_data: Dict[str, Any]) -> bool:
        """Send migrated keys to target node."""
        node = self.cluster.membership.get_node(target_node)
        if not node:
            return False
        
        try:
            from .network.client import TCPClient
            client = TCPClient(node.host, node.client_port, timeout=30.0)
            if client.connect():
                for key, data in keys_data.items():
                    value = data[0] if isinstance(data, tuple) else data
                    response = client.send_command("SET", key, str(value))
                client.disconnect()
                return True
        except Exception:
            pass
        
        return False
    
    def _receive_migration_keys(self, keys_data: Dict[str, Any]) -> bool:
        """Receive migrated keys."""
        for key, data in keys_data.items():
            value = data[0] if isinstance(data, tuple) else data
            self.store.set(key, value)
        return True
    
    def _delete_migrated_keys(self, keys: List[str]):
        """Delete migrated keys after successful transfer."""
        for key in keys:
            self.store.delete(key)
    
    def _handle_leader_kill(self):
        """Handle leader kill fault."""
        if self.cluster.is_leader():
            print(f"[{self.node_id}] FAULT: Leader killed, stepping down")
            self.cluster.election.step_down()
    
    def _handle_pause(self, duration: float):
        """Handle process pause fault."""
        print(f"[{self.node_id}] FAULT: Pausing for {duration}s")
        time.sleep(duration)
        print(f"[{self.node_id}] FAULT: Resumed")
    
    def _handle_crash(self):
        """Handle crash fault."""
        print(f"[{self.node_id}] FAULT: Simulated crash")
        self.stop()
    
    def join_cluster(self, seed_address: str) -> bool:
        """Join an existing cluster."""
        return self.cluster.join_cluster(seed_address)


def create_node(node_id: str, client_port: int = 7001, 
                cluster_port: int = 8001, **kwargs) -> DatabaseNode:
    """
    Factory function to create a database node.
    
    Args:
        node_id: Unique identifier for this node
        client_port: Port for client connections
        cluster_port: Port for cluster communication
        **kwargs: Additional config options
        
    Returns:
        Configured DatabaseNode instance
    """
    config = NodeConfig(
        node_id=node_id,
        client_port=client_port,
        cluster_port=cluster_port,
        **kwargs
    )
    return DatabaseNode(config)
