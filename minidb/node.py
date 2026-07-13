"""
Main database node implementation.
"""

import json
import time
import threading
import socket
from typing import Dict, List, Optional, Any, Tuple

from .config import NodeConfig, ConsistencyLevel, NodeRole, NodeState
from .storage import KVStore, AOFPersistence, SnapshotPersistence
from .storage.aof import AOFCommand, AOFEntry
from .network import TCPServer, Protocol, Message, MessageType
from .cluster import ClusterCoordinator, NodeInfo, ReadCoordinator
from .cluster.election import LogEntry
from .sharding import ConsistentHashRing, PartitionManager, ShardMigrationManager, ShardRouter
from .repair import AntiEntropyManager
from .chaos import (
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
        self.router.update_node_address(self.node_id, f"{self.config.host}:{self.config.client_port}")
        self.router.set_node_health(self.node_id, True)
        self._known_ring_nodes = {self.node_id}
        self._write_sequence = 0
        self._write_lock = threading.RLock()
    
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
            # STRONG reads the write leader only — never a hash-ring fallback
            # that may hold unreplicated/orphan REPLICATE data.
            get_primary_owner=lambda key: self.cluster.get_leader_id(),
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
        self._sync_topology()
        
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
                    expires_at = entry.expires_at
                    if expires_at is None and entry.ttl and hasattr(entry, 'timestamp'):
                        expires_at = entry.timestamp + entry.ttl
                    if expires_at and expires_at <= time.time():
                        return
                    self.store.set(
                        entry.key,
                        entry.value,
                        version=entry.version,
                        expires_at=expires_at,
                        created_at=entry.created_at,
                        updated_at=entry.timestamp,
                        coordinator_id=entry.coordinator_id
                    )
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
                self._sync_topology()
                
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
                return self._handle_info(args)
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
            elif cmd == "__LEADER_WRITE":
                return self._handle_leader_write(args)
            elif cmd == "__OWNER_WRITE":
                return self._handle_owner_write(args)
            elif cmd == "__LOCAL_READ":
                return self._handle_local_read(args)
            elif cmd == "__LOCAL_KEYS":
                return self._handle_local_keys(args)
            elif cmd == "__REPLICA_SET":
                return self._handle_replica_set(args)
            elif cmd == "__IMPORT_KEYS":
                return self._handle_import_keys(args)
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
        try:
            ttl = int(args[2]) if len(args) > 2 else None
        except ValueError:
            return Protocol.create_response(False, error="TTL must be an integer")

        return self._coordinate_or_forward_write("SET", key, value=value, ttl=ttl)
    
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

        return self._coordinate_or_forward_write("SET", key, value=value, ttl=ttl)
    
    def _handle_get(self, args: List[str]) -> Message:
        """Handle GET command with consistency levels."""
        if len(args) < 1:
            return Protocol.create_response(False, error="GET requires key")
        
        key = args[0]
        
        # Default to configured consistency (QUORUM under FailForge). ANY here
        # previously caused read-after-write violations: QUORUM writes only need
        # majority, so a local-only GET could hit a lagging replica.
        consistency = self.config.default_consistency
        if len(args) > 1:
            try:
                consistency = ConsistencyLevel(args[1].upper())
            except ValueError:
                pass
        
        # Serialize with in-flight leader writes so GETs never observe a
        # half-applied write that will later fail and leave no client ACK.
        with self._write_lock:
            value, found, metadata = self.read_coordinator.read(key, consistency)

            if found:
                return Protocol.create_response(True, data=value)

            # Quorum/strong/all: unreachable majority is an error, not a null hit.
            if metadata.get("quorum_failed"):
                return Protocol.create_response(
                    False,
                    error=f"Quorum read failed for key {key}"
                )

            return Protocol.create_response(True, data=None)

    def _handle_delete(self, args: List[str]) -> Message:
        """Handle DELETE command."""
        if len(args) < 1:
            return Protocol.create_response(False, error="DELETE requires key")
        
        key = args[0]

        return self._coordinate_or_forward_write("DELETE", key)

    def _coordinate_or_forward_write(self, operation: str, key: str,
                                     value: Any = None,
                                     ttl: Optional[int] = None) -> Message:
        """
        Route every write through the current election leader.

        Hash-ring co-primaries previously allowed two nodes to ACK concurrent
        SETs for the same key (FailForge corrupt/stale residual). Single-leader
        coordination serializes the write path.
        """
        if not self.cluster.is_leader():
            leader_id = self.cluster.get_leader_id()
            if not leader_id:
                return Protocol.create_response(False, error="No cluster leader for write")
            return self._forward_write(leader_id, operation, key, value=value, ttl=ttl)

        return self._apply_partition_write(operation, key, value=value, ttl=ttl)

    def _forward_write(self, node_id: str, operation: str, key: str,
                       value: Any = None,
                       ttl: Optional[int] = None) -> Message:
        """Forward a write to the coordinating leader (or legacy primary owner)."""
        node = self.cluster.membership.get_node(node_id)
        if not node or node.state != NodeState.ALIVE:
            return Protocol.create_response(False, error=f"Write coordinator unavailable for key: {key}")

        try:
            from .network.client import TCPClient
            client = TCPClient(node.host, node.client_port, timeout=10.0)
            if not client.connect():
                return Protocol.create_response(False, error=f"Failed to reach write coordinator {node_id}")

            if operation == "SET":
                response = client.send_command(
                    "__LEADER_WRITE",
                    operation,
                    key,
                    json.dumps(value),
                    "" if ttl is None else str(ttl)
                )
            else:
                response = client.send_command("__LEADER_WRITE", operation, key)

            client.disconnect()
            if response:
                # Stale leader: retry once against the current election winner.
                err = (response.payload or {}).get("error") or ""
                if (not response.payload.get("success")) and "Leader mismatch" in err:
                    leader_id = self.cluster.get_leader_id()
                    if leader_id and leader_id != node_id:
                        return self._forward_write(leader_id, operation, key, value=value, ttl=ttl)
                return response
        except Exception:
            pass

        return Protocol.create_response(False, error=f"Failed to forward write for key: {key}")

    def _apply_partition_write(self, operation: str, key: str,
                               value: Any = None,
                               ttl: Optional[int] = None) -> Message:
        """Coordinate a write for the key's replica set from the primary owner."""
        if not self.cluster.is_leader():
            return Protocol.create_response(False, error=f"This node is not the write leader for {key}")

        # Refuse writes until the ring can place a full replica set for RF.
        # Early single-node ownership produced false local-only successes.
        ring_size = self.ring.get_node_count()
        if ring_size < self.config.replication_factor:
            return Protocol.create_response(
                False,
                error=(
                    f"Cluster not ready for {operation}: ring has {ring_size} "
                    f"nodes, need {self.config.replication_factor}"
                ),
            )

        # Prefer full membership as the replica set so ALL/QUORUM track the
        # live cluster, not a stale ring after restarts.
        alive = [n.node_id for n in self.cluster.membership.get_alive_nodes()]
        if self.node_id not in alive:
            alive = [self.node_id] + alive
        replicas = alive[: max(self.config.replication_factor, len(alive))]
        if self.node_id not in replicas:
            replicas = [self.node_id] + [r for r in replicas if r != self.node_id]

        with self._write_lock:
            current = self.store.get_with_metadata(key)
            version = (current.version + 1) if current else 1
            now = time.time() + self.fault_injector.get_clock_skew()
            created_at = current.created_at if current else now
            expires_at = now + ttl if ttl is not None else None
            entry = LogEntry(
                term=self.cluster.election.current_term,
                index=self._next_write_index(),
                command=operation,
                key=key,
                value=value,
                ttl=ttl,
                timestamp=now,
                version=version,
                expires_at=expires_at,
                created_at=created_at,
                coordinator_id=self.node_id
            )

            remote_replicas = [replica for replica in replicas if replica != self.node_id]
            alive_ids = {
                n.node_id
                for n in self.cluster.membership.get_alive_nodes()
                if n.node_id != self.node_id
            }
            targets = [r for r in remote_replicas if r in alive_ids]
            # Require every ring replica to be membership-alive before acking a
            # sync write. Partial rings left durable orphans that FailForge
            # scored as corrupt "never successfully written" values.
            if remote_replicas and len(targets) < len(remote_replicas):
                missing = sorted(set(remote_replicas) - set(targets))
                return Protocol.create_response(
                    False,
                    error=(
                        f"Replication targets unavailable for {operation}: "
                        f"missing {missing} (replica set {replicas})"
                    ),
                )

            # Replicate first (followers apply on REPLICATE). Only apply on the
            # leader after enough remote acks so concurrent GETs never observe a
            # never-acked local value. If replicate fails, no remote acked under
            # our sync send loop, so nothing durable is left on remotes.
            success = self.cluster.replication.replicate(
                entry,
                self.config.default_consistency,
                targets=targets
            )

            if success:
                # Commit staged prepares on remotes, then apply on the leader so
                # a value is never client-visible (STRONG/QUORUM) without an ACK path.
                if hasattr(self.cluster, "commit_to_followers"):
                    self.cluster.commit_to_followers(entry, targets)
                self._apply_log_entry(entry)
                return Protocol.create_response(True, data="OK")

            return Protocol.create_response(
                False,
                error=f"Replication failed for {operation} on replica set {replicas}"
            )

    def _revert_partition_write(self, key: str, operation: str, previous) -> None:
        """Undo a primary-local apply after a failed sync replicate."""
        if previous is None:
            if operation == "SET":
                self.store.delete(key)
            return
        self.store.set(
            key,
            previous.value,
            version=previous.version,
            expires_at=previous.expires_at,
            created_at=previous.created_at,
            updated_at=previous.updated_at,
            coordinator_id=previous.coordinator_id,
        )
    def _handle_exists(self, args: List[str]) -> Message:
        """Handle EXISTS command."""
        if len(args) < 1:
            return Protocol.create_response(False, error="EXISTS requires key")
        
        key = args[0]
        _, exists, _ = self.read_coordinator.read(key, ConsistencyLevel.ANY)
        return Protocol.create_response(True, data=1 if exists else 0)
    
    def _handle_keys(self, args: List[str]) -> Message:
        """Handle KEYS command."""
        pattern = args[0] if args else "*"
        keys = sorted(self._collect_cluster_keys(pattern))
        return Protocol.create_response(True, data=keys)
    
    def _handle_info(self, args: Optional[List[str]] = None) -> Message:
        """Handle INFO command."""
        args = args or []
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

        if args:
            section = args[0].lower()
            sections = {
                "node": {
                    "node_id": info["node_id"],
                    "role": info["role"],
                    "leader_id": info["leader_id"],
                    "term": info["term"],
                    "uptime": info["uptime"]
                },
                "storage": {
                    "keys": info["keys"],
                    "stats": info["stats"]
                },
                "cluster": {
                    "leader_id": info["leader_id"],
                    "cluster_size": info["cluster_size"],
                    "role": info["role"],
                    "term": info["term"]
                },
                "reads": self.read_coordinator.get_stats(),
                "migration": self.migration.get_migration_status()
            }

            if section not in sections:
                return Protocol.create_response(
                    False,
                    error=f"Unknown INFO section: {section}"
                )
            return Protocol.create_response(True, data=sections[section])

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
        all_keys = sorted(self._collect_cluster_keys("*"))
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

    def _handle_leader_write(self, args: List[str]) -> Message:
        """Handle an internal write routed to the election leader (any key)."""
        if len(args) < 2:
            return Protocol.create_response(False, error="__LEADER_WRITE requires command and key")

        operation = args[0].upper()
        key = args[1]
        value = None
        ttl = None

        if operation == "SET":
            if len(args) < 3:
                return Protocol.create_response(False, error="SET requires value")
            try:
                value = json.loads(args[2])
            except json.JSONDecodeError:
                value = args[2]

            if len(args) > 3 and args[3] not in ("", "None", "null"):
                try:
                    ttl = int(args[3])
                except ValueError:
                    return Protocol.create_response(False, error="TTL must be an integer")
        elif operation != "DELETE":
            return Protocol.create_response(False, error=f"Unsupported leader write: {operation}")

        if not self.cluster.is_leader():
            return Protocol.create_response(
                False,
                error=f"Leader mismatch for key {key}: this node is not the write leader"
            )

        return self._apply_partition_write(operation, key, value=value, ttl=ttl)

    def _handle_owner_write(self, args: List[str]) -> Message:
        """Handle an internal write already routed to the primary owner."""
        if len(args) < 2:
            return Protocol.create_response(False, error="__OWNER_WRITE requires command and key")

        operation = args[0].upper()
        key = args[1]
        value = None
        ttl = None

        if operation == "SET":
            if len(args) < 3:
                return Protocol.create_response(False, error="SET requires value")
            try:
                value = json.loads(args[2])
            except json.JSONDecodeError:
                value = args[2]

            if len(args) > 3 and args[3] not in ("", "None", "null"):
                try:
                    ttl = int(args[3])
                except ValueError:
                    return Protocol.create_response(False, error="TTL must be an integer")
        elif operation != "DELETE":
            return Protocol.create_response(False, error=f"Unsupported owner write: {operation}")

        primary_owner = self.ring.get_node(key)
        if primary_owner != self.node_id:
            return Protocol.create_response(
                False,
                error=f"Owner mismatch for key {key}: expected {primary_owner}, got {self.node_id}"
            )

        return self._apply_partition_write(operation, key, value=value, ttl=ttl)

    def _handle_local_read(self, args: List[str]) -> Message:
        """Handle an internal local-only read with metadata."""
        if not args:
            return Protocol.create_response(False, error="__LOCAL_READ requires key")

        metadata = self.store.get_with_metadata(args[0])
        if metadata is None:
            return Protocol.create_response(True, data={
                "found": False,
                "value": None,
                "version": 0,
                "expires_at": None,
                "updated_at": 0.0,
                "created_at": 0.0,
                "coordinator_id": ""
            })

        return Protocol.create_response(True, data={
            "found": True,
            "value": metadata.value,
            "version": metadata.version,
            "expires_at": metadata.expires_at,
            "updated_at": metadata.updated_at,
            "created_at": metadata.created_at,
            "coordinator_id": metadata.coordinator_id
        })

    def _handle_local_keys(self, args: List[str]) -> Message:
        """Return only local keys for internal fan-out commands."""
        pattern = args[0] if args else "*"
        return Protocol.create_response(True, data=self.store.keys(pattern))

    def _handle_replica_set(self, args: List[str]) -> Message:
        """Apply a replica value without owner rerouting."""
        if len(args) < 6:
            return Protocol.create_response(
                False,
                error="__REPLICA_SET requires key, value, version, updated_at, created_at, and coordinator_id"
            )

        key = args[0]

        try:
            value = json.loads(args[1])
        except json.JSONDecodeError:
            value = args[1]

        try:
            version = int(args[2])
        except ValueError:
            return Protocol.create_response(False, error="Version must be an integer")

        expires_at = None
        if args[3] not in ("", "None", "null"):
            try:
                expires_at = float(args[3])
            except ValueError:
                expires_at = None

        try:
            updated_at = float(args[4])
            created_at = float(args[5])
        except ValueError:
            return Protocol.create_response(False, error="Timestamps must be numeric")

        coordinator_id = args[6] if len(args) > 6 else ""
        current = self.store.get_with_metadata(key)
        current_order = self._metadata_sort_key(current) if current else (0, 0.0, "")
        incoming_order = (version, updated_at, coordinator_id)
        if current and incoming_order < current_order:
            return Protocol.create_response(True, data="IGNORED")

        self.store.set(
            key,
            value,
            version=version,
            expires_at=expires_at,
            created_at=created_at,
            updated_at=updated_at,
            coordinator_id=coordinator_id
        )
        if self.aof:
            self.aof.append(
                AOFCommand.SET,
                key,
                value,
                version=version,
                expires_at=expires_at,
                created_at=created_at,
                updated_at=updated_at,
                coordinator_id=coordinator_id
            )
        return Protocol.create_response(True, data="OK")

    def _handle_import_keys(self, args: List[str]) -> Message:
        """Bulk import migrated keys with preserved metadata."""
        if not args:
            return Protocol.create_response(False, error="__IMPORT_KEYS requires a payload")

        try:
            payload = json.loads(args[0])
        except json.JSONDecodeError:
            return Protocol.create_response(False, error="Invalid import payload")

        imported = 0
        skipped = 0
        fanout_items = []

        for key, item in payload.items():
            version = int(item["version"])
            updated_at = float(item.get("updated_at") or time.time())
            created_at = float(item.get("created_at") or updated_at)
            expires_at = item.get("expires_at")
            coordinator_id = item.get("coordinator_id", "")

            current = self.store.get_with_metadata(key)
            current_order = self._metadata_sort_key(current) if current else (0, 0.0, "")
            incoming_order = (version, updated_at, coordinator_id)
            if current and incoming_order < current_order:
                skipped += 1
                continue

            self.store.set(
                key,
                item["value"],
                version=version,
                expires_at=expires_at,
                created_at=created_at,
                updated_at=updated_at,
                coordinator_id=coordinator_id
            )
            if self.aof:
                self.aof.append(
                    AOFCommand.SET,
                    key,
                    item["value"],
                    version=version,
                    expires_at=expires_at,
                    created_at=created_at,
                    updated_at=updated_at,
                    coordinator_id=coordinator_id
                )
            imported += 1

            if self.ring.get_node(key) == self.node_id:
                fanout_items.append(
                    {
                        "key": key,
                        "value": item["value"],
                        "version": version,
                        "expires_at": expires_at,
                        "updated_at": updated_at,
                        "created_at": created_at,
                        "coordinator_id": coordinator_id,
                    }
                )

        replicated = 0
        for item in fanout_items:
            targets = [
                replica for replica in self.ring.get_nodes(item["key"], self.config.replication_factor)
                if replica != self.node_id
            ]
            replicated += self._replicate_value_to_nodes(
                item["key"],
                item["value"],
                item["version"],
                {
                    "expires_at": item["expires_at"],
                    "updated_at": item["updated_at"],
                    "created_at": item["created_at"],
                    "coordinator_id": item["coordinator_id"],
                },
                targets
            )

        return Protocol.create_response(
            True,
            data={"imported": imported, "skipped": skipped, "replicated": replicated}
        )
    
    
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
            self.store.set(
                entry.key,
                entry.value,
                version=entry.version,
                expires_at=entry.expires_at,
                created_at=entry.created_at,
                updated_at=entry.timestamp,
                coordinator_id=entry.coordinator_id
            )
            if self.aof:
                self.aof.append(
                    AOFCommand.SET,
                    entry.key,
                    entry.value,
                    ttl=entry.ttl,
                    version=entry.version,
                    expires_at=entry.expires_at,
                    created_at=entry.created_at,
                    updated_at=entry.timestamp,
                    coordinator_id=entry.coordinator_id
                )
        elif entry.command == "DELETE":
            self.store.delete(entry.key)
            if self.aof:
                self.aof.append(
                    AOFCommand.DELETE,
                    entry.key,
                    version=entry.version,
                    updated_at=entry.timestamp,
                    coordinator_id=entry.coordinator_id
                )
    
    def _get_data_for_repair(self) -> Dict[str, Tuple]:
        """Get data for anti-entropy repair."""
        data = self.store.get_all_data()
        return {k: (v[0], v[2]) for k, v in data.items()}  # key -> (value, version)
    
    def _apply_repair_data(self, key: str, value: Any, version: int,
                           metadata: Optional[Dict[str, Any]] = None):
        """Apply repaired data from another node."""
        metadata = metadata or {}
        current = self.store.get_with_metadata(key)
        updated_at = float(metadata.get("updated_at") or time.time())
        created_at = float(metadata.get("created_at") or updated_at)
        expires_at = metadata.get("expires_at")
        coordinator_id = metadata.get("coordinator_id", self.node_id)

        current_order = self._metadata_sort_key(current) if current else (0, 0.0, "")
        incoming_order = (version, updated_at, coordinator_id)
        if current and incoming_order < current_order:
            return

        self.store.set(
            key,
            value,
            version=version,
            expires_at=expires_at,
            created_at=created_at,
            updated_at=updated_at,
            coordinator_id=coordinator_id
        )
        if self.aof:
            self.aof.append(
                AOFCommand.SET,
                key,
                value,
                version=version,
                expires_at=expires_at,
                created_at=created_at,
                updated_at=updated_at,
                coordinator_id=coordinator_id
            )
    
    def _get_local_value_for_read(self, key: str) -> Tuple[Any, int, bool, Dict[str, Any]]:
        """Get local value for read coordinator."""
        metadata = self.store.get_with_metadata(key)
        if metadata is None:
            return None, 0, False, {"updated_at": 0.0, "coordinator_id": "", "created_at": 0.0}
        return (
            metadata.value,
            metadata.version,
            True,
            {
                "updated_at": metadata.updated_at,
                "coordinator_id": metadata.coordinator_id,
                "expires_at": metadata.expires_at,
                "created_at": metadata.created_at
            }
        )
    
    def _get_remote_value_for_read(self, key: str, node_id: str) -> Tuple[Any, int, bool, Dict[str, Any]]:
        """Get remote value for read coordinator."""
        node = self.cluster.membership.get_node(node_id)
        if not node:
            return None, 0, False, {"updated_at": 0.0, "coordinator_id": "", "created_at": 0.0}
        
        try:
            from .network.client import TCPClient
            client = TCPClient(node.host, node.client_port, timeout=5.0)
            if client.connect():
                response = client.send_command("__LOCAL_READ", key)
                client.disconnect()
                if response and response.payload.get("success"):
                    data = response.payload.get("data", {})
                    return (
                        data.get("value"),
                        int(data.get("version", 0)),
                        bool(data.get("found")),
                        {
                            "updated_at": float(data.get("updated_at", 0.0)),
                            "coordinator_id": data.get("coordinator_id", ""),
                            "expires_at": data.get("expires_at"),
                            "created_at": float(data.get("created_at", 0.0))
                        }
                    )
        except Exception:
            pass
        
        return None, 0, False, {"updated_at": 0.0, "coordinator_id": "", "created_at": 0.0}
    
    def _repair_value(self, key: str, value: Any, version: int,
                      target_node_id: Optional[str] = None,
                      metadata: Optional[Dict[str, Any]] = None):
        """Repair a stale value locally or on a remote replica."""
        metadata = dict(metadata or {})
        if not target_node_id or target_node_id == self.node_id:
            self._apply_repair_data(key, value, version, metadata)
            return

        if not metadata:
            current = self.store.get_with_metadata(key)
            if current:
                metadata = {
                    "updated_at": current.updated_at,
                    "created_at": current.created_at,
                    "expires_at": current.expires_at,
                    "coordinator_id": current.coordinator_id or self.node_id,
                }
            else:
                now = time.time()
                metadata = {
                    "updated_at": now,
                    "created_at": now,
                    "expires_at": None,
                    "coordinator_id": self.node_id,
                }

        self._replicate_value_to_nodes(key, value, version, metadata, [target_node_id])

    def _sync_topology(self):
        """Synchronize ring and routing metadata with cluster membership."""
        members = self.cluster.membership.get_all_nodes()
        active_node_ids = set()

        for member in members:
            if member.state == NodeState.DEAD:
                self.router.remove_node(member.node_id)
                continue

            active_node_ids.add(member.node_id)
            self.router.update_node_address(
                member.node_id,
                f"{member.host}:{member.client_port}"
            )
            self.router.set_node_health(member.node_id, member.state == NodeState.ALIVE)

        new_nodes = active_node_ids - self._known_ring_nodes
        removed_nodes = self._known_ring_nodes - active_node_ids

        for node_id in sorted(new_nodes):
            self.partitions.add_node(node_id)
            self.migration.on_node_join(node_id)

        for node_id in sorted(removed_nodes):
            if node_id == self.node_id:
                continue
            self.partitions.remove_node(node_id)
            self.router.remove_node(node_id)
            self.migration.on_node_leave(node_id)

        self._known_ring_nodes = active_node_ids

    def _collect_cluster_keys(self, pattern: str) -> List[str]:
        """Collect matching keys from all alive nodes and de-duplicate them."""
        keys = set(self.store.keys(pattern))

        for node in self.cluster.membership.get_alive_nodes():
            if node.node_id == self.node_id:
                continue

            try:
                from .network.client import TCPClient
                client = TCPClient(node.host, node.client_port, timeout=5.0)
                if client.connect():
                    response = client.send_command("__LOCAL_KEYS", pattern)
                    client.disconnect()
                    if response and response.payload.get("success"):
                        keys.update(response.payload.get("data", []))
            except Exception:
                pass

        return sorted(keys)

    def _next_write_index(self) -> int:
        """Return the next local write sequence number."""
        with self._write_lock:
            self._write_sequence += 1
            return self._write_sequence

    @staticmethod
    def _metadata_sort_key(metadata) -> Tuple[int, float, str]:
        """Order metadata deterministically."""
        return (
            metadata.version,
            metadata.updated_at,
            metadata.coordinator_id or ""
        )

    def _replicate_value_to_nodes(self, key: str, value: Any, version: int,
                                  metadata: Dict[str, Any],
                                  target_nodes: List[str]) -> int:
        """Replicate an explicit value and metadata to a set of replica nodes."""
        replicated = 0
        expires_at = metadata.get("expires_at")
        updated_at = float(metadata.get("updated_at", time.time()))
        created_at = float(metadata.get("created_at", updated_at))
        coordinator_id = metadata.get("coordinator_id", self.node_id)

        for node_id in target_nodes:
            if node_id == self.node_id:
                continue

            node = self.cluster.membership.get_node(node_id)
            if not node or node.state != NodeState.ALIVE:
                continue

            try:
                from .network.client import TCPClient
                client = TCPClient(node.host, node.client_port, timeout=5.0)
                if client.connect():
                    response = client.send_command(
                        "__REPLICA_SET",
                        key,
                        json.dumps(value),
                        str(version),
                        "" if expires_at is None else str(expires_at),
                        str(updated_at),
                        str(created_at),
                        coordinator_id
                    )
                    client.disconnect()
                    if response and response.payload.get("success"):
                        replicated += 1
            except Exception:
                pass

        return replicated
    
    def _send_migration_keys(self, target_node: str, keys_data: Dict[str, Any]) -> bool:
        """Send migrated keys to target node."""
        node = self.cluster.membership.get_node(target_node)
        if not node:
            return False
        
        try:
            from .network.client import TCPClient
            client = TCPClient(node.host, node.client_port, timeout=30.0)
            if client.connect():
                payload = {}
                for key, data in keys_data.items():
                    if isinstance(data, tuple) and len(data) >= 6:
                        value, expires_at, version, created_at, updated_at, coordinator_id = data[:6]
                    elif isinstance(data, tuple) and len(data) >= 3:
                        value, expires_at, version = data[:3]
                        created_at = updated_at = time.time()
                        coordinator_id = self.node_id
                    else:
                        value = data
                        expires_at = None
                        version = 1
                        created_at = updated_at = time.time()
                        coordinator_id = self.node_id

                    payload[key] = {
                        "value": value,
                        "expires_at": expires_at,
                        "version": version,
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "coordinator_id": coordinator_id
                    }

                response = client.send_command("__IMPORT_KEYS", json.dumps(payload))
                client.disconnect()
                return bool(response and response.payload.get("success"))
        except Exception:
            pass
        
        return False
    
    def _receive_migration_keys(self, keys_data: Dict[str, Any]) -> bool:
        """Receive migrated keys."""
        for key, data in keys_data.items():
            if isinstance(data, tuple) and len(data) >= 6:
                value, expires_at, version, created_at, updated_at, coordinator_id = data[:6]
                self.store.set(
                    key,
                    value,
                    version=version,
                    expires_at=expires_at,
                    created_at=created_at,
                    updated_at=updated_at,
                    coordinator_id=coordinator_id
                )
                if self.aof:
                    self.aof.append(
                        AOFCommand.SET,
                        key,
                        value,
                        version=version,
                        expires_at=expires_at,
                        created_at=created_at,
                        updated_at=updated_at,
                        coordinator_id=coordinator_id
                    )
            else:
                value = data[0] if isinstance(data, tuple) else data
                self.store.set(key, value)
                if self.aof:
                    self.aof.append(AOFCommand.SET, key, value)
        return True
    
    def _delete_migrated_keys(self, keys: List[str]):
        """Delete migrated keys after successful transfer."""
        for key in keys:
            metadata = self.store.get_with_metadata(key)
            self.store.delete(key)
            if self.aof:
                self.aof.append(
                    AOFCommand.DELETE,
                    key,
                    version=metadata.version if metadata else 1,
                    updated_at=time.time(),
                    coordinator_id=metadata.coordinator_id if metadata else self.node_id
                )
    
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
