"""
Failure-oriented tests for shard ownership, persistence, repair, and rebalancing.
"""

import os
import sys
import random
import shutil
import threading
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minidb.config import ConsistencyLevel
from minidb.chaos.fault_injection import create_network_partition
from minidb.network.client import TCPClient
from minidb.node import create_node


BASE_DIR = Path("test_resilience_data")


def assert_success(response, label):
    assert response is not None, f"{label}: no response"
    assert response.payload.get("success"), f"{label}: {response.payload}"
    return response.payload.get("data")


def wait_for(predicate, timeout=12.0, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def build_cluster(node_count=3, replication_factor=3, base_name="cluster", **node_kwargs):
    base_dir = BASE_DIR / base_name
    if base_dir.exists():
        shutil.rmtree(base_dir)
    base_dir.mkdir(parents=True)

    ports = []
    nodes = []
    client_base = 7501 if node_count <= 3 else 7601
    cluster_base = 8501 if node_count <= 3 else 8601

    for index in range(node_count):
        client_port = client_base + index
        cluster_port = cluster_base + index
        ports.append((client_port, cluster_port))
        node = create_node(
            f"node{index + 1}",
            client_port=client_port,
            cluster_port=cluster_port,
            data_dir=str(base_dir),
            replication_factor=replication_factor,
            **node_kwargs,
        )
        node.start()
        nodes.append(node)

    time.sleep(3)
    seed_address = f"localhost:{ports[0][1]}"
    for node in nodes[1:]:
        assert node.join_cluster(seed_address)

    assert wait_for(lambda: all(n.cluster.membership.node_count() == node_count for n in nodes))
    assert wait_for(lambda: all(n.ring.get_node_count() == node_count for n in nodes))
    assert wait_for(lambda: any(n.cluster.is_leader() for n in nodes))

    return nodes, ports, base_dir


def stop_cluster(nodes, base_dir):
    for node in nodes:
        try:
            node.cluster.election._running = False
            node.cluster.replication._running = False
            node.cluster.membership._running = False
            node._running = False
        except Exception:
            pass

    for node in nodes:
        try:
            node.stop()
        except Exception:
            pass

    if base_dir.exists():
        shutil.rmtree(base_dir)


def connect_client(port):
    client = TCPClient("localhost", port)
    assert client.connect(), f"failed to connect to port {port}"
    return client


def get_node(nodes, node_id):
    return next(node for node in nodes if node.node_id == node_id)


def get_port(nodes, ports, node_id):
    index = next(i for i, node in enumerate(nodes) if node.node_id == node_id)
    return ports[index][0]


def local_read(port, key):
    client = connect_client(port)
    try:
        return assert_success(client.send_command("__LOCAL_READ", key), f"__LOCAL_READ {key}")
    finally:
        client.disconnect()


def find_key_for_owner(ring, owner_id, prefix="owner:key", limit=5000):
    for index in range(limit):
        key = f"{prefix}:{index}"
        if ring.get_node(key) == owner_id:
            return key
    raise AssertionError(f"failed to find a key for owner {owner_id}")


def test_partition_write_and_read_repair():
    nodes, ports, base_dir = build_cluster(
        node_count=3,
        replication_factor=3,
        base_name="partition_repair",
        aof_enabled=False,
        snapshot_enabled=False,
        default_consistency=ConsistencyLevel.QUORUM,
    )

    try:
        # Writes are leader-coordinated; partition from the write leader.
        assert wait_for(lambda: sum(1 for n in nodes if n.cluster.is_leader()) == 1)
        leader = next(n for n in nodes if n.cluster.is_leader())
        key = "repair:key"
        stale_replica_id = next(n.node_id for n in nodes if n.node_id != leader.node_id)

        leader.fault_injector.enable()
        leader.fault_injector.inject_fault(create_network_partition([stale_replica_id]))

        write_port = get_port(nodes, ports, leader.node_id)
        client = connect_client(write_port)
        try:
            assert_success(client.send_command("SET", key, "value-1"), "partitioned write") == "OK"
        finally:
            client.disconnect()

        stale_port = get_port(nodes, ports, stale_replica_id)
        stale_before = local_read(stale_port, key)
        assert stale_before["found"] is False

        leader.fault_injector.clear_all_faults()
        leader.fault_injector.disable()

        stale_client = connect_client(stale_port)
        try:
            assert assert_success(stale_client.send_command("GET", key, "QUORUM"), "repair read") == "value-1"
        finally:
            stale_client.disconnect()

        assert wait_for(
            lambda: local_read(stale_port, key)["found"] and local_read(stale_port, key)["value"] == "value-1"
        ), "stale replica was not repaired"

        print("[OK] Partitioned write and read repair test passed")
    finally:
        stop_cluster(nodes, base_dir)


def test_leader_crash_during_replication():
    nodes, ports, base_dir = build_cluster(
        node_count=3,
        replication_factor=3,
        base_name="leader_crash",
        aof_enabled=False,
        snapshot_enabled=False,
        default_consistency=ConsistencyLevel.QUORUM,
    )

    try:
        leader = next(node for node in nodes if node.cluster.is_leader())
        key = find_key_for_owner(nodes[0].ring, leader.node_id, prefix="leader-owned")
        leader_port = get_port(nodes, ports, leader.node_id)

        original_send = leader.cluster.replication._send_replicate
        crash_once = {"done": False}

        def crash_after_first_ack(follower_id, entry):
            success = original_send(follower_id, entry)
            if success and not crash_once["done"]:
                crash_once["done"] = True
                threading.Thread(target=leader.stop, daemon=True).start()
                time.sleep(0.05)
            return success

        leader.cluster.replication._send_replicate = crash_after_first_ack

        client = connect_client(leader_port)
        try:
            response = client.send_command("SET", key, "survives")
        finally:
            client.disconnect()

        assert response is None or response.payload.get("success"), f"unexpected write failure: {response}"

        surviving_nodes = [node for node in nodes if node.node_id != leader.node_id]
        assert wait_for(lambda: any(node.cluster.is_leader() for node in surviving_nodes))

        survivor_port = next(port for port, _ in ports if port != leader_port)
        survivor_client = connect_client(survivor_port)
        try:
            assert assert_success(
                survivor_client.send_command("GET", key, "QUORUM"),
                "post-failover read",
            ) == "survives"
        finally:
            survivor_client.disconnect()

        print("[OK] Leader crash during replication test passed")
    finally:
        stop_cluster(nodes, base_dir)


def test_restart_with_persistence_recovery():
    base_dir = BASE_DIR / "persistence_recovery"
    if base_dir.exists():
        shutil.rmtree(base_dir)
    base_dir.mkdir(parents=True)

    # Single-node persistence: RF=1 so QUORUM is local-only (RF=3 would
    # correctly refuse writes on a solo process after the FailForge RAW fix).
    node = create_node(
        "node1",
        client_port=7701,
        cluster_port=8701,
        data_dir=str(base_dir),
        aof_enabled=True,
        snapshot_enabled=True,
        snapshot_interval=0.2,
        ttl_check_interval=0.1,
        replication_factor=1,
        default_consistency=ConsistencyLevel.ONE,
    )
    node.start()

    try:
        assert wait_for(lambda: node.cluster.is_leader())
        client = connect_client(7701)
        try:
            assert_success(client.send_command("SET", "snap:key", "before-snapshot"), "snap:key")
            time.sleep(0.6)
            assert_success(client.send_command("SET", "tail:key", "after-snapshot"), "tail:key")
            assert_success(client.send_command("SET", "version:key", "v1"), "version:key v1")
            assert_success(client.send_command("SET", "version:key", "v2"), "version:key v2")
            assert_success(client.send_command("SETEX", "ttl:key", "10", "ttl-value"), "ttl:key")
        finally:
            client.disconnect()
    finally:
        node.stop()

    time.sleep(0.6)

    restarted = create_node(
        "node1",
        client_port=7701,
        cluster_port=8701,
        data_dir=str(base_dir),
        replication_factor=1,
        default_consistency=ConsistencyLevel.ONE,
        aof_enabled=True,
        snapshot_enabled=True,
        snapshot_interval=0.2,
        ttl_check_interval=0.1,
    )
    restarted.start()

    try:
        assert wait_for(lambda: restarted.cluster.is_leader())
        client = connect_client(7701)
        try:
            assert assert_success(client.send_command("GET", "snap:key"), "restart snap:key") == "before-snapshot"
            assert assert_success(client.send_command("GET", "tail:key"), "restart tail:key") == "after-snapshot"
            assert assert_success(client.send_command("GET", "version:key"), "restart version:key") == "v2"
        finally:
            client.disconnect()

        version_meta = local_read(7701, "version:key")
        assert version_meta["version"] == 2
        assert version_meta["created_at"] <= version_meta["updated_at"]

        ttl_meta = local_read(7701, "ttl:key")
        assert ttl_meta["found"] is True
        assert wait_for(lambda: local_read(7701, "ttl:key")["found"] is False, timeout=12.0)

        print("[OK] Persistence recovery test passed")
    finally:
        restarted.stop()
        if base_dir.exists():
            shutil.rmtree(base_dir)


def test_concurrent_writes_same_key():
    nodes, ports, base_dir = build_cluster(
        node_count=3,
        replication_factor=3,
        base_name="concurrent_writes",
        aof_enabled=False,
        snapshot_enabled=False,
        default_consistency=ConsistencyLevel.QUORUM,
    )

    try:
        key = "hot:key"
        write_values = [f"value-{index}" for index in range(12)]
        results = []

        def worker(index, value):
            client = connect_client(ports[index % len(ports)][0])
            try:
                response = client.send_command("SET", key, value)
                results.append(bool(response and response.payload.get("success")))
            finally:
                client.disconnect()

        threads = [
            threading.Thread(target=worker, args=(index, value), daemon=True)
            for index, value in enumerate(write_values)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5.0)

        success_count = sum(results)
        assert success_count == len(write_values), f"expected {len(write_values)} successful writes, got {success_count}"

        replica_ports = [port for port, _ in ports]
        assert wait_for(
            lambda: len({local_read(port, key)["version"] for port in replica_ports}) == 1
            and len({local_read(port, key)["value"] for port in replica_ports}) == 1
        ), "replicas did not converge after concurrent writes"

        final_versions = {local_read(port, key)["version"] for port in replica_ports}
        assert final_versions == {len(write_values)}

        print("[OK] Concurrent write convergence test passed")
    finally:
        stop_cluster(nodes, base_dir)


def test_rebalance_during_active_traffic():
    nodes, ports, base_dir = build_cluster(
        node_count=3,
        replication_factor=2,
        base_name="rebalance_traffic",
        aof_enabled=False,
        snapshot_enabled=False,
        default_consistency=ConsistencyLevel.QUORUM,
    )

    writer_running = True
    writer_thread = None
    written = {}
    write_lock = threading.Lock()

    def writer():
        index = 0
        while writer_running:
            key = f"rebalance:key:{index}"
            value = f"value-{index}"
            port = random.choice([client_port for client_port, _ in ports])
            client = connect_client(port)
            try:
                response = client.send_command("SET", key, value)
                if response and response.payload.get("success"):
                    with write_lock:
                        written[key] = value
                    index += 1
            finally:
                client.disconnect()
            time.sleep(0.02)

    try:
        writer_thread = threading.Thread(target=writer, daemon=True)
        writer_thread.start()
        time.sleep(1.0)

        node4 = create_node(
            "node4",
            client_port=7504,
            cluster_port=8504,
            data_dir=str(base_dir),
            aof_enabled=False,
            snapshot_enabled=False,
            replication_factor=2,
        )
        node4.start()
        nodes.append(node4)
        ports.append((7504, 8504))

        assert node4.join_cluster(f"localhost:{ports[0][1]}")
        assert wait_for(lambda: all(n.cluster.membership.node_count() == 4 for n in nodes))
        assert wait_for(lambda: all(n.ring.get_node_count() == 4 for n in nodes))

        for client_port, _ in ports:
            client = connect_client(client_port)
            try:
                assert_success(client.send_command("REBALANCE"), f"REBALANCE {client_port}")
            finally:
                client.disconnect()

        time.sleep(1.5)
        writer_running = False
        writer_thread.join(timeout=5.0)

        for client_port, _ in ports:
            client = connect_client(client_port)
            try:
                assert_success(client.send_command("REBALANCE"), f"REBALANCE settle {client_port}")
            finally:
                client.disconnect()

        time.sleep(2.0)

        with write_lock:
            all_items = list(written.items())
            sample_items = random.sample(all_items, min(20, len(all_items)))

        assert sample_items, "expected writes during rebalance"

        for key, expected_value in sample_items:
            replicas = nodes[0].ring.get_nodes(key, 2)
            primary_port = get_port(nodes, ports, replicas[0])
            primary_client = connect_client(primary_port)
            try:
                assert assert_success(primary_client.send_command("GET", key, "QUORUM"), f"settle {key}") == expected_value
            finally:
                primary_client.disconnect()

            assert wait_for(
                lambda: all(
                    local_read(get_port(nodes, ports, replica_id), key)["value"] == expected_value
                    for replica_id in replicas
                )
            ), f"rebalance lost {key}: {[local_read(get_port(nodes, ports, replica_id), key)['value'] if local_read(get_port(nodes, ports, replica_id), key)['found'] else None for replica_id in replicas]}"

        moved_keys = [
            key for key, _ in all_items
            if nodes[0].ring.get_node(key) == "node4"
        ]
        assert moved_keys, "expected some sampled keys to move to node4"
        moved_key = moved_keys[0]
        moved_data = local_read(7504, moved_key)
        assert moved_data["found"] is True

        print("[OK] Rebalance during active traffic test passed")
    finally:
        writer_running = False
        if writer_thread:
            try:
                writer_thread.join(timeout=2.0)
            except Exception:
                pass
        stop_cluster(nodes, base_dir)


if __name__ == "__main__":
    test_partition_write_and_read_repair()
    test_leader_crash_during_replication()
    test_restart_with_persistence_recovery()
    test_concurrent_writes_same_key()
    test_rebalance_during_active_traffic()
    print("All resilience tests passed.")
