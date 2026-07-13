"""
Unit/integration tests for QUORUM write-ack + read-after-write consistency.

These cover the FailForge seed-42 failure class:
  - GET defaulted to ANY while writes used QUORUM → stale local reads
  - QUORUM writes succeeded with zero remote replicas when the ring was empty
"""

import os
import sys
import shutil
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minidb.config import ConsistencyLevel
from minidb.cluster.election import LogEntry
from minidb.cluster.replication import ReplicationManager
from minidb.network.client import TCPClient
from minidb.node import create_node


BASE_DIR = Path("test_resilience_data") / "quorum_raw"


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


def build_cluster(node_count=3, replication_factor=3, base_name="raw"):
    base_dir = BASE_DIR / base_name
    if base_dir.exists():
        shutil.rmtree(base_dir)
    base_dir.mkdir(parents=True)

    ports = []
    nodes = []
    client_base = 7701
    cluster_base = 8701

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
            aof_enabled=False,
            snapshot_enabled=False,
            default_consistency=ConsistencyLevel.QUORUM,
        )
        node.start()
        nodes.append(node)

    time.sleep(2)
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


def test_quorum_replicate_fails_without_followers():
    """QUORUM must not ack a write when no remote replicas are available (RF>1)."""
    mgr = ReplicationManager(
        node_id="n1",
        replication_factor=3,
        default_consistency=ConsistencyLevel.QUORUM,
        replication_timeout=1.0,
    )
    entry = LogEntry(term=1, index=1, command="SET", key="k", value="v")

    assert mgr.replicate(entry, ConsistencyLevel.QUORUM, targets=[]) is False
    assert mgr.replicate(entry, ConsistencyLevel.ONE, targets=[]) is True

    # No callback and no targets: still fail closed for sync levels.
    assert mgr.replicate(entry, ConsistencyLevel.QUORUM, targets=None) is False


def test_read_after_write_default_quorum_from_any_node():
    """
    After a successful SET under default QUORUM, a bare GET (no consistency arg)
    from every node must return the acknowledged value.
    """
    nodes, ports, base_dir = build_cluster(base_name="raw_happy")
    try:
        key = "raw:key"
        value = "ack-42"

        # Write via first client port; owner may forward.
        writer = TCPClient("localhost", ports[0][0], timeout=5.0)
        assert writer.connect()
        assert assert_success(writer.send_command("SET", key, value), "SET") == "OK"
        writer.disconnect()

        # Bare GET from every node — must use default_consistency=QUORUM.
        for client_port, _ in ports:
            client = TCPClient("localhost", client_port, timeout=5.0)
            assert client.connect(), f"connect {client_port}"
            got = assert_success(client.send_command("GET", key), f"GET via {client_port}")
            assert got == value, f"stale/missing on port {client_port}: {got!r}"
            client.disconnect()

        print("[OK] Default-QUORUM read-after-write from any node")
    finally:
        stop_cluster(nodes, base_dir)


def _leader_and_port(nodes, ports):
    assert wait_for(lambda: sum(1 for n in nodes if n.cluster.is_leader()) == 1), "need exactly one leader"
    leader = next(n for n in nodes if n.cluster.is_leader())
    port = next(ports[i][0] for i, n in enumerate(nodes) if n.node_id == leader.node_id)
    return leader, port


def test_quorum_write_requires_remote_ack():
    """With RF=3, a write whose remotes all fail must not be acknowledged."""
    nodes, ports, base_dir = build_cluster(base_name="raw_no_ack")
    try:
        leader, leader_port = _leader_and_port(nodes, ports)
        # Force every replicate send to fail on the write leader.
        leader.cluster.replication._send_replicate = lambda follower_id, entry: False

        key = "leader-only-key"

        client = TCPClient("localhost", leader_port, timeout=5.0)
        assert client.connect()
        response = client.send_command("SET", key, "should-fail")
        client.disconnect()

        assert response is not None
        assert response.payload.get("success") is False, (
            f"write should fail without remote acks: {response.payload}"
        )
        # Primary-first + revert: never-acked write must not remain visible.
        meta = leader.store.get_with_metadata(key)
        assert meta is None, f"orphaned never-acked value on leader: {meta}"
        print("[OK] QUORUM write fails closed without remote acks")
    finally:
        stop_cluster(nodes, base_dir)


def test_failed_write_not_visible_via_get_strong_or_quorum():
    """
    Residual class: GET must not return a value from a write that never acked.
    Simulates remote-ack failure after primary-local apply (reverted).
    """
    nodes, ports, base_dir = build_cluster(base_name="raw_invisible_fail")
    try:
        key = "invis:leader-key"

        # Seed an acked value so we also check failed follow-up does not replace it.
        seeded = False
        for _ in range(10):
            leader, leader_port = _leader_and_port(nodes, ports)
            seed_client = TCPClient("localhost", leader_port, timeout=5.0)
            if not seed_client.connect():
                continue
            resp = seed_client.send_command("SET", key, "stable")
            seed_client.disconnect()
            if resp and resp.payload.get("success"):
                seeded = True
                break
            time.sleep(0.3)
        assert seeded, "failed to seed stable value under current leader"

        leader, leader_port = _leader_and_port(nodes, ports)
        leader.cluster.replication._send_replicate = lambda follower_id, entry: False

        client = TCPClient("localhost", leader_port, timeout=5.0)
        assert client.connect()
        bad = client.send_command("SET", key, "never-acked-phantom")
        # If leadership moved, re-resolve and retry the failing write once.
        if bad is not None and bad.payload.get("success"):
            pass  # unexpected success would still be checked below
        elif bad is None or not bad.payload.get("success"):
            if bad is None or "Leader" in str(bad.payload.get("error")):
                leader, leader_port = _leader_and_port(nodes, ports)
                leader.cluster.replication._send_replicate = lambda follower_id, entry: False
                client.disconnect()
                client = TCPClient("localhost", leader_port, timeout=5.0)
                assert client.connect()
                bad = client.send_command("SET", key, "never-acked-phantom")
        assert bad is not None and bad.payload.get("success") is False

        for level in ("STRONG", "QUORUM", "ANY"):
            got = assert_success(client.send_command("GET", key, level), f"GET {level}")
            assert got == "stable", f"{level} saw never-acked value: {got!r}"
        client.disconnect()
        print("[OK] Failed write not visible via GET")
    finally:
        stop_cluster(nodes, base_dir)


def test_single_leader_serializes_same_key_writes():
    """Two clients must not both ACK concurrent SETs that diverge without order."""
    nodes, ports, base_dir = build_cluster(base_name="raw_single_leader")
    try:
        import threading
        results = []

        def do_set(port, value):
            client = TCPClient("localhost", port, timeout=5.0)
            assert client.connect()
            resp = client.send_command("SET", "same-key", value)
            client.disconnect()
            results.append((value, resp.payload.get("success") if resp else None,
                            resp.payload.get("data") if resp else None))

        t1 = threading.Thread(target=do_set, args=(ports[0][0], "A"))
        t2 = threading.Thread(target=do_set, args=(ports[1][0], "B"))
        t1.start(); t2.start(); t1.join(); t2.join()

        # At least one must succeed; final STRONG read must match a successful write.
        oks = [v for v, ok, _ in results if ok]
        assert oks, f"no successful write: {results}"
        client = TCPClient("localhost", ports[0][0], timeout=5.0)
        assert client.connect()
        final = assert_success(client.send_command("GET", "same-key", "STRONG"), "final")
        client.disconnect()
        assert final in oks, f"final {final!r} not in successful writes {oks}"
        print("[OK] Single-leader same-key writes serialize")
    finally:
        stop_cluster(nodes, base_dir)
