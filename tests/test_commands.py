"""
Command audit for the Mini-Redis/Cassandra command surface.

This exercises both network-facing commands and local CLI helpers so we catch
regressions where commands exist in docs but are not actually wired up.
"""

import shutil
import time
from pathlib import Path

from minidb.cli import DatabaseCLI
from minidb.network.client import TCPClient
from minidb.node import create_node


BASE_DIR = Path("test_command_data")


def assert_success(response, label):
    assert response is not None, f"{label}: no response"
    assert response.payload.get("success"), f"{label}: {response.payload}"
    return response.payload.get("data")


def wait_for(predicate, timeout=10.0, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def build_cluster(replication_factor=3):
    if BASE_DIR.exists():
        shutil.rmtree(BASE_DIR)
    BASE_DIR.mkdir()

    nodes = []
    ports = [(7401, 8401), (7402, 8402), (7403, 8403)]

    for index, (client_port, cluster_port) in enumerate(ports, start=1):
        node = create_node(
            f"node{index}",
            client_port=client_port,
            cluster_port=cluster_port,
            data_dir=str(BASE_DIR / f"node{index}"),
            aof_enabled=False,
            snapshot_enabled=False,
            replication_factor=replication_factor
        )
        node.start()
        nodes.append(node)

    time.sleep(3)
    for node in nodes[1:]:
        assert node.join_cluster("localhost:8401")

    assert wait_for(lambda: all(n.cluster.membership.node_count() == 3 for n in nodes))
    assert wait_for(lambda: all(n.ring.get_node_count() == 3 for n in nodes))

    leader_index = next(i for i, node in enumerate(nodes) if node.cluster.is_leader())
    return nodes, ports, leader_index


def stop_cluster(nodes):
    for node in nodes:
        try:
            node.stop()
        except Exception:
            pass

    if BASE_DIR.exists():
        shutil.rmtree(BASE_DIR)


def test_cli_local_commands():
    cli = DatabaseCLI()

    help_text = cli.execute("HELP")["message"]
    assert "SETEX <key> <ttl> <value>" in help_text
    assert "FAULT [action]" in help_text
    assert "RATELIMIT - Show rate limit statistics" in help_text

    assert cli.execute("HELP SETEX")["message"].startswith("SETEX")
    assert cli.execute("DEBUG on")["message"] == "Debug mode: ON"
    assert cli.execute("CONSISTENCY ONE")["message"] == "Default consistency set to: ONE"
    assert cli.execute("QUIT") is None


def test_network_commands_and_failover():
    nodes, ports, leader_index = build_cluster()

    try:
        leader_client = TCPClient("localhost", ports[leader_index][0])
        assert leader_client.connect()

        follower_index = 1 if leader_index == 0 else 0
        follower_client = TCPClient("localhost", ports[follower_index][0])
        assert follower_client.connect()

        assert_success(leader_client.send_command("PING"), "PING") == "PONG"
        assert_success(leader_client.send_command("SET", "user:1", "Alice"), "SET") == "OK"
        assert_success(leader_client.send_command("SETEX", "session:1", "60", "abc"), "SETEX") == "OK"
        assert assert_success(leader_client.send_command("GET", "user:1"), "GET") == "Alice"
        assert assert_success(leader_client.send_command("GET", "user:1", "QUORUM"), "GET QUORUM") == "Alice"
        assert assert_success(leader_client.send_command("EXISTS", "user:1"), "EXISTS") == 1
        keys = assert_success(leader_client.send_command("KEYS", "*"), "KEYS")
        assert sorted(keys) == ["session:1", "user:1"]

        info = assert_success(leader_client.send_command("INFO"), "INFO")
        assert info["role"] == "LEADER"
        assert info["cluster_size"] == 3

        storage_info = assert_success(leader_client.send_command("INFO", "storage"), "INFO storage")
        assert storage_info["keys"] == 2

        cluster = assert_success(leader_client.send_command("CLUSTER"), "CLUSTER")
        assert cluster["alive_count"] == 3
        assert len(cluster["members"]) == 3

        nodes_info = assert_success(leader_client.send_command("NODES"), "NODES")
        assert len(nodes_info["nodes"]) == 3

        leader_info = assert_success(leader_client.send_command("LEADER"), "LEADER")
        assert leader_info["leader_id"] == nodes[leader_index].node_id

        ring = assert_success(leader_client.send_command("RING", "5"), "RING")
        assert ring["node_count"] == 3
        assert len({entry["node"] for entry in ring["ring"]}) >= 2

        shards = assert_success(leader_client.send_command("SHARDS"), "SHARDS")
        assert shards["total_keys"] == 2
        assert sum(shards["distribution"].values()) == 2
        assert len(shards["distribution"]) >= 1

        route = assert_success(leader_client.send_command("ROUTE", "user:1"), "ROUTE")
        assert len(route["replicas"]) == 3
        assert route["primary_owner"] in route["replicas"]

        replicas = assert_success(leader_client.send_command("REPLICAS", "user:1"), "REPLICAS")
        assert len(replicas["replicas"]) == 3

        stats = assert_success(leader_client.send_command("STATS"), "STATS")
        assert "storage" in stats and "reads" in stats

        rebalance = assert_success(leader_client.send_command("REBALANCE"), "REBALANCE")
        assert "migrations_started" in rebalance

        migrate = assert_success(leader_client.send_command("MIGRATE", "STATUS"), "MIGRATE STATUS")
        assert "stats" in migrate

        ratelimit = assert_success(leader_client.send_command("RATELIMIT"), "RATELIMIT")
        assert "rate_limiter" in ratelimit

        assert "Not leader" in follower_client.send_command("SET", "bad", "write").payload.get("error", "")

        assert_success(leader_client.send_command("FAULT", "ENABLE"), "FAULT ENABLE")
        assert_success(leader_client.send_command("FAULT", "DELAY", "5"), "FAULT DELAY")
        active_faults = assert_success(leader_client.send_command("FAULT", "LIST"), "FAULT LIST")
        assert len(active_faults) >= 1
        assert_success(leader_client.send_command("FAULT", "CLEAR"), "FAULT CLEAR")
        assert_success(leader_client.send_command("FAULT", "DISABLE"), "FAULT DISABLE")

        assert_success(leader_client.send_command("DEL", "user:1"), "DEL") == "OK"
        assert assert_success(leader_client.send_command("GET", "user:1"), "GET deleted") is None

        old_leader = nodes[leader_index].node_id
        failover = assert_success(leader_client.send_command("FAILOVER"), "FAILOVER")
        assert "Stepping down" in failover["message"]

        assert wait_for(
            lambda: all(
                node.cluster.get_leader_id() and node.cluster.get_leader_id() != old_leader
                for node in nodes
            ),
            timeout=10.0
        )

        new_leaders = set()
        for client_port, _ in ports:
            client = TCPClient("localhost", client_port)
            client.connect()
            leader_data = assert_success(client.send_command("LEADER"), f"LEADER {client_port}")
            new_leaders.add(leader_data["leader_id"])
            client.disconnect()

        assert len(new_leaders) == 1
        assert old_leader not in new_leaders

    finally:
        leader_client.disconnect()
        follower_client.disconnect()
        stop_cluster(nodes)


if __name__ == "__main__":
    test_cli_local_commands()
    test_network_commands_and_failover()
    print("All command tests passed.")
