"""Fail-closed consistency checks independent of election timing."""
from types import SimpleNamespace

from minidb.cluster.read_coordinator import ReadCoordinator
from minidb.config import ConsistencyLevel
from minidb.node import create_node


def test_reduced_ring_does_not_reduce_read_consistency():
    reader = ReadCoordinator("a", replication_factor=3)
    reader.set_callbacks(get_replicas=lambda key: ["a"],
                         get_local_value=lambda key: ("old", 1, True, {}))
    try:
        for level in (ConsistencyLevel.ALL, ConsistencyLevel.QUORUM):
            value, found, metadata = reader.read("key", level)
            assert not found
            assert metadata["quorum_failed"]
        assert reader.read("key", ConsistencyLevel.ONE)[:2] == ("old", True)
    finally:
        reader._executor.shutdown(wait=True)


def test_prepared_but_uncommitted_write_is_not_acknowledged(tmp_path):
    node = create_node("a", data_dir=str(tmp_path), replication_factor=3,
                       aof_enabled=False, snapshot_enabled=False,
                       default_consistency=ConsistencyLevel.ALL)
    node.ring.get_node_count = lambda: 3
    node.cluster.is_leader = lambda: True
    node.cluster.membership.get_alive_nodes = lambda: [SimpleNamespace(node_id=n) for n in ("a", "b", "c")]
    node.cluster.replication.replicate = lambda *args, **kwargs: True
    node.cluster.commit_to_followers = lambda *args: 0
    response = node._apply_partition_write("SET", "key", "uncommitted")
    assert not response.payload["success"]
    assert node.store.get_with_metadata("key") is None
    node.cluster.commit_to_followers = lambda *args: 2
    assert node._apply_partition_write("SET", "key", "committed").payload["success"]
    assert node.store.get_with_metadata("key").value == "committed"


def test_committed_entry_is_flushed_before_return(tmp_path, monkeypatch):
    import os
    from minidb.cluster.election import LogEntry
    node = create_node("a", data_dir=str(tmp_path), aof_enabled=True,
                       snapshot_enabled=False)
    synced = []
    real_fsync = os.fsync
    def record_fsync(fd):
        synced.append(fd)
        return real_fsync(fd)
    monkeypatch.setattr(os, "fsync", record_fsync)
    try:
        node._apply_log_entry(LogEntry(term=1, index=1, command="SET", key="key", value="durable"))
        assert synced, "commit acknowledgement would precede AOF fsync"
        assert "durable" in open(node.aof.aof_path).read()
    finally:
        node.aof.close()
