from tests.helpers.assertions import (
    assert_commit_index,
    assert_log_replicated,
    wait_for_leader,
    wait_for_log_replication,
)

from pathlib import Path

from raft.node import TickNode
from raft.messages import LogEntry


def test_persist_and_restore_state(tmp_path: Path):
    state_dir = tmp_path / "raft-state"

    node = TickNode(id=7, peer_ids=[1, 2], random_seed=123, state_dir=state_dir)
    node.term = 5
    node.voted_for = 2
    node.log = [LogEntry(data="alpha", term=3), LogEntry(data="beta", term=5)]
    node.commit_idx = 1
    node.last_applied = 1
    node._persist_state()

    restored = TickNode(id=7, peer_ids=[1, 2], random_seed=999, state_dir=state_dir)

    assert restored.term == 5
    assert restored.voted_for == 2
    assert len(restored.log) == 2
    assert restored.log[0].data == "alpha"
    assert restored.log[1].data == "beta"
    assert restored.commit_idx == 1
    assert restored.last_applied == 1

    persisted_file = state_dir / "node_7.json"
    assert persisted_file.exists(), "State file was not created"


def test_leader_restart_recovers_log(cluster_3):
    leader_id = wait_for_leader(cluster_3, timeout_ticks=500)
    assert leader_id is not None, "No leader elected within timeout"

    cluster_3.append_entry(leader_id, "persisted_cmd")
    cluster_3.tick_cluster(n_ticks=300)

    assert_log_replicated(cluster_3, expected_length=1)
    assert_commit_index(cluster_3, min_commit_idx=0)

    cluster_3.kill_node(leader_id)

    new_leader = wait_for_leader(cluster_3, timeout_ticks=500)
    assert new_leader is not None, "No leader elected after old leader crashed"
    assert new_leader != leader_id, "Old leader should not still be running"

    cluster_3.append_entry(new_leader, "post_crash_cmd")
    cluster_3.tick_cluster(n_ticks=300)

    assert_log_replicated(cluster_3, expected_length=2)
    assert_commit_index(cluster_3, min_commit_idx=1)

    cluster_3.start_node(leader_id, random_seed=99)

    assert wait_for_log_replication(cluster_3, expected_length=2, timeout_ticks=500)
    cluster_3.tick_cluster(n_ticks=200)
    assert_commit_index(cluster_3, min_commit_idx=1)
