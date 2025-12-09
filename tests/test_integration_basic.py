"""Basic integration tests for multiprocess Raft cluster.

These tests verify the fundamental behavior of the tick-based
multiprocess Raft implementation.
"""

import pytest

from tests.helpers.assertions import assert_commit_index, assert_log_replicated, assert_single_leader, wait_for_leader


def test_leader_election_3_nodes(cluster_3):
    leader_id = wait_for_leader(cluster_3, timeout_ticks=500)
    assert leader_id is not None, "No leader elected within timeout"

    assert_single_leader(cluster_3)

    cluster_3.print_cluster_state()


def test_leader_election_5_nodes(cluster_5):
    leader_id = wait_for_leader(cluster_5, timeout_ticks=500)
    assert leader_id is not None, "No leader elected within timeout"

    assert_single_leader(cluster_5)

    cluster_5.print_cluster_state()


def test_log_replication_3_nodes(cluster_3):
    leader_id = wait_for_leader(cluster_3, timeout_ticks=500)
    assert leader_id is not None, "No leader elected"

    print(f"\n[TEST] Leader elected: node {leader_id}")

    commands = ["cmd1", "cmd2", "cmd3"]
    for cmd in commands:
        success = cluster_3.append_entry(leader_id, cmd)
        assert success, f"Failed to append command '{cmd}'"
        print(f"[TEST] Submitted command: {cmd}")

        # Give time for replication
        cluster_3.tick_cluster(n_ticks=100)

    # Wait for full replication
    cluster_3.tick_cluster(n_ticks=200)

    assert_log_replicated(cluster_3, expected_length=3)
    assert_commit_index(cluster_3, min_commit_idx=2)

    cluster_3.print_cluster_state()


def test_sequential_commands(cluster_3):
    leader_id = wait_for_leader(cluster_3, timeout_ticks=500)
    assert leader_id is not None, "No leader elected"

    # Submit multiple commands rapidly
    for i in range(5):
        success = cluster_3.append_entry(leader_id, f"rapid_{i}")
        assert success, f"Failed to append command rapid_{i}"

    cluster_3.tick_cluster(n_ticks=300)

    assert_log_replicated(cluster_3, expected_length=5)
    assert_commit_index(cluster_3, min_commit_idx=4)

    cluster_3.print_cluster_state()


@pytest.mark.timeout(30)
def test_cluster_health(cluster_3):
    """Test that cluster remains healthy over extended period."""
    leader_id = wait_for_leader(cluster_3, timeout_ticks=500)
    assert leader_id is not None

    cluster_3.append_entry(leader_id, "health_check")

    cluster_3.tick_cluster(n_ticks=1000)

    assert_single_leader(cluster_3)

    assert_log_replicated(cluster_3, expected_length=1)

    cluster_3.print_cluster_state()
