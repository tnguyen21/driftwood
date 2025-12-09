"""Test assertion helpers for Raft cluster testing.

This module provides common assertion functions for verifying
Raft cluster behavior in tests.
"""

from typing import Any

from tests.helpers.cluster import MultiprocessCluster


def wait_for_leader(cluster: MultiprocessCluster, timeout_ticks: int = 500) -> int | None:
    """Tick cluster until a leader is elected.

    Args:
        cluster: The cluster to tick
        timeout_ticks: Maximum number of ticks to wait

    Returns:
        Node ID of the elected leader, or None if no leader elected
    """
    for _ in range(timeout_ticks):
        cluster.tick_cluster(n_ticks=1)
        for node_id in range(cluster.n_nodes):
            state = cluster.get_node_state(node_id)
            if state and state["state"] == "LEADER":
                return node_id
    return None


def find_leader(cluster: MultiprocessCluster) -> int | None:
    """Find the current leader without advancing time.

    Args:
        cluster: The cluster to query

    Returns:
        Node ID of the leader, or None if no leader
    """
    for node_id in range(cluster.n_nodes):
        state = cluster.get_node_state(node_id)
        if state and state["state"] == "LEADER":
            return node_id
    return None


def count_leaders(cluster: MultiprocessCluster) -> int:
    """Count the number of leaders in the cluster.

    Args:
        cluster: The cluster to query

    Returns:
        Number of nodes in LEADER state
    """
    count = 0
    for node_id in range(cluster.n_nodes):
        state = cluster.get_node_state(node_id)
        if state and state["state"] == "LEADER":
            count += 1
    return count


def assert_single_leader(cluster: MultiprocessCluster):
    """Assert that exactly one leader exists.

    Args:
        cluster: The cluster to check

    Raises:
        AssertionError: If there are zero or multiple leaders
    """
    leaders = []
    for node_id in range(cluster.n_nodes):
        state = cluster.get_node_state(node_id)
        if state and state["state"] == "LEADER":
            leaders.append(node_id)

    assert len(leaders) == 1, f"Expected exactly one leader, found {len(leaders)}: {leaders}"


def assert_no_leader(cluster: MultiprocessCluster):
    """Assert that no leader exists (e.g., minority partition).

    Args:
        cluster: The cluster to check

    Raises:
        AssertionError: If any leader exists
    """
    leaders = []
    for node_id in range(cluster.n_nodes):
        state = cluster.get_node_state(node_id)
        if state and state["state"] == "LEADER":
            leaders.append(node_id)

    assert len(leaders) == 0, f"Expected no leader, found {len(leaders)}: {leaders}"


def assert_log_replicated(cluster: MultiprocessCluster, expected_length: int):
    """Assert all running nodes have replicated log.

    Args:
        cluster: The cluster to check
        expected_length: Minimum expected log length

    Raises:
        AssertionError: If any node's log is too short
    """
    for node_id in range(cluster.n_nodes):
        state = cluster.get_node_state(node_id)
        if state:  # Only check running nodes
            actual_length = len(state["log"])
            assert actual_length >= expected_length, f"Node {node_id} log too short: {actual_length} < {expected_length}"


def assert_commit_index(cluster: MultiprocessCluster, min_commit_idx: int):
    """Assert all running nodes have advanced commit index.

    Args:
        cluster: The cluster to check
        min_commit_idx: Minimum expected commit index

    Raises:
        AssertionError: If any node's commit index is too low
    """
    for node_id in range(cluster.n_nodes):
        state = cluster.get_node_state(node_id)
        if state:  # Only check running nodes
            actual_commit = state["commit_idx"]
            assert actual_commit >= min_commit_idx, f"Node {node_id} commit_idx too low: {actual_commit} < {min_commit_idx}"


def assert_logs_match(cluster: MultiprocessCluster, up_to_index: int | None = None):
    """Assert all running nodes have matching logs.

    Args:
        cluster: The cluster to check
        up_to_index: If specified, only check log entries up to this index

    Raises:
        AssertionError: If logs don't match
    """
    logs = []
    for node_id in range(cluster.n_nodes):
        state = cluster.get_node_state(node_id)
        if state:
            log = state["log"]
            if up_to_index is not None:
                log = log[: up_to_index + 1]
            logs.append((node_id, log))

    if not logs:
        return  # No running nodes

    reference_log = logs[0][1]
    for node_id, log in logs[1:]:
        assert log == reference_log, f"Node {node_id} log doesn't match:\n  Node {logs[0][0]}: {reference_log}\n  Node {node_id}: {log}"


def assert_term_convergence(cluster: MultiprocessCluster):
    """Assert all running nodes have converged to the same term.

    Args:
        cluster: The cluster to check

    Raises:
        AssertionError: If terms don't match
    """
    terms = []
    for node_id in range(cluster.n_nodes):
        state = cluster.get_node_state(node_id)
        if state:
            terms.append((node_id, state["term"]))

    if not terms:
        return

    reference_term = terms[0][1]
    for node_id, term in terms[1:]:
        assert term == reference_term, f"Terms don't match: Node {terms[0][0]} has term {reference_term}, Node {node_id} has term {term}"


def wait_for_replication(cluster: MultiprocessCluster, expected_commit_idx: int, timeout_ticks: int = 500) -> bool:
    """Wait for all nodes to replicate up to a commit index.

    Args:
        cluster: The cluster to tick
        expected_commit_idx: Target commit index
        timeout_ticks: Maximum ticks to wait

    Returns:
        True if all nodes reached the commit index, False on timeout
    """
    for _ in range(timeout_ticks):
        all_committed = True
        for node_id in range(cluster.n_nodes):
            state = cluster.get_node_state(node_id)
            if state and state["commit_idx"] < expected_commit_idx:
                all_committed = False
                break

        if all_committed:
            return True

        cluster.tick_cluster(n_ticks=1)

    return False


def wait_for_log_replication(cluster: MultiprocessCluster, expected_length: int, timeout_ticks: int = 500) -> bool:
    """Wait for all nodes to replicate log entries.

    Args:
        cluster: The cluster to tick
        expected_length: Expected log length
        timeout_ticks: Maximum ticks to wait

    Returns:
        True if all nodes have the log entries, False on timeout
    """
    for _ in range(timeout_ticks):
        all_replicated = True
        for node_id in range(cluster.n_nodes):
            state = cluster.get_node_state(node_id)
            if state and len(state["log"]) < expected_length:
                all_replicated = False
                break

        if all_replicated:
            return True

        cluster.tick_cluster(n_ticks=1)

    return False


def get_cluster_summary(cluster: MultiprocessCluster) -> dict[str, Any]:
    """Get a summary of the cluster state.

    Args:
        cluster: The cluster to summarize

    Returns:
        Dictionary with cluster summary information
    """
    states = []
    for node_id in range(cluster.n_nodes):
        state = cluster.get_node_state(node_id)
        if state:
            states.append(
                {
                    "id": node_id,
                    "state": state["state"],
                    "term": state["term"],
                    "tick": state["current_tick"],
                    "commit_idx": state["commit_idx"],
                    "log_length": len(state["log"]),
                }
            )
        else:
            states.append({"id": node_id, "state": "DEAD"})

    leader_count = sum(1 for s in states if s.get("state") == "LEADER")
    terms = [s["term"] for s in states if s.get("state") != "DEAD"]

    return {
        "nodes": states,
        "leader_count": leader_count,
        "unique_terms": len(set(terms)) if terms else 0,
        "max_term": max(terms) if terms else 0,
    }
