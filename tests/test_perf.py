import time

import pytest

from tests.helpers.assertions import find_leader, wait_for_leader, wait_for_log_replication


def _measure_throughput(cluster, num_cmds: int, label: str) -> float:
    leader_id = wait_for_leader(cluster, timeout_ticks=500)
    assert leader_id is not None, f"No leader elected for {label}"

    start = time.perf_counter()
    for i in range(num_cmds):
        leader_id = find_leader(cluster) or wait_for_leader(cluster, timeout_ticks=200)
        assert leader_id is not None, f"No leader available while sending command {i} for {label}"
        cluster.append_entry(leader_id, f"perf_cmd_{i}")
        cluster.tick_cluster(n_ticks=100)

    cluster.tick_cluster(n_ticks=300)
    assert wait_for_log_replication(cluster, expected_length=num_cmds, timeout_ticks=500)
    end = time.perf_counter()

    throughput = num_cmds / max(end - start, 1e-6)
    print(f"[PERF] {label}: {throughput:.2f} commits/sec over {num_cmds} commands")
    return throughput


@pytest.mark.timeout(30)
def test_perf_three_node_cluster(cluster_3):
    throughput = _measure_throughput(cluster_3, num_cmds=10, label="3-node")
    assert throughput > 0


@pytest.mark.timeout(40)
def test_perf_five_node_cluster(cluster_5):
    throughput = _measure_throughput(cluster_5, num_cmds=10, label="5-node")
    assert throughput > 0
