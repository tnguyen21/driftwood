import time
import pytest

from tests.helpers.cluster import InProcessCluster, MultiprocessCluster


@pytest.fixture
def cluster_3():
    cluster = MultiprocessCluster(n_nodes=3)
    cluster.start_all(random_seed=42)
    time.sleep(0.3)

    yield cluster

    cluster.shutdown()


@pytest.fixture
def cluster_5():
    cluster = MultiprocessCluster(n_nodes=5)
    cluster.start_all(random_seed=42)
    time.sleep(0.3)

    yield cluster

    cluster.shutdown()


@pytest.fixture
def cluster_7():
    cluster = MultiprocessCluster(n_nodes=7)
    cluster.start_all(random_seed=42)
    time.sleep(0.4)

    yield cluster

    cluster.shutdown()


@pytest.fixture
def in_process_cluster_3():
    cluster = InProcessCluster(n_nodes=3, random_seed=42)

    yield cluster

    cluster.shutdown()


@pytest.fixture
def in_process_cluster_5():
    cluster = InProcessCluster(n_nodes=5, random_seed=42)

    yield cluster

    cluster.shutdown()
