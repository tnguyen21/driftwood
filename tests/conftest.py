"""Pytest configuration and fixtures for Raft testing.

This module provides pytest fixtures for creating and managing
test clusters of various sizes.
"""

import time

import pytest

from tests.helpers.cluster import InProcessCluster, MultiprocessCluster


@pytest.fixture
def cluster_3():
    """3-node multiprocess cluster fixture.

    The cluster is automatically started before the test and
    shut down after the test completes.
    """
    cluster = MultiprocessCluster(n_nodes=3)
    cluster.start_all(random_seed=42)
    time.sleep(0.3)  # Let processes start

    yield cluster

    cluster.shutdown()


@pytest.fixture
def cluster_5():
    """5-node multiprocess cluster fixture.

    The cluster is automatically started before the test and
    shut down after the test completes.
    """
    cluster = MultiprocessCluster(n_nodes=5)
    cluster.start_all(random_seed=42)
    time.sleep(0.3)  # Let processes start

    yield cluster

    cluster.shutdown()


@pytest.fixture
def cluster_7():
    """7-node multiprocess cluster fixture.

    The cluster is automatically started before the test and
    shut down after the test completes.
    """
    cluster = MultiprocessCluster(n_nodes=7)
    cluster.start_all(random_seed=42)
    time.sleep(0.4)  # Let processes start

    yield cluster

    cluster.shutdown()


@pytest.fixture
def in_process_cluster_3():
    """3-node in-process cluster fixture for fast unit tests.

    The cluster runs in the same process with in-memory message
    passing for fast deterministic testing.
    """
    cluster = InProcessCluster(n_nodes=3, random_seed=42)

    yield cluster

    cluster.shutdown()


@pytest.fixture
def in_process_cluster_5():
    """5-node in-process cluster fixture for fast unit tests.

    The cluster runs in the same process with in-memory message
    passing for fast deterministic testing.
    """
    cluster = InProcessCluster(n_nodes=5, random_seed=42)

    yield cluster

    cluster.shutdown()
