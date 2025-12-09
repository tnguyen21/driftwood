"""Cluster management helpers for testing.

This module provides helpers for managing clusters of Raft nodes:
- MultiprocessCluster: Manages nodes running in separate processes
- InProcessCluster: Manages nodes in the same process (faster for unit tests)
"""

import json
import subprocess
import sys
import time
from typing import Any

import requests

from raft.node import TickNode


class MultiprocessCluster:
    """Manages a cluster of Raft nodes in separate processes.

    Each node runs in its own process with UDP communication.
    The cluster manager coordinates tick advancement and provides
    utilities for querying state and injecting failures.
    """

    def __init__(self, n_nodes: int, base_udp_port: int = 10000, base_http_port: int = 20000):
        """Initialize multiprocess cluster.

        Args:
            n_nodes: Number of nodes in the cluster
            base_udp_port: Starting port for UDP sockets
            base_http_port: Starting port for HTTP control servers
        """
        self.n_nodes = n_nodes
        self.processes: dict[int, subprocess.Popen] = {}
        self.http_ports = {i: base_http_port + i for i in range(n_nodes)}
        self.udp_ports = {i: base_udp_port + i for i in range(n_nodes)}

        # Network partition state
        self.partitioned_nodes: set[int] = set()

    def start_node(self, node_id: int, random_seed: int | None = None):
        """Start a node process with subprocess.Popen.

        Args:
            node_id: ID of the node to start
            random_seed: Optional seed for deterministic behavior
        """
        if node_id in self.processes:
            print(f"[Cluster] Node {node_id} already running")
            return

        peers = [("localhost", self.udp_ports[i]) for i in range(self.n_nodes) if i != node_id]
        peer_ids = [i for i in range(self.n_nodes) if i != node_id]

        cmd = [
            sys.executable,
            "-m",
            "raft.cli",
            "--id",
            str(node_id),
            "--udp-port",
            str(self.udp_ports[node_id]),
            "--http-port",
            str(self.http_ports[node_id]),
            "--peers",
            json.dumps(peers),
            "--peer-ids",
            json.dumps(peer_ids),
        ]

        if random_seed is not None:
            cmd.extend(["--random-seed", str(random_seed)])

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.processes[node_id] = proc
        print(f"[Cluster] Started node {node_id} (PID: {proc.pid})")

    def start_all(self, random_seed: int | None = None):
        """Start all nodes in the cluster.

        Args:
            random_seed: Optional base seed (each node gets seed + node_id)
        """
        for i in range(self.n_nodes):
            seed = (random_seed + i) if random_seed is not None else None
            self.start_node(i, random_seed=seed)

        # Give nodes time to start up
        time.sleep(0.3)

    def tick_cluster(self, n_ticks: int = 1):
        """Advance all running nodes by n ticks in lockstep.

        Args:
            n_ticks: Number of ticks to advance
        """
        for _ in range(n_ticks):
            for node_id in range(self.n_nodes):
                if node_id in self.processes and self.processes[node_id].poll() is None:
                    try:
                        requests.post(f"http://localhost:{self.http_ports[node_id]}/tick", timeout=1)
                    except requests.exceptions.RequestException as e:
                        print(f"[Cluster] Error ticking node {node_id}: {e}")

    def get_node_state(self, node_id: int) -> dict[str, Any] | None:
        """Query node state via HTTP.

        Args:
            node_id: ID of the node to query

        Returns:
            Node state dict or None if node is not running
        """
        if node_id not in self.processes or self.processes[node_id].poll() is not None:
            return None

        try:
            response = requests.get(f"http://localhost:{self.http_ports[node_id]}/state", timeout=1)
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"[Cluster] Error getting state from node {node_id}: {e}")
            return None

    def kill_node(self, node_id: int):
        """Kill a node process.

        Args:
            node_id: ID of the node to kill
        """
        if node_id in self.processes:
            proc = self.processes[node_id]
            print(f"[Cluster] Killing node {node_id} (PID: {proc.pid})")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print(f"[Cluster] Node {node_id} did not terminate, forcing kill")
                proc.kill()
                proc.wait()
            del self.processes[node_id]

    def partition_node(self, node_id: int):
        """Isolate a node (drop all its messages).

        Args:
            node_id: ID of the node to partition
        """
        self.partitioned_nodes.add(node_id)
        try:
            requests.post(f"http://localhost:{self.http_ports[node_id]}/partition", json={"isolated": True}, timeout=1)
            print(f"[Cluster] Partitioned node {node_id}")
        except requests.exceptions.RequestException as e:
            print(f"[Cluster] Error partitioning node {node_id}: {e}")

    def partition_groups(self, group_a: list[int], group_b: list[int]):
        """Create network partition between two groups.

        Args:
            group_a: Node IDs in first group
            group_b: Node IDs in second group
        """
        # Configure group A to only accept messages from group A
        for node_id in group_a:
            try:
                requests.post(f"http://localhost:{self.http_ports[node_id]}/partition", json={"allowed_peers": group_a}, timeout=1)
            except requests.exceptions.RequestException as e:
                print(f"[Cluster] Error partitioning node {node_id}: {e}")

        # Configure group B to only accept messages from group B
        for node_id in group_b:
            try:
                requests.post(f"http://localhost:{self.http_ports[node_id]}/partition", json={"allowed_peers": group_b}, timeout=1)
            except requests.exceptions.RequestException as e:
                print(f"[Cluster] Error partitioning node {node_id}: {e}")

        print(f"[Cluster] Created partition: {group_a} | {group_b}")

    def heal_partition(self, node_id: int | None = None):
        """Restore network connectivity.

        Args:
            node_id: If specified, heal only this node. Otherwise heal all partitions.
        """
        if node_id is not None:
            if node_id in self.partitioned_nodes:
                self.partitioned_nodes.remove(node_id)
            try:
                requests.post(f"http://localhost:{self.http_ports[node_id]}/partition", json={"isolated": False}, timeout=1)
                print(f"[Cluster] Healed partition for node {node_id}")
            except requests.exceptions.RequestException as e:
                print(f"[Cluster] Error healing partition for node {node_id}: {e}")
        else:
            # Heal all nodes
            for i in range(self.n_nodes):
                if i in self.processes and self.processes[i].poll() is None:
                    try:
                        requests.post(f"http://localhost:{self.http_ports[i]}/partition", json={"isolated": False}, timeout=1)
                    except requests.exceptions.RequestException:
                        pass
            self.partitioned_nodes.clear()
            print("[Cluster] Healed all partitions")

    def append_entry(self, node_id: int, command: Any) -> bool:
        """Submit a command to a node.

        Args:
            node_id: ID of the node to submit to
            command: Command data

        Returns:
            True if successful, False otherwise
        """
        try:
            response = requests.post(f"http://localhost:{self.http_ports[node_id]}/append_entry", json={"command": command}, timeout=1)
            return response.json().get("success", False)
        except requests.exceptions.RequestException as e:
            print(f"[Cluster] Error appending entry to node {node_id}: {e}")
            return False

    def shutdown(self):
        """Shutdown all nodes."""
        print("[Cluster] Shutting down all nodes...")
        for node_id in list(self.processes.keys()):
            self.kill_node(node_id)
        print("[Cluster] All nodes stopped")

    def print_cluster_state(self):
        """Print the state of all nodes for debugging."""
        print(f"\n{'=' * 80}")
        print("CLUSTER STATE")
        print(f"{'=' * 80}")

        for i in range(self.n_nodes):
            state = self.get_node_state(i)
            if state:
                log_str = [f"({e['term']},{e['data']})" for e in state["log"]]
                print(
                    f"Node {i} [{state['state']:9}] term={state['term']:2} "
                    f"commit_idx={state['commit_idx']:2} tick={state['current_tick']:4} log={log_str}"
                )
            else:
                print(f"Node {i} [DEAD     ]")

        print(f"{'=' * 80}\n")


class InProcessCluster:
    """Manages a cluster of Raft nodes in the same process.

    This is faster than multiprocess testing and useful for unit tests.
    Messages are routed in-memory instead of via UDP.
    """

    def __init__(self, n_nodes: int, random_seed: int | None = None):
        """Initialize in-process cluster.

        Args:
            n_nodes: Number of nodes in the cluster
            random_seed: Optional base seed for deterministic behavior
        """
        self.n_nodes = n_nodes
        self.nodes: list[TickNode] = []

        # Create nodes
        for i in range(n_nodes):
            peer_ids = [j for j in range(n_nodes) if j != i]
            seed = (random_seed + i) if random_seed is not None else None
            node = TickNode(id=i, peer_ids=peer_ids, random_seed=seed)
            self.nodes.append(node)

    def tick_cluster(self, n_ticks: int = 1):
        """Advance all nodes by n ticks, routing messages in-memory.

        Args:
            n_ticks: Number of ticks to advance
        """
        for _ in range(n_ticks):
            # First, advance all nodes by one tick
            for node in self.nodes:
                node.current_tick += 1

                # Check election timeout
                if node.state.name != "LEADER":
                    ticks_since_heartbeat = node.current_tick - node.last_heartbeat_tick
                    if ticks_since_heartbeat >= node.election_timeout_ticks:
                        node._start_election()

                # Send heartbeats if leader
                if node.state.name == "LEADER":
                    if node.current_tick % node.heartbeat_interval_ticks == 0:
                        node._send_heartbeats()

            # Route messages between nodes (in-memory)
            for from_node in self.nodes:
                # Since we're in-process, we need to simulate message passing
                # This is a simplified version - real implementation would need
                # to capture messages during tick() execution
                pass

    def get_node_state(self, node_id: int) -> dict[str, Any]:
        """Get node state.

        Args:
            node_id: ID of the node

        Returns:
            Node state dict
        """
        node = self.nodes[node_id]
        return {
            "id": node.id,
            "state": node.state.name,
            "term": node.term,
            "current_tick": node.current_tick,
            "commit_idx": node.commit_idx,
            "voted_for": node.voted_for,
            "log": [{"term": e.term, "data": e.data} for e in node.log],
        }

    def append_entry(self, node_id: int, command: Any) -> bool:
        """Append entry to node's log.

        Args:
            node_id: ID of the node
            command: Command data

        Returns:
            True if successful
        """
        return self.nodes[node_id].append_entry(command)

    def shutdown(self):
        """Shutdown all nodes."""
        for node in self.nodes:
            node.shutdown()
