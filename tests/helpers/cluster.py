"""Cluster management helpers for testing.

This module provides helpers for managing clusters of Raft nodes:
- MultiprocessCluster: Manages nodes running in separate processes with UDP control
"""

import json
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from raft.messages import (
    ControlQueryState,
    ControlSubmitCommand,
    ControlTick,
    decode_message,
)


class MultiprocessCluster:
    """Manages a cluster of Raft nodes in separate processes.

    Each node runs in its own process with UDP communication.
    The cluster manager coordinates tick advancement via UDP control messages.
    """

    def __init__(self, n_nodes: int, base_udp_port: int = 10000):
        """Initialize multiprocess cluster.

        Args:
            n_nodes: Number of nodes in the cluster
            base_udp_port: Starting port for UDP sockets
        """
        self.n_nodes = n_nodes
        self.processes: dict[int, subprocess.Popen] = {}
        self.udp_ports = {i: base_udp_port + i for i in range(n_nodes)}
        self.state_dir = Path(tempfile.mkdtemp(prefix="dinghy-raft-state-"))

        # Control socket for sending messages to nodes
        self.control_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.control_sock.settimeout(1.0)  # 1 second timeout for receives

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
            "--peers",
            json.dumps(peers),
            "--peer-ids",
            json.dumps(peer_ids),
            "--state-dir",
            str(self.state_dir),
        ]

        if random_seed is not None:
            cmd.extend(["--random-seed", str(random_seed)])

        # Don't capture output so we can see debug prints
        proc = subprocess.Popen(cmd)
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
        tick_msg = ControlTick()
        for _ in range(n_ticks):
            for node_id in range(self.n_nodes):
                if node_id in self.processes and self.processes[node_id].poll() is None:
                    try:
                        addr = ("localhost", self.udp_ports[node_id])
                        self.control_sock.sendto(tick_msg.to_bytes(), addr)
                        # Small delay to let tick complete before next one
                        time.sleep(0.001)
                    except Exception as e:
                        print(f"[Cluster] Error ticking node {node_id}: {e}")

    def get_node_state(self, node_id: int) -> dict[str, Any] | None:
        """Query node state via UDP control message.

        Args:
            node_id: ID of the node to query

        Returns:
            Node state dict or None if node is not running
        """
        if node_id not in self.processes or self.processes[node_id].poll() is not None:
            return None

        try:
            # Send query state message
            query_msg = ControlQueryState()
            addr = ("localhost", self.udp_ports[node_id])
            self.control_sock.sendto(query_msg.to_bytes(), addr)

            # Wait for response
            data, _ = self.control_sock.recvfrom(4096)
            response = decode_message(data)

            # Convert to dict format expected by tests
            return {
                "id": response.node_id,
                "state": response.state,
                "term": response.term,
                "current_tick": response.current_tick,
                "commit_idx": response.commit_idx,
                "voted_for": response.voted_for,
                "log": response.log,
            }
        except socket.timeout:
            print(f"[Cluster] Timeout getting state from node {node_id}")
            return None
        except Exception as e:
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

    def append_entry(self, node_id: int, command: Any) -> bool:
        """Submit a command to a node.

        Args:
            node_id: ID of the node to submit to
            command: Command data

        Returns:
            True if node is running (actual success depends on if node is leader)
        """
        try:
            submit_msg = ControlSubmitCommand(command=command)
            addr = ("localhost", self.udp_ports[node_id])
            self.control_sock.sendto(submit_msg.to_bytes(), addr)
            # Note: We don't wait for response, just assume it was delivered
            return True
        except Exception as e:
            print(f"[Cluster] Error appending entry to node {node_id}: {e}")
            return False

    def shutdown(self):
        """Shutdown all nodes."""
        print("[Cluster] Shutting down all nodes...")
        for node_id in list(self.processes.keys()):
            self.kill_node(node_id)
        # Close control socket
        try:
            self.control_sock.close()
        except Exception:
            pass
        try:
            shutil.rmtree(self.state_dir)
        except Exception:
            pass
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
