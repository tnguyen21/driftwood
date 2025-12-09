"""CLI entry point for running a Raft node process.

This module allows starting a Raft node as a standalone process with
UDP communication and HTTP control server. Used by multiprocess tests.

Usage:
    python -m raft.cli \
        --id 0 \
        --udp-port 10000 \
        --http-port 20000 \
        --peers '[["localhost", 10001], ["localhost", 10002]]' \
        --peer-ids '[1, 2]'
"""

import argparse
import json
import signal
import sys
import time

from raft.node import TickNode
from raft.server import ControlServer


def main():
    """Main entry point for node process."""
    parser = argparse.ArgumentParser(description="Run a tick-based Raft node")

    parser.add_argument("--id", type=int, required=True, help="Node ID (unique integer)")
    parser.add_argument("--udp-port", type=int, required=True, help="UDP port for Raft messages")
    parser.add_argument("--http-port", type=int, required=True, help="HTTP port for control server")
    parser.add_argument("--peers", required=True, help='JSON list of peer addresses: [["host", port], ...]')
    parser.add_argument("--peer-ids", required=True, help="JSON list of peer IDs: [0, 1, 2, ...]")
    parser.add_argument("--addr", default="localhost", help="Address to bind UDP socket (default: localhost)")
    parser.add_argument("--http-addr", default="localhost", help="Address to bind HTTP server (default: localhost)")
    parser.add_argument("--random-seed", type=int, help="Random seed for deterministic behavior")

    args = parser.parse_args()

    # Parse JSON arguments
    try:
        peers = json.loads(args.peers)
        peer_ids = json.loads(args.peer_ids)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON arguments: {e}")
        sys.exit(1)

    # Convert peers to tuples
    peers = [tuple(peer) for peer in peers]

    # Create node
    node = TickNode(id=args.id, peer_ids=peer_ids, random_seed=args.random_seed)

    # Start UDP socket
    node.start_udp(addr=args.addr, port=args.udp_port, peers=peers)

    # Start HTTP control server in background thread
    server = ControlServer(node, host=args.http_addr, port=args.http_port)
    _ = server.run_in_thread()  # Start in background, we don't need the thread reference

    print(f"[Node {args.id}] Started successfully")
    print(f"[Node {args.id}]   UDP: {args.addr}:{args.udp_port}")
    print(f"[Node {args.id}]   HTTP: {args.http_addr}:{args.http_port}")
    print(f"[Node {args.id}]   Peers: {peer_ids}")

    # Setup signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        print(f"\n[Node {args.id}] Received signal {sig}, shutting down...")
        node.shutdown()
        server.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Keep the process alive
    try:
        while node.running:
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass

    print(f"[Node {args.id}] Process exiting")


if __name__ == "__main__":
    main()
