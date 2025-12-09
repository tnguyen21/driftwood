"""CLI entry point for running a Raft node process.

This module allows starting a Raft node as a standalone process with
UDP communication. Control messages are sent via UDP to coordinate testing.

Usage:
    python -m raft.cli \
        --id 0 \
        --udp-port 10000 \
        --peers '[["localhost", 10001], ["localhost", 10002]]' \
        --peer-ids '[1, 2]'
"""

import argparse
import json
import signal
import sys

from raft.messages import MessageType, decode_message
from raft.node import TickNode


def main():
    parser = argparse.ArgumentParser(description="Run a tick-based Raft node")

    parser.add_argument("--id", type=int, required=True, help="Node ID (unique integer)")
    parser.add_argument("--udp-port", type=int, required=True, help="UDP port for Raft and control messages")
    parser.add_argument("--peers", required=True, help='JSON list of peer addresses: [["host", port], ...]')
    parser.add_argument("--peer-ids", required=True, help="JSON list of peer IDs: [0, 1, 2, ...]")
    parser.add_argument("--addr", default="localhost", help="Address to bind UDP socket (default: localhost)")
    parser.add_argument("--random-seed", type=int, help="Random seed for deterministic behavior")

    args = parser.parse_args()

    try:
        peers = json.loads(args.peers)
        peer_ids = json.loads(args.peer_ids)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON arguments: {e}")
        sys.exit(1)

    peers = [tuple(peer) for peer in peers]

    node = TickNode(id=args.id, peer_ids=peer_ids, random_seed=args.random_seed)
    node.start_udp(addr=args.addr, port=args.udp_port, peers=peers)

    # Set socket to blocking mode for control loop (tick() will temporarily set non-blocking)
    node.sock.setblocking(True)

    print(f"[Node {args.id}] Started successfully")
    print(f"[Node {args.id}]   UDP: {args.addr}:{args.udp_port}")
    print(f"[Node {args.id}]   Peers: {peer_ids}")

    # Setup signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        print(f"\n[Node {args.id}] Received signal {sig}, shutting down...")
        node.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Main control loop - wait for messages
    # Control messages trigger actions, Raft messages are handled during tick()
    while node.running:
        try:
            # Block waiting for next message
            data, addr = node.sock.recvfrom(1024)

            # Try to decode and check if it's a control message
            try:
                msg = decode_message(data)
                msg_type = msg.type

                # Check if it's a control message
                if msg_type in (
                    MessageType.CONTROL_TICK,
                    MessageType.CONTROL_QUERY_STATE,
                    MessageType.CONTROL_SUBMIT_COMMAND,
                    MessageType.CONTROL_PARTITION,
                    MessageType.CONTROL_SHUTDOWN,
                ):
                    # Handle control message
                    response = node.handle_control_message(data, addr)
                    if response:
                        node.sock.sendto(response, addr)
                else:
                    # It's a Raft message - handle it directly (shouldn't happen often in main loop)
                    node._handle_message(data, addr)

            except Exception as e:
                print(f"[Node {args.id}] Error processing message: {e}")

        except Exception as e:
            if node.running:
                print(f"[Node {args.id}] Error in main loop: {e}")
            break

    print(f"[Node {args.id}] Process exiting")


if __name__ == "__main__":
    main()
