"""HTTP control server for tick-based Raft node.

This module provides an HTTP server that allows external test coordinators
to control a Raft node. It exposes endpoints for:
- Tick advancement (deterministic time progression)
- State inspection
- Client command submission
- Network partition simulation
- Graceful shutdown
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from raft.node import TickNode


class ControlHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress default logging

    def do_POST(self):
        if self.path == "/tick":
            self.server.node.tick()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {"status": "ok", "current_tick": self.server.node.current_tick}
            self.wfile.write(json.dumps(response).encode())

        elif self.path == "/append_entry":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            data = json.loads(body.decode())

            success = self.server.node.append_entry(data["command"])

            self.send_response(200 if success else 400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {"success": success, "is_leader": self.server.node.state.name == "LEADER"}
            self.wfile.write(json.dumps(response).encode())

        elif self.path == "/partition":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            data = json.loads(body.decode())

            isolated = data.get("isolated", False)
            allowed_peers = data.get("allowed_peers")

            self.server.node.set_partition(isolated=isolated, allowed_peers=allowed_peers)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {"status": "ok", "partition_state": self.server.node.partition_state}
            self.wfile.write(json.dumps(response).encode())

        elif self.path == "/shutdown":
            self.server.node.shutdown()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {"status": "shutting_down"}
            self.wfile.write(json.dumps(response).encode())

            # Shutdown the HTTP server in a separate thread
            threading.Thread(target=self.server.shutdown).start()

        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path == "/state":
            state = {
                "id": self.server.node.id,
                "state": self.server.node.state.name,
                "term": self.server.node.term,
                "current_tick": self.server.node.current_tick,
                "commit_idx": self.server.node.commit_idx,
                "voted_for": self.server.node.voted_for,
                "log": [{"term": e.term, "data": e.data} for e in self.server.node.log],
                "next_idx": self.server.node.next_idx if self.server.node.state.name == "LEADER" else {},
                "match_idx": self.server.node.match_idx if self.server.node.state.name == "LEADER" else {},
            }

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(state).encode())

        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {"status": "healthy", "node_id": self.server.node.id, "running": self.server.node.running}
            self.wfile.write(json.dumps(response).encode())

        else:
            self.send_response(404)
            self.end_headers()


class ControlServer(HTTPServer):
    """HTTP server with reference to Raft node.

    This server runs in a separate thread to avoid blocking the node's
    tick-based processing loop.
    """

    def __init__(self, node: "TickNode", host: str = "localhost", port: int = 20000):
        super().__init__((host, port), ControlHandler)
        self.node = node
        print(f"[Node {node.id}] HTTP control server started on {host}:{port}")

    def run_in_thread(self):
        thread = threading.Thread(target=self.serve_forever, daemon=True)
        thread.start()
        return thread
