"""Tick-based Raft node implementation with UDP communication.

This module provides a deterministic Raft node that uses tick-based logical clocks
instead of real-time timers. The node communicates with peers over UDP sockets,
but time advancement is controlled externally via the tick() method.
"""

import random
import socket
from dataclasses import asdict
from enum import Enum
from typing import Any

from raft.messages import (
    AppendEntries,
    AppendEntriesResponse,
    LogEntry,
    RequestVote,
    VoteResponse,
    decode_message,
)


class State(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class TickNode:
    def __init__(self, id: int = 0, peer_ids: list[int] | None = None, random_seed: int | None = None):
        self.id = id
        self.peer_ids = peer_ids or []

        self._random = random.Random(random_seed)

        # Raft state
        self.state = State.FOLLOWER
        self.term = 0
        self.log: list[LogEntry] = []
        self.voted_for: int | None = None
        self.votes_recvd: int | None = None

        # Tick-based timing
        self.current_tick = 0
        self.last_heartbeat_tick = 0
        self.election_timeout_ticks = self._random_election_timeout_ticks()
        self.heartbeat_interval_ticks = 50  # send heartbeat every 50 ticks

        # Commit state
        self.commit_idx = -1
        self.last_applied = -1

        # Leader state - keyed by peer_id
        self.next_idx: dict[int, int] = {}
        self.match_idx: dict[int, int] = {}

        # UDP socket and peer addresses
        self.sock: socket.socket | None = None
        self.peers: list[tuple[str, int]] = []  # (host, port) for each peer

        # Network partition simulation
        self.partition_state = {"isolated": False, "allowed_peers": None}

        # Running flag
        self.running = True

    def start_udp(self, addr: str = "localhost", port: int = 10000, peers: list[tuple[str, int]] | None = None):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((addr, port))
        self.sock.setblocking(False)  # Non-blocking for tick-based processing
        self.peers = peers or []
        print(f"[Node {self.id}] UDP socket started on {addr}:{port}")

    def tick(self):
        """Advance the node by one tick.

        This is the main entry point for deterministic testing.
        Each tick:
        1. Processes any pending UDP messages (non-blocking)
        2. Checks election timeout
        3. Sends heartbeats if leader
        """
        if not self.running:
            return

        self.current_tick += 1

        # Debug: print every 50 ticks
        if self.current_tick % 50 == 0:
            ticks_since_hb = self.current_tick - self.last_heartbeat_tick
            print(f"[Node {self.id}] Tick {self.current_tick}: state={self.state.name}, term={self.term}, ticks_since_hb={ticks_since_hb}, timeout={self.election_timeout_ticks}")

        # Process all available UDP messages (non-blocking)
        while True:
            try:
                data, addr = self.sock.recvfrom(1024)
                self._handle_message(data, addr)
            except BlockingIOError:
                # No more messages available
                break
            except Exception as e:
                print(f"[Node {self.id}] Error receiving message: {e}")
                break

        # Check election timeout (if not leader)
        if self.state != State.LEADER:
            ticks_since_heartbeat = self.current_tick - self.last_heartbeat_tick
            if ticks_since_heartbeat >= self.election_timeout_ticks:
                self._start_election()

        # Send heartbeats (if leader)
        if self.state == State.LEADER:
            if self.current_tick % self.heartbeat_interval_ticks == 0:
                self._send_heartbeats()

    # Core Raft logic (synchronous versions)

    def _handle_message(self, data: bytes, addr: tuple[str, int]):
        try:
            msg = decode_message(data)
        except Exception as e:
            print(f"[Node {self.id}] Error decoding message: {e}")
            return

        # Extract sender ID from the message itself (not from address)
        # Different message types have sender info in different fields
        from_node_id = None
        if hasattr(msg, 'candidate_id'):
            from_node_id = msg.candidate_id
        elif hasattr(msg, 'leader_id'):
            from_node_id = msg.leader_id
        elif hasattr(msg, 'peer_id'):
            from_node_id = msg.peer_id

        # Check partition state (if we can identify sender)
        if from_node_id is not None and not self._should_accept_message(from_node_id):
            return

        if from_node_id is not None:
            print(f"[Node {self.id}] [{self.state.name:9}] [tick {self.current_tick:4}] Received {msg.type} from node {from_node_id}")
        else:
            print(f"[Node {self.id}] [{self.state.name:9}] [tick {self.current_tick:4}] Received {msg.type} from {addr}")

        match msg:
            case RequestVote():
                self._handle_request_vote(msg, addr)
            case VoteResponse():
                self._handle_vote_response(msg, addr)
            case AppendEntries():
                self._handle_append_entries(msg, addr)
            case AppendEntriesResponse():
                self._handle_append_entries_response(msg, addr)
            case _:
                print(f"[Node {self.id}] Unknown message type")

    def _handle_request_vote(self, msg: RequestVote, addr: tuple[str, int]):
        self._become_follower(msg.term)

        vote_granted = False
        if self.voted_for is None or self.voted_for == msg.candidate_id:
            my_last_idx = len(self.log) - 1
            my_last_term = self.log[my_last_idx].term if self.log else -1

            log_ok = msg.last_log_term > my_last_term or (msg.last_log_term == my_last_term and msg.last_log_index >= my_last_idx)

            if log_ok:
                vote_granted = True
                self.voted_for = msg.candidate_id
                print(f"[Node {self.id}] [{self.state.name:9}] Granting vote to candidate {msg.candidate_id} for term {self.term}")
            else:
                print(f"[Node {self.id}] [{self.state.name:9}] Denying vote to candidate {msg.candidate_id} - log not up-to-date")
        else:
            print(f"[Node {self.id}] [{self.state.name:9}] Denying vote to candidate {msg.candidate_id} - already voted for {self.voted_for}")

        if vote_granted:
            self._reset_election_timer()

        response = VoteResponse(term=self.term, vote_granted=vote_granted)
        self._send_to_addr(response, addr)

    def _handle_vote_response(self, msg: VoteResponse, addr: tuple[str, int]):
        if self._become_follower(msg.term):
            return

        if self.state == State.CANDIDATE and msg.vote_granted:
            self.votes_recvd += 1
            total_nodes = len(self.peer_ids) + 1
            print(f"[Node {self.id}] [CANDIDATE] Received vote ({self.votes_recvd}/{total_nodes})")

            if self.votes_recvd >= (total_nodes // 2 + 1):
                self._become_leader()

        self._reset_election_timer()

    def _handle_append_entries(self, msg: AppendEntries, addr: tuple[str, int]):
        reply = AppendEntriesResponse(peer_id=self.id, term=self.term, success=False)

        if msg.term < self.term:
            self._send_to_addr(reply, addr)
            return

        if msg.term >= self.term:
            self._become_follower(msg.term)

        if msg.last_log_index == -1 or (msg.last_log_index < len(self.log) and self.log[msg.last_log_index].term == msg.last_log_term):
            reply.success = True

            log_insert_idx = msg.last_log_index + 1
            new_entries_idx = 0
            entries = [LogEntry(**e) for e in msg.entries]

            while log_insert_idx < len(self.log) and new_entries_idx < len(entries):
                if self.log[log_insert_idx].term != entries[new_entries_idx].term:
                    break
                log_insert_idx += 1
                new_entries_idx += 1

            # Truncate conflicting entries and append new ones
            if new_entries_idx < len(entries):
                self.log = self.log[:log_insert_idx] + entries[new_entries_idx:]
                print(f"[Node {self.id}] [{self.state.name:9}] Appended {len(entries[new_entries_idx:])} entries at index {log_insert_idx}")

            # Set match_idx after appending entries
            reply.match_idx = msg.last_log_index + len(entries)

            if msg.leader_commit > self.commit_idx:
                old_commit = self.commit_idx
                self.commit_idx = min(msg.leader_commit, len(self.log) - 1)
                print(f"[Node {self.id}] [{self.state.name:9}] Advanced commit_idx from {old_commit} to {self.commit_idx}")

            self._reset_election_timer()

        self._send_to_addr(reply, addr)

    def _handle_append_entries_response(self, msg: AppendEntriesResponse, addr: tuple[str, int]):
        if msg.term >= self.term:
            self._become_follower(msg.term)

        # Log inconsistency - back up and retry
        if not msg.success:
            self.next_idx[msg.peer_id] = max(0, self.next_idx[msg.peer_id] - 1)
            return

        if self.state == State.LEADER and self.term == msg.term:
            # Update replication state for this peer
            self.match_idx[msg.peer_id] = msg.match_idx
            self.next_idx[msg.peer_id] = msg.match_idx + 1
            print(f"[Node {self.id}] [LEADER   ] Peer {msg.peer_id} replicated up to index {msg.match_idx}")

            # Check if we can advance commit_idx
            old_commit = self.commit_idx
            for i in range(self.commit_idx + 1, len(self.log)):
                if self.log[i].term == self.term:  # only commit current term entries
                    match_count = 1  # leader counts as having it
                    for peer_id, peer_match in self.match_idx.items():
                        if peer_match >= i:
                            match_count += 1
                    if match_count * 2 > len(self.peer_ids) + 1:
                        self.commit_idx = i

            if self.commit_idx > old_commit:
                print(f"[Node {self.id}] [LEADER   ] Advanced commit_idx from {old_commit} to {self.commit_idx}")

    def _start_election(self):
        self.state = State.CANDIDATE
        self.term += 1
        self.voted_for = self.id
        self.votes_recvd = 1
        self._reset_election_timer()
        print(f"[Node {self.id}] [CANDIDATE] [tick {self.current_tick:4}] Starting election for term {self.term}")

        msg = RequestVote(
            term=self.term,
            candidate_id=self.id,
            last_log_index=len(self.log) - 1,
            last_log_term=self.log[-1].term if self.log else -1,
        )

        self._broadcast(msg)

    def _send_heartbeats(self):
        for i, peer_id in enumerate(self.peer_ids):
            peer_next_idx = self.next_idx[peer_id]
            last_log_idx = peer_next_idx - 1
            last_log_term = -1
            if last_log_idx >= 0:
                last_log_term = self.log[last_log_idx].term
            entries = self.log[peer_next_idx:]

            msg = AppendEntries(
                term=self.term,
                leader_id=self.id,
                last_log_index=last_log_idx,
                last_log_term=last_log_term,
                entries=[asdict(e) for e in entries],
                leader_commit=self.commit_idx,
            )

            self._send_to_peer(msg, i)

    # State transitions

    def _become_follower(self, new_term: int) -> bool:
        if new_term > self.term:
            old_state = self.state
            self.term = new_term
            self.state = State.FOLLOWER
            self.voted_for = None
            self.votes_recvd = None
            if old_state != State.FOLLOWER:
                print(f"[Node {self.id}] [{old_state.name:9}] Stepping down to FOLLOWER for term {new_term}")
            return True
        return False

    def _become_leader(self):
        self.state = State.LEADER
        self.next_idx = {peer_id: len(self.log) for peer_id in self.peer_ids}
        self.match_idx = {peer_id: -1 for peer_id in self.peer_ids}
        print(
            f"[Node {self.id}] [LEADER   ] [tick {self.current_tick:4}] Won election with {self.votes_recvd}/{len(self.peer_ids) + 1} votes in term {self.term}"
        )

    # Network and partition management

    def set_partition(self, isolated: bool = False, allowed_peers: list[int] | None = None):
        """Configure network partition for testing.

        Args:
            isolated: If True, drop all incoming messages
            allowed_peers: If set, only accept messages from these peer IDs
        """
        self.partition_state = {"isolated": isolated, "allowed_peers": allowed_peers}
        print(f"[Node {self.id}] Partition state updated: {self.partition_state}")

    def _should_accept_message(self, from_peer_id: int) -> bool:
        """Check if message should be dropped due to partition.

        Args:
            from_peer_id: ID of the peer sending the message

        Returns:
            True if message should be accepted, False if it should be dropped
        """
        if self.partition_state["isolated"]:
            return False
        if self.partition_state["allowed_peers"] is not None:
            return from_peer_id in self.partition_state["allowed_peers"]
        return True

    def _get_node_id_from_addr(self, addr: tuple[str, int]) -> int | None:
        try:
            idx = self.peers.index(addr)
            return self.peer_ids[idx]
        except (ValueError, IndexError):
            return None

    def _send_to_addr(self, msg, addr: tuple[str, int]):
        if not self.running or not self.sock:
            return
        try:
            self.sock.sendto(msg.to_bytes(), addr)
        except Exception as e:
            print(f"[Node {self.id}] Error sending to {addr}: {e}")

    def _send_to_peer(self, msg, peer_idx: int):
        if peer_idx < len(self.peers):
            self._send_to_addr(msg, self.peers[peer_idx])

    def _broadcast(self, msg):
        for i in range(len(self.peers)):
            self._send_to_peer(msg, i)

    # Utilities

    def _reset_election_timer(self):
        self.last_heartbeat_tick = self.current_tick
        self.election_timeout_ticks = self._random_election_timeout_ticks()

    def _random_election_timeout_ticks(self) -> int:
        return self._random.randint(150, 300)

    def append_entry(self, data: Any) -> bool:
        if self.state != State.LEADER:
            return False

        entry = LogEntry(data=data, term=self.term)
        self.log.append(entry)
        print(f"[Node {self.id}] [LEADER   ] Appended entry '{data}' to log at index {len(self.log) - 1}")
        return True

    def shutdown(self):
        print(f"[Node {self.id}] Shutting down...")
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
