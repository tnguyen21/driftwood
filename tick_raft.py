import random
from dataclasses import asdict
from typing import Any

from raft import (
    State,
    RequestVote,
    VoteResponse,
    AppendEntries,
    AppendEntriesResponse,
    LogEntry,
    decode_message,
)


class TickNode:
    """Tick-based Raft node for deterministic testing.

    Instead of async timers, this implementation uses tick() to advance state.
    This makes testing completely deterministic - no race conditions or timing issues.
    """

    def __init__(self, id=0, peer_ids=None):
        self.id = id
        self.peer_ids = peer_ids or []

        # Raft state
        self.state = State.FOLLOWER
        self.term = 0
        self.log = []
        self.voted_for = None
        self.votes_recvd = None

        # Tick-based timing (instead of timestamps)
        self.current_tick = 0
        self.last_heartbeat_tick = 0
        self.election_timeout_ticks = self._random_election_timeout_ticks()
        self.heartbeat_interval_ticks = 50  # send heartbeat every 50 ticks

        # Commit state
        self.commit_idx = -1
        self.last_applied = -1

        # Leader state - keyed by peer_id
        self.next_idx = {}
        self.match_idx = {}

        # Message queue for testing
        self.message_queue = []

        # Outbound message buffer (messages to send)
        self.outbound_messages = []

    def tick(self):
        """Advance the node by one tick.

        This is the main entry point for deterministic testing.
        Each tick:
        1. Processes pending messages
        2. Checks election timeout
        3. Sends heartbeats if leader
        """
        self.current_tick += 1

        # Process all pending messages
        while self.message_queue:
            msg, from_node_id = self.message_queue.pop(0)
            self._handle_message(msg, from_node_id)

        # Check election timeout (if not leader)
        if self.state != State.LEADER:
            ticks_since_heartbeat = self.current_tick - self.last_heartbeat_tick
            if ticks_since_heartbeat >= self.election_timeout_ticks:
                self._start_election()

        # Send heartbeats (if leader)
        if self.state == State.LEADER:
            if self.current_tick % self.heartbeat_interval_ticks == 0:
                self._send_heartbeats()

    def receive_message(self, msg_bytes: bytes, from_node_id: int):
        """Queue a message to be processed on the next tick."""
        msg = decode_message(msg_bytes)
        self.message_queue.append((msg, from_node_id))

    def get_outbound_messages(self):
        """Get and clear all outbound messages."""
        messages = self.outbound_messages
        self.outbound_messages = []
        return messages

    # Core Raft logic (synchronous versions)

    def _handle_message(self, msg, from_node_id):
        """Handle an incoming message (synchronous)."""
        print(f"[Node {self.id}] [{self.state.name:9}] [tick {self.current_tick:4}] Received {msg.type} from node {from_node_id}")

        match msg:
            case RequestVote():
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
                    print(
                        f"[Node {self.id}] [{self.state.name:9}] Denying vote to candidate {msg.candidate_id} - already voted for {self.voted_for}"
                    )

                if vote_granted:
                    self._reset_election_timer()

                response = VoteResponse(term=self.term, vote_granted=vote_granted)
                self._send_to(response, from_node_id)

            case VoteResponse():
                if self._become_follower(msg.term):
                    return

                if self.state == State.CANDIDATE and msg.vote_granted:
                    self.votes_recvd += 1
                    total_nodes = len(self.peer_ids) + 1
                    print(f"[Node {self.id}] [CANDIDATE] Received vote ({self.votes_recvd}/{total_nodes})")

                    if self.votes_recvd >= (total_nodes // 2 + 1):
                        self._become_leader()

                self._reset_election_timer()

            case AppendEntries():
                reply = AppendEntriesResponse(peer_id=self.id, term=self.term, success=False)

                if msg.term < self.term:
                    self._send_to(reply, from_node_id)
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
                        print(
                            f"[Node {self.id}] [{self.state.name:9}] Appended {len(entries[new_entries_idx:])} entries at index {log_insert_idx}"
                        )

                    # Set match_idx after appending entries
                    reply.match_idx = msg.last_log_index + len(entries)

                    if msg.leader_commit > self.commit_idx:
                        old_commit = self.commit_idx
                        self.commit_idx = min(msg.leader_commit, len(self.log) - 1)
                        print(f"[Node {self.id}] [{self.state.name:9}] Advanced commit_idx from {old_commit} to {self.commit_idx}")

                    self._reset_election_timer()

                self._send_to(reply, from_node_id)

            case AppendEntriesResponse():
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

            case _:
                print("unknown message type")

    def _start_election(self):
        """Start a new election."""
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
        """Send heartbeats/log entries to all peers."""
        for peer_id in self.peer_ids:
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

            self._send_to(msg, peer_id)

    # State transitions

    def _become_follower(self, new_term):
        """Transition to follower state if term is higher."""
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
        """Transition to leader state."""
        self.state = State.LEADER
        self.next_idx = {peer_id: len(self.log) for peer_id in self.peer_ids}
        self.match_idx = {peer_id: -1 for peer_id in self.peer_ids}
        print(
            f"[Node {self.id}] [LEADER   ] [tick {self.current_tick:4}] Won election with {self.votes_recvd}/{len(self.peer_ids) + 1} votes in term {self.term}"
        )

    # Utilities

    def _reset_election_timer(self):
        """Reset the election timeout."""
        self.last_heartbeat_tick = self.current_tick
        self.election_timeout_ticks = self._random_election_timeout_ticks()

    def _random_election_timeout_ticks(self):
        """Generate a random election timeout in ticks."""
        return random.randint(150, 300)

    def _send_to(self, msg, to_node_id):
        """Queue a message to send to a specific node."""
        self.outbound_messages.append((msg.to_bytes(), to_node_id))

    def _broadcast(self, msg):
        """Queue a message to broadcast to all peers."""
        for peer_id in self.peer_ids:
            self._send_to(msg, peer_id)

    def append_entry(self, data: Any):
        """Append a new entry to the log (only valid for leader)."""
        if self.state != State.LEADER:
            return False

        entry = LogEntry(data=data, term=self.term)
        self.log.append(entry)
        print(f"[Node {self.id}] [LEADER   ] Appended entry '{data}' to log at index {len(self.log) - 1}")
        return True


# Test helpers for deterministic testing


def tick_cluster(nodes, n_ticks=1):
    """Advance all nodes by n ticks, routing messages between them."""
    for _ in range(n_ticks):
        for node in nodes:
            node.tick()

        # Route messages between nodes
        all_messages = []
        for node in nodes:
            messages = node.get_outbound_messages()
            for msg_bytes, to_node_id in messages:
                all_messages.append((msg_bytes, node.id, to_node_id))

        # Deliver messages
        for msg_bytes, from_node_id, to_node_id in all_messages:
            target_node = next((n for n in nodes if n.id == to_node_id), None)
            if target_node:
                target_node.receive_message(msg_bytes, from_node_id)


def find_leader(nodes):
    """Find the current leader in the cluster."""
    for node in nodes:
        if node.state == State.LEADER:
            return node
    return None


def verify_log_replication(nodes):
    """Print the log state of all nodes."""
    print(f"\n{'=' * 80}")
    print("LOG REPLICATION STATUS")
    print(f"{'=' * 80}")

    for node in nodes:
        log_str = [f"({e.term},{e.data})" for e in node.log]
        print(
            f"Node {node.id} [{node.state.name:9}] term={node.term:2} commit_idx={node.commit_idx:2} tick={node.current_tick:4} log={log_str}"
        )

    print(f"{'=' * 80}\n")


# Example test


def test_basic_election():
    """Deterministic test: leader election within 300 ticks."""
    print(f"\n{'#' * 80}")
    print("TEST: BASIC LEADER ELECTION")
    print(f"{'#' * 80}\n")

    nodes = [TickNode(id=i, peer_ids=[j for j in range(3) if j != i]) for i in range(3)]

    # Tick until a leader is elected (should happen within 300 ticks)
    for tick in range(300):
        tick_cluster(nodes)
        leader = find_leader(nodes)
        if leader:
            print(f"\n[TEST] Leader elected after {tick + 1} ticks: node {leader.id} in term {leader.term}")
            break

    assert leader is not None, "No leader elected within 300 ticks"
    verify_log_replication(nodes)

    return nodes, leader


def test_log_replication():
    """Deterministic test: log replication."""
    print(f"\n{'#' * 80}")
    print("TEST: LOG REPLICATION")
    print(f"{'#' * 80}\n")

    nodes, leader = test_basic_election()

    print(f"\n[TEST] Submitting 3 commands to leader (node {leader.id})...")
    leader.append_entry("cmd1")
    leader.append_entry("cmd2")
    leader.append_entry("cmd3")

    print(f"[TEST] Ticking cluster to replicate logs...")
    tick_cluster(nodes, n_ticks=100)

    verify_log_replication(nodes)

    assert all(len(node.log) >= 3 for node in nodes), "Not all nodes replicated entries"
    assert all(node.commit_idx >= 2 for node in nodes), "Not all nodes committed entries"

    print("[TEST] All nodes successfully replicated and committed entries!")


if __name__ == "__main__":
    test_log_replication()
