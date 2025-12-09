import socket, asyncio, random, json
from enum import Enum
from dataclasses import dataclass, asdict, field
from typing import Any


class State(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class MessageType:
    REQUEST_VOTE = "request_vote"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_ENTRIES_RESPONSE = "append_entries_response"


class Message:
    def to_bytes(self):
        return json.dumps(asdict(self)).encode("utf-8")


@dataclass
class RequestVote(Message):
    type: str = MessageType.REQUEST_VOTE
    term: int = 0
    candidate_id: int = 0
    last_log_index: int = 0
    last_log_term: int = 0


@dataclass
class VoteResponse(Message):
    type: str = MessageType.VOTE_RESPONSE
    term: int = 0
    vote_granted: bool = False


@dataclass
class AppendEntries(Message):
    type: str = MessageType.APPEND_ENTRIES
    term: int = 0
    leader_id: int = 0
    last_log_index: int = 0
    last_log_term: int = 0
    entries: list[Any] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesResponse(Message):
    type: str = MessageType.APPEND_ENTRIES_RESPONSE
    peer_id: int = 0
    term: int = 0
    success: bool = False
    match_idx: int = -1


@dataclass
class LogEntry(Message):
    data: Any = None
    term: int = 0


MESSAGE_CLASSES = {
    MessageType.REQUEST_VOTE: RequestVote,
    MessageType.VOTE_RESPONSE: VoteResponse,
    MessageType.APPEND_ENTRIES: AppendEntries,
    MessageType.APPEND_ENTRIES_RESPONSE: AppendEntriesResponse,
}


def decode_message(data: bytes) -> Message:
    obj = json.loads(data.decode("utf-8"))
    cls = MESSAGE_CLASSES[obj["type"]]
    return cls(**obj)


class Node:
    def __init__(self, addr="localhost", port=10000, id=0, peers=None, peer_ids=None):
        self.id, self.peers, self.peer_ids, self.addr, self.port = id, peers or [], peer_ids or [], addr, port

        self.state, self.term = State.FOLLOWER, 0
        self.log = []
        self.voted_for, self.votes_recvd = None, None

        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat = None

        self.sock = None

        self.commit_idx = -1
        self.last_applied = -1

        # state if leader - keyed by peer_id
        self.next_idx = {}
        self.match_idx = {}

        # shutdown flag
        self.running = True

    async def run(self):
        await asyncio.gather(
            self.listen_for_messages(),
            self.election_timer(),
            self.send_heartbeats(),
        )

    # Background tasks

    async def listen_for_messages(self):
        loop = asyncio.get_running_loop()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.addr, self.port))
        self.sock.setblocking(False)

        self.last_heartbeat = loop.time()

        while self.running:
            try:
                data, addr = await loop.sock_recvfrom(self.sock, 1024)
                await self.handle_message(data, addr)
            except Exception as e:
                if not self.running:
                    break
                raise

    async def election_timer(self):
        """Monitors election timeout and triggers elections"""
        while self.running:
            await asyncio.sleep(0.01)

            if self.state == State.LEADER:
                continue
            if self.last_heartbeat is None:
                continue

            elapsed = asyncio.get_running_loop().time() - self.last_heartbeat
            if elapsed >= self.election_timeout:
                await self.start_election()

    async def send_heartbeats(self):
        while self.running:
            await asyncio.sleep(0.05)

            if self.state != State.LEADER:
                continue

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

                await self.send_to(msg.to_bytes(), self.peers[i])

    # Core logic

    async def handle_message(self, data, addr):
        msg = decode_message(data)
        print(f"[Node {self.id}] [{self.state.name:9}] Received {msg.type} from {addr}")

        match msg:
            case RequestVote():
                await self.handle_request_vote(msg, addr)
            case VoteResponse():
                await self.handle_vote_response(msg, addr)
            case AppendEntries():
                await self.handle_append_entries(msg, addr)
            case AppendEntriesResponse():
                await self.handle_append_entries_response(msg, addr)
            case _:
                print("unknown message type")

    async def handle_request_vote(self, msg, addr):
        self.become_follower(msg.term)

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

        response = VoteResponse(term=self.term, vote_granted=vote_granted)
        await self.send_to(response.to_bytes(), addr)

    async def handle_vote_response(self, msg, addr):
        if self.become_follower(msg.term):
            return

        if self.state == State.CANDIDATE and msg.vote_granted:
            self.votes_recvd += 1
            total_nodes = len(self.peers) + 1
            print(f"[Node {self.id}] [CANDIDATE] Received vote ({self.votes_recvd}/{total_nodes})")

            if self.votes_recvd >= (total_nodes // 2 + 1):
                self.state = State.LEADER
                self.next_idx = {peer_id: len(self.log) for peer_id in self.peer_ids}
                self.match_idx = {peer_id: -1 for peer_id in self.peer_ids}
                print(f"[Node {self.id}] [LEADER   ] 🎉 WON ELECTION with {self.votes_recvd}/{total_nodes} votes in term {self.term}")

        self.reset_election_timer()

    async def handle_append_entries(self, msg, addr):
        reply = AppendEntriesResponse(peer_id=self.id, term=self.term, success=False)

        if msg.term < self.term:
            await self.send_to(reply.to_bytes(), addr)
            return

        if msg.term >= self.term:
            self.become_follower(msg.term)

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

            self.reset_election_timer()

        await self.send_to(reply.to_bytes(), addr)

    async def handle_append_entries_response(self, msg, addr):
        if msg.term >= self.term:
            self.become_follower(msg.term)

        # Log inconsistency - back up and retry
        if not msg.success:
            self.next_idx[msg.peer_id] = max(0, self.next_idx[msg.peer_id] - 1)
            return

        if self.state == State.LEADER and self.term == msg.term:
            # Update replication state for this peer
            self.match_idx[msg.peer_id] = msg.match_idx
            self.next_idx[msg.peer_id] = msg.match_idx + 1
            print(f"[Node {self.id}] [LEADER   ] Peer {msg.peer_id} replicated up to index {msg.match_idx}")

            # Now check if we can advance commit_idx
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

    async def start_election(self):
        self.state = State.CANDIDATE
        self.term += 1
        self.voted_for = self.id
        self.votes_recvd = 1
        self.reset_election_timer()
        print(f"[Node {self.id}] [CANDIDATE] 🗳️  Starting election for term {self.term}")

        msg = RequestVote(
            term=self.term,
            candidate_id=self.id,
            last_log_index=len(self.log) - 1,
            last_log_term=self.log[-1].term if self.log else -1,
        )

        await self.broadcast_to_peers(msg.to_bytes())

    # State management

    def become_follower(self, new_term):
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

    def become_leader(self):
        self.state = State.LEADER
        self.next_idx = {peer_id: len(self.log) for peer_id in self.peer_ids}
        self.match_idx = {peer_id: -1 for peer_id in self.peer_ids}
        print(f"node {self.id} became LEADER")

    # Utilities

    async def send_to(self, message, addr):
        if not self.running or not self.sock:
            return
        try:
            loop = asyncio.get_running_loop()
            await loop.sock_sendto(self.sock, message, addr)
        except (OSError, AttributeError):
            # Socket closed or invalid, ignore
            pass

    async def broadcast_to_peers(self, message):
        if not self.running or not self.sock:
            return
        loop = asyncio.get_running_loop()
        for peer_addr in self.peers:
            try:
                await loop.sock_sendto(self.sock, message, peer_addr)
            except (OSError, AttributeError):
                # Socket closed or invalid, ignore
                pass

    def reset_election_timer(self):
        """Called when receiving valid heartbeat or granting vote"""
        self.last_heartbeat = asyncio.get_running_loop().time()
        self.election_timeout = self._random_election_timeout()

    def _random_election_timeout(self):
        return random.uniform(0.3, 0.5)

    def shutdown(self):
        """Shutdown this node gracefully"""
        print(f"[Node {self.id}] Shutting down...")
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None


# Helper methods for testing
async def run_cluster(nodes):
    await asyncio.gather(*[n.run() for n in nodes])


def find_leader(nodes):
    for node in nodes:
        if node.state == State.LEADER:
            return node
    return None


async def submit_command(nodes, command):
    leader = find_leader(nodes)
    if leader is None:
        print(f"[CLIENT] No leader found, cannot submit command: {command}")
        return False

    print(f"[CLIENT] Submitting command '{command}' to leader (node {leader.id})")
    leader.log.append(LogEntry(data=command, term=leader.term))
    print(f"[CLIENT] Command appended to leader's log at index {len(leader.log) - 1}")
    return True


def verify_log_replication(nodes, expected_length=None):
    print(f"\n{'=' * 60}")
    print("VERIFYING LOG REPLICATION")
    print(f"{'=' * 60}")

    for node in nodes:
        log_str = [f"({e.term},{e.data})" for e in node.log]
        print(f"Node {node.id} [{node.state.name:9}] term={node.term} commit_idx={node.commit_idx} log={log_str}")

    # Check if all nodes have the same committed entries
    if expected_length is not None:
        all_match = all(len(node.log) >= expected_length for node in nodes)
        print(f"\nAll nodes have at least {expected_length} entries: {all_match}")

    print(f"{'=' * 60}\n")


async def wait_for_leader_election(nodes, timeout=5.0):
    print("[TEST] Waiting for leader election...")
    start = asyncio.get_running_loop().time()

    while asyncio.get_running_loop().time() - start < timeout:
        leader = find_leader(nodes)
        if leader is not None:
            print(f"[TEST] Leader elected: node {leader.id} in term {leader.term}")
            return leader
        await asyncio.sleep(0.1)

    print("[TEST] No leader elected within timeout")
    return None


async def wait_for_replication(nodes, expected_commit_idx, timeout=3.0):
    print(f"[TEST] Waiting for replication to commit_idx={expected_commit_idx}...")
    start = asyncio.get_running_loop().time()

    while asyncio.get_running_loop().time() - start < timeout:
        all_committed = all(node.commit_idx >= expected_commit_idx for node in nodes)
        if all_committed:
            print(f"[TEST] All nodes replicated to commit_idx={expected_commit_idx}")
            return True
        await asyncio.sleep(0.1)

    print(f"[TEST] Replication timeout - not all nodes reached commit_idx={expected_commit_idx}")
    for node in nodes:
        print(f"  Node {node.id}: commit_idx={node.commit_idx}")
    return False


# Test scenarios


async def test_basic_replication(nodes):
    """Test 1: Basic command replication with stable cluster"""
    print(f"\n{'#' * 60}")
    print("TEST 1: BASIC REPLICATION")
    print(f"{'#' * 60}\n")

    leader = await wait_for_leader_election(nodes)
    if not leader:
        return

    await asyncio.sleep(0.5)

    commands = ["cmd1", "cmd2", "cmd3"]
    for cmd in commands:
        await submit_command(nodes, cmd)
        await asyncio.sleep(0.3)  # Give time for replication

    await wait_for_replication(nodes, expected_commit_idx=2)

    verify_log_replication(nodes, expected_length=3)


async def test_leader_failure(nodes):
    """Test 2: Leader failure and re-election"""
    print(f"\n{'#' * 60}")
    print("TEST 2: LEADER FAILURE AND RE-ELECTION")
    print(f"{'#' * 60}\n")

    old_leader = await wait_for_leader_election(nodes)
    if not old_leader:
        return

    old_leader_id = old_leader.id
    print(f"[TEST] Simulating leader failure: stopping node {old_leader_id}")

    old_leader.state = State.FOLLOWER
    old_leader.reset_election_timer()

    await asyncio.sleep(0.7)  # Wait for new election

    new_leader = find_leader(nodes)
    if new_leader and new_leader.id != old_leader_id:
        print(f"[TEST] New leader elected: node {new_leader.id}")
    else:
        print("[TEST] Failed to elect new leader or same leader")

    verify_log_replication(nodes)


async def test_sequential_commands(nodes):
    """Test 3: Sequential command submission"""
    print(f"\n{'#' * 60}")
    print("TEST 3: SEQUENTIAL COMMANDS")
    print(f"{'#' * 60}\n")

    leader = await wait_for_leader_election(nodes)
    if not leader:
        return

    await asyncio.sleep(0.5)

    print("[TEST] Submitting 5 commands rapidly...")
    for i in range(5):
        await submit_command(nodes, f"rapid_{i}")

    await wait_for_replication(nodes, expected_commit_idx=4)

    verify_log_replication(nodes, expected_length=5)


async def run_tests(nodes):
    await asyncio.sleep(1.0)

    await test_basic_replication(nodes)
    await asyncio.sleep(1.0)

    await test_sequential_commands(nodes)
    await asyncio.sleep(1.0)

    await test_leader_failure(nodes)
    await asyncio.sleep(1.0)

    print(f"\n{'=' * 60}")
    print("ALL TESTS COMPLETED")
    print(f"{'=' * 60}\n")

    print("[TEST] Shutting down cluster...")
    for node in nodes:
        node.shutdown()


async def run_cluster_with_tests(nodes):
    await asyncio.gather(
        run_cluster(nodes),
        run_tests(nodes),
    )


if __name__ == "__main__":
    n_nodes, nodes = 3, []

    addrs = [("localhost", port) for port in range(10_000, 10_000 + n_nodes)]
    for id in range(len(addrs)):
        peers = addrs.copy()
        my_addr, my_port = peers.pop(id)
        peer_ids = [i for i in range(n_nodes) if i != id]
        nodes.append(Node(my_addr, my_port, id, peers, peer_ids))

    try:
        asyncio.run(run_cluster_with_tests(nodes))
    except KeyboardInterrupt:
        print("Shutting down!")
