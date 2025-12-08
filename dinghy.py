import socket, asyncio, random, json
from enum import Enum, auto
from dataclasses import dataclass, asdict


class State(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class MessageType(Enum):
    REQUEST_VOTE = "request_vote"
    VOTE_RESPONSE = "vote_response"
    HEARTBEAT = "heartbeat"


class Message:
    def to_bytes(self):
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes):
        return cls(**json.loads(data.decode("utf-8")))


@dataclass
class RequestVote(Message):
    type: str = MessageType.REQUEST_VOTE.value
    term: int = 0
    candidate_id: int = 0
    last_log_index: int = 0
    last_log_term: int = 0


@dataclass
class VoteResponse(Message):
    type: str = MessageType.VOTE_RESPONSE.value
    term: int = 0
    vote_granted: bool = False


@dataclass
class Heartbeat(Message):
    type: str = MessageType.HEARTBEAT.value
    term: int = 0


class Node:
    def __init__(self, addr="localhost", port=10000, id=0, peers=[]):
        self.id, self.peers, self.addr, self.port = id, peers, addr, port

        self.state, self.term = State.FOLLOWER, 0

        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat = None
        self.voted_for, self.votes_recvd = None, None

        self.sock = None

    def become_follower(self, new_term):
        if new_term > self.term:
            self.term = new_term
            self.state = State.FOLLOWER
            self.voted_for = None
            self.votes_recvd = None
            return True
        return False

    def become_leader(self):
        self.state = State.LEADER
        print(f"node {self.id} became LEADER with {self.votes_recvd} votes")

    def _random_election_timeout(self):
        return random.uniform(0.3, 0.5)

    async def election_timer(self):
        """Monitors election timeout and triggers elections"""
        while True:
            await asyncio.sleep(0.01)

            if self.state == State.LEADER:
                continue
            if self.last_heartbeat is None:
                continue

            elapsed = asyncio.get_running_loop().time() - self.last_heartbeat
            if elapsed >= self.election_timeout:
                await self.start_election()

    async def broadcast_to_peers(self, message):
        loop = asyncio.get_running_loop()
        for peer_addr in self.peers:
            await loop.sock_sendto(self.sock, message, peer_addr)

    async def send_to(self, message, addr):
        loop = asyncio.get_running_loop()
        await loop.sock_sendto(self.sock, message, addr)

    async def start_election(self):
        print(f"Node {self.id}: Starting election for term {self.term + 1}")
        self.state = State.CANDIDATE
        self.term += 1
        self.voted_for = self.id
        self.votes_recvd = 1
        self.reset_election_timer()

        msg = RequestVote(
            term=self.term,
            candidate_id=self.id,
            last_log_index=0,
            last_log_term=0,
        )

        await self.broadcast_to_peers(msg.to_bytes())

    async def send_heartbeats(self):
        while True:
            await asyncio.sleep(0.05)

            if self.state != State.LEADER:
                continue

            msg = Heartbeat(term=self.term)
            await self.broadcast_to_peers(msg.to_bytes())

    async def listen_for_messages(self):
        loop = asyncio.get_running_loop()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.addr, self.port))
        self.sock.setblocking(False)

        self.last_heartbeat = loop.time()

        while True:
            data, addr = await loop.sock_recvfrom(self.sock, 1024)
            await self.handle_message(data, addr)

    async def handle_message(self, data, addr):
        """Process incoming message, potentially respond directly"""
        print(f"node {self.id}: recv from {addr}: {data}")
        parsed = json.loads(data.decode("utf-8"))
        msg_type = parsed.get("type")

        match msg_type:
            case MessageType.REQUEST_VOTE.value:
                _ = self.become_follower(parsed["term"])

                vote_granted = False
                if self.state == State.FOLLOWER and (self.voted_for is None or self.voted_for == parsed["candidate_id"]):
                    vote_granted = True
                    self.voted_for = parsed["candidate_id"]

                msg = VoteResponse(term=self.term, vote_granted=vote_granted)
                await self.send_to(msg.to_bytes(), addr)

            case MessageType.VOTE_RESPONSE.value:
                if self.become_follower(parsed["term"]):
                    return

                if self.state == State.CANDIDATE and parsed["vote_granted"]:
                    self.votes_recvd += 1
                    total_nodes = len(self.peers) + 1

                    if self.votes_recvd >= (total_nodes // 2 + 1):
                        self.state = State.LEADER
                        print(f"node {self.id} became LEADER with {self.votes_recvd} votes")

            case MessageType.HEARTBEAT.value:
                if parsed["term"] >= self.term:
                    self.state = State.FOLLOWER
                    self.term = parsed["term"]
                    self.voted_for = None
            case _:
                print("unknown message type")

        self.reset_election_timer()

    def reset_election_timer(self):
        """Called when receiving valid heartbeat or granting vote"""
        self.last_heartbeat = asyncio.get_running_loop().time()
        self.election_timeout = self._random_election_timeout()

    async def run(self):
        await asyncio.gather(
            self.send_heartbeats(),
            self.election_timer(),
            self.listen_for_messages(),
        )


async def run_cluster(nodes):
    await asyncio.gather(*[n.run() for n in nodes])


if __name__ == "__main__":
    n_nodes, nodes = 3, []

    addrs = [("localhost", port) for port in range(10_000, 10_000 + n_nodes)]
    for id in range(len(addrs)):
        peers = addrs.copy()
        my_addr, my_port = peers.pop(id)
        nodes.append(Node(my_addr, my_port, id, peers))

    try:
        asyncio.run(run_cluster(nodes))
    except KeyboardInterrupt:
        print("\nShutting down cluster...")
        for n in nodes:
            if n.sock:
                n.sock.close()
