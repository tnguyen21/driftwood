import json
from dataclasses import asdict, dataclass, field
from typing import Any


class MessageType:
    REQUEST_VOTE = "request_vote"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_ENTRIES_RESPONSE = "append_entries_response"


class Message:
    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


@dataclass
class RequestVote(Message):
    type: str = MessageType.REQUEST_VOTE
    sender_id: int = 0
    term: int = 0
    last_log_index: int = 0
    last_log_term: int = 0


@dataclass
class VoteResponse(Message):
    type: str = MessageType.VOTE_RESPONSE
    sender_id: int = 0
    term: int = 0
    vote_granted: bool = False


@dataclass
class AppendEntries(Message):
    type: str = MessageType.APPEND_ENTRIES
    sender_id: int = 0
    term: int = 0
    last_log_index: int = 0
    last_log_term: int = 0
    entries: list[Any] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesResponse(Message):
    type: str = MessageType.APPEND_ENTRIES_RESPONSE
    sender_id: int = 0
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
