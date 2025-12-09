import json
from dataclasses import asdict, dataclass, field
from typing import Any


class MessageType:
    REQUEST_VOTE = "request_vote"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_ENTRIES_RESPONSE = "append_entries_response"
    # Control messages for testing
    CONTROL_TICK = "control_tick"
    CONTROL_QUERY_STATE = "control_query_state"
    CONTROL_STATE_RESPONSE = "control_state_response"
    CONTROL_SUBMIT_COMMAND = "control_submit_command"
    CONTROL_SHUTDOWN = "control_shutdown"


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


# Control messages for testing
@dataclass
class ControlTick(Message):
    type: str = MessageType.CONTROL_TICK


@dataclass
class ControlQueryState(Message):
    type: str = MessageType.CONTROL_QUERY_STATE


@dataclass
class ControlStateResponse(Message):
    type: str = MessageType.CONTROL_STATE_RESPONSE
    node_id: int = 0
    state: str = ""
    term: int = 0
    current_tick: int = 0
    commit_idx: int = 0
    voted_for: int | None = None
    log: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ControlSubmitCommand(Message):
    type: str = MessageType.CONTROL_SUBMIT_COMMAND
    command: Any = None


@dataclass
class ControlShutdown(Message):
    type: str = MessageType.CONTROL_SHUTDOWN


MESSAGE_CLASSES = {
    MessageType.REQUEST_VOTE: RequestVote,
    MessageType.VOTE_RESPONSE: VoteResponse,
    MessageType.APPEND_ENTRIES: AppendEntries,
    MessageType.APPEND_ENTRIES_RESPONSE: AppendEntriesResponse,
    MessageType.CONTROL_TICK: ControlTick,
    MessageType.CONTROL_QUERY_STATE: ControlQueryState,
    MessageType.CONTROL_STATE_RESPONSE: ControlStateResponse,
    MessageType.CONTROL_SUBMIT_COMMAND: ControlSubmitCommand,
    MessageType.CONTROL_SHUTDOWN: ControlShutdown,
}


def decode_message(data: bytes) -> Message:
    obj = json.loads(data.decode("utf-8"))
    cls = MESSAGE_CLASSES[obj["type"]]
    return cls(**obj)
