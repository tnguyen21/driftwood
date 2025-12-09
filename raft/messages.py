"""Message types for Raft consensus protocol.

This module defines all message types used for communication between Raft nodes:
- RequestVote: Candidate requests votes during election
- VoteResponse: Response to vote request
- AppendEntries: Leader replicates log entries (also serves as heartbeat)
- AppendEntriesResponse: Response to append entries request
- LogEntry: Individual log entry with data and term
"""

import json
from dataclasses import asdict, dataclass, field
from typing import Any


class MessageType:
    """Message type constants."""

    REQUEST_VOTE = "request_vote"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_ENTRIES_RESPONSE = "append_entries_response"


class Message:
    """Base message class with serialization."""

    def to_bytes(self) -> bytes:
        """Serialize message to bytes for transmission."""
        return json.dumps(asdict(self)).encode("utf-8")


@dataclass
class RequestVote(Message):
    """Request vote message sent by candidate during election.

    Attributes:
        type: Message type identifier
        term: Candidate's term number
        candidate_id: ID of the candidate requesting vote
        last_log_index: Index of candidate's last log entry
        last_log_term: Term of candidate's last log entry
    """

    type: str = MessageType.REQUEST_VOTE
    term: int = 0
    candidate_id: int = 0
    last_log_index: int = 0
    last_log_term: int = 0


@dataclass
class VoteResponse(Message):
    """Response to vote request.

    Attributes:
        type: Message type identifier
        term: Current term of the responding node
        vote_granted: Whether the vote was granted
    """

    type: str = MessageType.VOTE_RESPONSE
    term: int = 0
    vote_granted: bool = False


@dataclass
class AppendEntries(Message):
    """Append entries message sent by leader.

    Used for both log replication and heartbeats (when entries is empty).

    Attributes:
        type: Message type identifier
        term: Leader's term
        leader_id: Leader's node ID
        last_log_index: Index of log entry immediately preceding new ones
        last_log_term: Term of last_log_index entry
        entries: Log entries to append (empty for heartbeat)
        leader_commit: Leader's commit index
    """

    type: str = MessageType.APPEND_ENTRIES
    term: int = 0
    leader_id: int = 0
    last_log_index: int = 0
    last_log_term: int = 0
    entries: list[Any] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesResponse(Message):
    """Response to append entries request.

    Attributes:
        type: Message type identifier
        peer_id: ID of the responding node
        term: Current term of the responding node
        success: True if follower contained entry matching last_log_index and last_log_term
        match_idx: Index of highest log entry known to be replicated on this node
    """

    type: str = MessageType.APPEND_ENTRIES_RESPONSE
    peer_id: int = 0
    term: int = 0
    success: bool = False
    match_idx: int = -1


@dataclass
class LogEntry(Message):
    """Individual log entry in the Raft log.

    Attributes:
        data: Client command data
        term: Term when entry was received by leader
    """

    data: Any = None
    term: int = 0


# Mapping of message type strings to message classes
MESSAGE_CLASSES = {
    MessageType.REQUEST_VOTE: RequestVote,
    MessageType.VOTE_RESPONSE: VoteResponse,
    MessageType.APPEND_ENTRIES: AppendEntries,
    MessageType.APPEND_ENTRIES_RESPONSE: AppendEntriesResponse,
}


def decode_message(data: bytes) -> Message:
    """Decode a message from bytes.

    Args:
        data: Serialized message bytes

    Returns:
        Deserialized message instance

    Raises:
        KeyError: If message type is not recognized
        json.JSONDecodeError: If data is not valid JSON
    """
    obj = json.loads(data.decode("utf-8"))
    cls = MESSAGE_CLASSES[obj["type"]]
    return cls(**obj)
