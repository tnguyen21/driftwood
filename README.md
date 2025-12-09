driftwood (a poor man's broken Raft)

---

a single-threaded, toy implementation of the [raft consensus algorithm](https://raft.github.io/). written to better my understanding.

doing the core event loop in a single-thread allows us to eschew a lot of the complexity of managing locks and doing safe mutations on a single node. this let's us really focus on the core parts of raft's interaction _between_ nodes.

raft is complex enough! let's not add more ways we can get confused and introduce subtle bugs!

---

election.py demonstrates the leader election algorithm. running it produces the following logs:

```
Node 1: Starting election for term 1
node 0: recv from ('127.0.0.1', 10001): b'{"type": "request_vote", "term": 1, "candidate_id": 1, "last_log_index": 0, "last_log_term": 0}'
node 2: recv from ('127.0.0.1', 10001): b'{"type": "request_vote", "term": 1, "candidate_id": 1, "last_log_index": 0, "last_log_term": 0}'
node 1: recv from ('127.0.0.1', 10000): b'{"type": "vote_response", "term": 1, "vote_granted": true}'
node 1 became LEADER with 2 votes
node 1: recv from ('127.0.0.1', 10002): b'{"type": "vote_response", "term": 1, "vote_granted": true}'
node 0: recv from ('127.0.0.1', 10001): b'{"type": "heartbeat", "term": 1}'
node 2: recv from ('127.0.0.1', 10001): b'{"type": "heartbeat", "term": 1}'
node 0: recv from ('127.0.0.1', 10001): b'{"type": "heartbeat", "term": 1}'
node 2: recv from ('127.0.0.1', 10001): b'{"type": "heartbeat", "term": 1}'
```

---

raft.py implements what the paper refers to as "`AppendEntries` messages" -- the core part of the algorithm that lets us replicate data across nodes in a safe, consistent manner.

it include's some simple tests that starts up the node, sends in data, and confirms the replication and consistency of the log (even if a node fails).
