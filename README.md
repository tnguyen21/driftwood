driftwood (a poor man's broken Raft)

...

a single-threaded, toy implementation of the [raft consensus algorithm](https://raft.github.io/). written to better my understanding.

doing the core event loop in a single-thread allows us to eschew a lot of the complexity of managing locks and doing safe mutations on a single node. this let's us really focus on the core parts of raft's interaction _between_ nodes.

raft is complex enough! let's not add more ways we can get confused and introduce subtle bugs!

this isn't intended to be a performant or particularly complete implementation of raft. rather, it hopes to demonstrate enough of the core mechanics of the algorithm that it's correct under reasonable conditions and the less pathological error scenarios.

...

all of the core raft logic lives in node.py

messages.py just has type definitions

...

start a node:

```
# this is in a single process
# you should start nodes in separate processes and specify peer lists
python -m raft.cli \
        --id 0 \
        --udp-port 10000 \
        --peers '[["localhost", 10001], ["localhost", 10002]]' \
        --peer-ids '[1, 2]'
```

run tests:

`uv run pytest tests -v -s`
