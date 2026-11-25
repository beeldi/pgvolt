# TLA+ models

Each folder models one lifecycle of the sync service where several processes move shared state: the replication
client, the shape log collector and its router shards, the consumers and their storage, the flush boundary, the
replication slot. TLC tries every ordering of a small instance against rules taken from the module docs and comments
and from what the HTTP API promises a client: no change lost or delivered twice, offsets only grow, up-to-date means
caught up with the log, a removed shape holds nothing back.

A folder holds:

- the model, `<Name>.tla`, each action commented with the file and function it traces to;
- `runs/<run>.cfg`, one TLC config per run: the checks on the code and one failing control per rule. When a folder
  holds several models, each config starts with `\* model: <Name>.tla`;
- `README.md`: the code and commit checked, the rules, the verdict of each run, the bugs found, where the model was
  wrong, and what it leaves out;
- `statecharts.md`: the state machines in Mermaid, written by hand.

One action is one message handled by one process, one ETS or storage call, or one outside event: a process exit, a
restart, a timeout, a lost or late reply. Bounds stay small (two shapes, two transactions of two fragments) with
every fault on: a result from a run with a fault switched off is not the result. Every rule has a control, a
variant that removes the guard the rule relies on and must fail with its own counterexample; a rule no control can
break checks nothing. A counterexample on the code's model is replayed against the code as an ExUnit test: red
means a bug, green means the model allows something the code cannot do, and the model is fixed.

`fork/tla/run.sh <folder>` runs every config of a folder, `fork/tla/run.sh <folder> <run>...` only those named.
Each run goes through `fork/tla/tlc.sh`: TLC v1.7.4 (checked against its pinned sha256) in Docker, 6 GB of memory,
3 GB of heap, 2 GB of working files and 30 minutes (`TLC_MEMORY`, `TLC_HEAP`, `TLC_WORKING_FILES` and
`TLC_TIME_LIMIT` override them for one run), one summary line, and a `<run>.out` beside the config that git ignores.
A run that hits a limit prints `inconclusive` and stops `run.sh` there. Large runs need a machine with spare cores
and memory, not a laptop. A config holding the line `\* simulate: <traces> <depth> <seed>` runs TLC's random
simulation instead: that many behaviours of at most that many steps, from that seed, for bounds too large to check
every ordering. It samples, so its controls must fail under the same line too.

A change to a modelled lifecycle starts from its folder: rerun the configs on the old code, update the model and
its README with the change. The regression tests, not the model, prove each bug.

| folder               | lifecycle                                                              |
| -------------------- | ---------------------------------------------------------------------- |
| `replication-flush/` | replication acknowledgement, collector restarts and the flush boundary |
| `subquery-moves/`    | dependency views, move-query snapshots and the shape log               |
