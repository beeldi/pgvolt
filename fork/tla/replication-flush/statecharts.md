# Replication flush statecharts

Written by hand from `ReplicationFlush.tla`. The red states are where `NoLostChange` breaks on the code.

## The replication client and the collector it dispatches to

```mermaid
stateDiagram-v2
    direction TB
    classDef mayBreak fill:#fde2e2,stroke:#c0392b,stroke-width:2px
    state "Streaming: fragments in flight to the collector" as Stream
    state "Collector gone, retry the head event in place" as Retry
    state "Reconnected: Postgres resends from the slot" as Disconnect
    state "Next collector takes the retried fragment" as Resumed
    state "Next collector takes the next fragment, the exit unseen" as Unseen
    [*] --> Stream
    Stream --> Stream: the collector answers a fragment
    Stream --> Retry: the collector exits with a call in flight (a DOWN), or a dispatch finds none
    Stream --> Unseen: the collector exits with no call in flight, and is started again first
    Retry --> Resumed: the shape supervisor starts a collector and consumers again
    Retry --> Disconnect: the retry budget runs out
    Retry --> Disconnect: fix candidate replayFromSlot
    Stream --> Disconnect: fix candidate restartFromSlot, on every collector exit
    Disconnect --> Stream: the next collector starts at the slot
    Resumed --> Stream
    Unseen --> Stream
    class Resumed mayBreak
    class Unseen mayBreak
```

## A transaction of two fragments across a collector exit

```mermaid
stateDiagram-v2
    direction TB
    classDef mayBreak fill:#fde2e2,stroke:#c0392b,stroke-width:2px
    state "First fragment routed: a shape holds the change uncommitted" as FirstRouted
    state "Collector exits before the commit fragment" as Exited
    state "Consumers stopped: the uncommitted change is discarded" as Discarded
    state "Commit fragment alone reaches the next collector" as CommitAlone
    state "Shape committed the transaction" as Committed
    state "Shape skipped the transaction: its change is lost" as Lost
    state "Shape had a change in the commit fragment: it raises, is invalidated" as Invalidated
    [*] --> FirstRouted
    FirstRouted --> Committed: the same collector routes the commit
    FirstRouted --> Exited
    Exited --> Discarded
    Discarded --> CommitAlone: the client retries only the fragment in flight
    Discarded --> CommitAlone: the client, which saw no exit, sends the commit to the next collector
    Discarded --> FirstRouted: fix candidate restartFromSlot, Postgres resends the transaction from its begin
    Discarded --> FirstRouted: journal mode, the commit was synced before the exit and the journal replays it
    CommitAlone --> Lost: the new assembler never saw the shape in this transaction
    CommitAlone --> Invalidated: a fragment without its begin and nothing pending
    Committed --> [*]
    Lost --> [*]
    Invalidated --> [*]
    class Lost mayBreak
```

## A shape's consumer

```mermaid
stateDiagram-v2
    direction TB
    classDef mayBreak fill:#fde2e2,stroke:#c0392b,stroke-width:2px
    state "Running: receives, appends, commits" as Running
    state "Crashed mid-delivery: invalidated at publish" as Crashed
    state "Exited as for a teardown (shutdown, killed, noproc): kept, flush pinned" as Held
    state "Removed: stopped, unregistered, pins dropped" as Removed
    state "Restored from storage by the restart" as Restored
    [*] --> Running
    Running --> Crashed: raises, or a fragment without its begin
    Running --> Held: exits with an ambiguous reason
    Held --> Removed: the stall check invalidates it after the grace period
    Held --> Restored: the collector exits first, the pin goes with it
    Crashed --> Removed
    Running --> Removed: the shape cleaner removes it
    Running --> Restored: the shape supervisor restarts in place
    Restored --> Running
    Removed --> [*]
    class Restored mayBreak
```
