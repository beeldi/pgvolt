# Replication flush

`ReplicationFlush.tla` models what happens to a transaction between the replication socket and the shape logs, and
what the service reports back to Postgres, when the shape log collector or a consumer exits in the middle.

Checked against this fork's replication and shape-log collector implementation. Each action names the function it traces to:

- `lib/electric/postgres/replication_client.ex`: decoding, the dispatch pipeline, the retry after a collector exit,
  the flush position reported to Postgres, the journal in ack-after-journal mode;
- `lib/electric/replication/shape_log_collector.ex` and `shape_log_collector/flush_tracker.ex`: routing, gathers,
  the processed LSN, flush pins and the flush boundary, removals, the gather stall check;
- `lib/electric/shapes/fragment_assembler.ex`, `shapes/router_worker.ex`, `shapes/consumer_registry.ex`: what each
  shape gets of a transaction split into fragments;
- `lib/electric/shapes/consumer.ex` and `shape_cache/pure_file_storage.ex`: appends, commits, flushes, a consumer's
  exit;
- `lib/electric/shapes/supervisor.ex`, `lib/electric/lsn_tracker.ex`: the in-place restart of the collector and the
  consumers, and where the next collector starts.

## Instance

Two shapes and two transactions of two fragments each. Frames 1 and 2 are the first transaction, 3 and 4 the second;
odd frames carry the begin, even ones the commit. Shape 1 has a change in frame 1 only, shape 2 in frame 2 only, and
both have changes in both frames of the second transaction. The constants vary the router (`RouterShards` 1 or 2),
the journal (`AckAfterJournal`) and the dispatch window (`PipelineWindow` 1 or 2). Every fault is on: the collector
exits (once per behaviour, by a raise, a kill or the gather stall check), a consumer exits mid-delivery with a crash
or with a reason a teardown also gives (`:shutdown`, `:killed`, `:noproc`), a shape is removed mid-transaction, the
retry budget runs out, a consumer's unflushed writes are lost at shutdown.

## Rules

| rule                | what a client is promised                                                                                 |
| ------------------- | --------------------------------------------------------------------------------------------------------- |
| `NoLostChange`      | a shape still served that has handled a commit holds every change of its own up to that commit            |
| `RecoverableAck`    | the slot never passes a commit whose changes are neither durable in every served shape nor in the journal |
| `IncreasingOffsets` | a shape's log never holds a change twice or out of order                                                  |
| `RemovedUnpinned`   | a removed shape holds no part of the flush boundary                                                       |

## Variants

`Variants` switches parts of the model:

- controls, one per rule, each removing the guard the rule relies on: `omitCommitOnly` (the assembler sends no
  commit-only fragment), `noFlushFence` (a keepalive reports the last dispatched commit), `noDedup` (a consumer
  appends what storage already holds), `keepRemovedPin` (a removal leaves its flush pins);
- fault limiters, which only shorten a counterexample: `noWriterExit` (no consumer exits), `exitBetweenTxns` (the
  collector exits only with nothing in flight, after a commit);
- fix candidates: `replayFromSlot` (when the client sees a collector exit it reconnects, Postgres resends from the
  slot, and the next collector starts there), `restartFromSlot` (the same on every collector exit, and the next
  collector takes no event before the reconnected client sends one).

## Runs

A run's name gives its constants: `r` the router shards, `j1` with the journal (`AckAfterJournal`), `w` the dispatch
window. The `bug-`, `patch-` and `ctl-` runs use one shard, no journal and a window of 1, except `bug-midtxn-j1` (the
journal) and `ctl-noFlushFence` (a window of 2). TLC v1.7.4, breadth first, on a Linux host.

| run                  | variants                            | rules                                  | result                 | distinct states |
| -------------------- | ----------------------------------- | -------------------------------------- | ---------------------- | --------------: |
| `bug-midtxn-j0`      | `noWriterExit`                      | `NoLostChange`                         | violated               |           3,506 |
| `bug-midtxn-j1`      | `noWriterExit`                      | `NoLostChange`                         | violated               |          37,371 |
| `bug-held-exit`      | `exitBetweenTxns`                   | `NoLostChange`                         | violated               |           6,765 |
| `code-lost-r1-j0-w1` | none                                | `NoLostChange`                         | violated               |           5,102 |
| `code-ack-r1-j0-w1`  | none                                | `RecoverableAck`                       | violated               |          19,217 |
| `code-r1-j0-w1`      | none                                | `IncreasingOffsets`, `RemovedUnpinned` | holds                  |       1,203,792 |
| `code-r1-j0-w2`      | none                                | `IncreasingOffsets`, `RemovedUnpinned` | holds                  |       2,285,718 |
| `code-r1-j1-w1`      | none                                | `IncreasingOffsets`, `RemovedUnpinned` | inconclusive at 30 min |      40,028,955 |
| `code-r1-j1-w2`      | none                                | `IncreasingOffsets`, `RemovedUnpinned` | inconclusive at 30 min |      41,516,040 |
| `code-r2-j0-w1`      | none                                | `IncreasingOffsets`, `RemovedUnpinned` | holds                  |       1,867,234 |
| `code-r2-j0-w2`      | none                                | `IncreasingOffsets`, `RemovedUnpinned` | holds                  |       9,622,247 |
| `code-r2-j1-w1`      | none                                | `IncreasingOffsets`, `RemovedUnpinned` | inconclusive at 30 min |      38,451,993 |
| `code-r2-j1-w2`      | none                                | `IncreasingOffsets`, `RemovedUnpinned` | inconclusive at 30 min |      41,460,492 |
| `patch-midtxn`       | `replayFromSlot`, `noWriterExit`    | `NoLostChange`                         | violated               |           2,713 |
| `patch-held-exit`    | `replayFromSlot`, `exitBetweenTxns` | `NoLostChange`                         | violated               |           6,345 |
| `fix-r1-j0-w1`       | `restartFromSlot`                   | all four                               | holds                  |         866,832 |
| `fix-r1-j0-w2`       | `restartFromSlot`                   | all four                               | holds                  |       1,805,074 |
| `fix-r1-j1-w1`       | `restartFromSlot`                   | all four                               | holds                  |      21,237,120 |
| `fix-r1-j1-w2`       | `restartFromSlot`                   | all four                               | holds                  |      26,523,943 |
| `fix-r2-j0-w1`       | `restartFromSlot`                   | all four                               | holds                  |       1,318,727 |
| `fix-r2-j0-w2`       | `restartFromSlot`                   | all four                               | holds                  |       7,365,994 |
| `fix-r2-j1-w1`       | `restartFromSlot`                   | all four                               | holds                  |      34,026,416 |
| `fix-r2-j1-w2`       | `restartFromSlot`                   | all four                               | inconclusive at 30 min |      37,590,972 |
| `ctl-omitCommitOnly` | `restartFromSlot`, `omitCommitOnly` | `NoLostChange`                         | violated (control)     |           2,221 |
| `ctl-noFlushFence`   | `restartFromSlot`, `noFlushFence`   | `RecoverableAck`                       | violated (control)     |           1,934 |
| `ctl-noDedup`        | `restartFromSlot`, `noDedup`        | `IncreasingOffsets`                    | violated (control)     |          18,936 |
| `ctl-keepRemovedPin` | `restartFromSlot`, `keepRemovedPin` | `RemovedUnpinned`                      | violated (control)     |           8,692 |

The `code-` runs check only the two rules the code keeps: `code-lost-` and `code-ack-` show it breaks the two others.
An inconclusive run stopped at the 30-minute limit with no violation in the states it had reached.

## Bugs

Both bugs are fixed in this tree via the fail-stop restart described below. Before the fix, an ExUnit replay of each path against real Postgres fails with a shape request answering 200 up-to-date without the matching row; the replays live in `packages/sync-service/test/integration/replication_fragment_restart_test.exs`.

### A collector exit mid-transaction loses the change of the earlier fragments

1. A transaction is decoded in two fragments. Shape 1 has a change in the first one only.
2. The collector routes the first fragment; shape 1's consumer appends the change, uncommitted.
3. The collector exits before it routes the commit fragment. The shape supervisor stops the consumers, which drop the
   uncommitted change, and starts a collector and the consumers again in place; the replication connection stays up.
4. The client sends the commit fragment to the next collector: retried because it was in flight, or dispatched for
   the first time because the client had no call in flight and never saw the exit. Its fragment assembler never saw
   the begin, so shape 1 is not in the transaction and gets no commit.
5. The processed LSN and then the slot pass the transaction. Shape 1 serves its log without the change and answers
   up-to-date; Postgres will not resend it.

Runs: `bug-midtxn-j0`, `bug-midtxn-j1` (`NoLostChange`), `code-lost-r1-j0-w1`, `code-ack-r1-j0-w1`
(`RecoverableAck`: the slot passes the lost change). Ack-after-journal mode does not prevent it: the journal replays
a transaction only once its commit is decoded.

### A consumer held for the stall check, then a collector exit, loses the transaction

1. A consumer exits while the collector waits on it, with a reason a teardown also gives. The collector holds the
   shape for the stall check, which rules after a grace period, keeps a flush pin for it, and moves the processed LSN
   past the transaction.
2. The collector exits before the stall check rules, between transactions. The pin goes with it.
3. The restart restores the shape from storage and the next collector starts at the processed LSN, past the
   transaction the shape never wrote. The shape is served without it.

Run: `bug-held-exit` (`NoLostChange`).

### Fix

`replayFromSlot` is the first fix tried: the client reconnects when it sees the exit. It fails (`patch-midtxn`,
`patch-held-exit`): the client monitors the collector only for the calls it has in flight, so an exit between calls
goes unseen, and the next collector starts at the processed LSN. Its code, run against the replays, makes the
in-flight one green and leaves the two others red, as the model says. `restartFromSlot` holds every rule on every
instance TLC finished (`fix-*`; `fix-r2-j1-w2` stopped at the time limit with no violation): every collector exit
sends the client back to the slot, and the next collector takes no event before the reconnected client's first. The code takes it as a fail-stop: `Shapes.Supervisor` allows no restart
(`max_restarts: 0`), so the first exit of the collector or of another process of the shape subsystem stops the
service, and the next start connects a new replication client that reads from the slot.

## Where the model was wrong

- The first draft let a restarted collector route frames the client had acknowledged to the collector before and does
  not send again. `SkipGap` and the `base`, `replayTo` and `skip` of `CollectorAt` fixed it.
- A first rule, that a commit is published only once every shape has it, failed on transient states after a replay
  from the slot that the code also goes through and recovers from. `NoLostChange`, per shape, replaced it.
- A first `ClientDown` let the client see every collector exit before the restart. The code sees one only with a call
  in flight or a dispatch that finds no collector. Found when the held-consumer replay stayed red with the
  `replayFromSlot` code applied; with it, `bug-midtxn-j1` held over 7,260,490 states and every `replayFromSlot` run
  held.

## What it leaves out

- Bounds: two shapes, two transactions of two fragments, one collector exit per behaviour.
- Liveness: deadlocks are not checked and no action is fair. `Detach` stands for every invalidation, the stall
  check's included, and may happen at any step, but nothing forces it: the model never says that a held shape is
  invalidated in the end, or that a gather finishes.
- A DOWN the client reads after the next collector is ready: `ClientDown` runs before `Restart`.
- Subqueries and the materializer, snapshots, a shape added mid-stream, the journal's buffer limit, storage
  corruption.
