-------------------------- MODULE ReplicationFlush --------------------------
(***************************************************************************)
(* The replication client, the shape log collector, two shape consumers,   *)
(* the flush boundary and the slot, with every fault on: the collector     *)
(* exits mid-transaction, a consumer exits mid-delivery, a shape is        *)
(* removed mid-transaction, the retry budget runs out, the connection     *)
(* restarts from the slot.                                                 *)
(*                                                                         *)
(* Two transactions of two fragments each: frames 1 and 2 are T1, frames  *)
(* 3 and 4 are T2. Frame 1 and 3 carry the begin, frames 2 and 4 the      *)
(* commit. Shape 1 has a change in frame 1 only of T1, shape 2 in frame 2 *)
(* only; both have changes in both frames of T2.                          *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS RouterShards, AckAfterJournal, PipelineWindow, Variants

Shapes == 1..2
Last == 4
Frames == 1..Last
Commits == {2, 4}
Txn(f) == (f + 1) \div 2
Begin(f) == (f % 2) = 1
Commit(f) == (f % 2) = 0
First(f) == (2 * Txn(f)) - 1
CommitThrough(f) == f - (f % 2)
Matches(s, f) == (f >= 3) \/ (s = f)
Max(a, b) == IF a > b THEN a ELSE b
Min(S) == CHOOSE n \in S : \A m \in S : n <= m
LastOf(xs) == IF Len(xs) = 0 THEN 0 ELSE xs[Len(xs)]
Values(xs) == {xs[i] : i \in 1..Len(xs)}
On(v) == v \in Variants

VARIABLES client, collector, shape, journal, slot, proof, failed
vars == <<client, collector, shape, journal, slot, proof, failed>>

(* decoded: frames read from the socket; sent: frames dispatched to the     *)
(* current collector; acked: frames that collector answered; status: the   *)
(* flush position the client reports to Postgres.                          *)
ClientAt(f) == [decoded |-> f, sent |-> f, acked |-> f, status |-> f, mode |-> "stream"]

(* base: frames at or below it were never routed by this collector. Frames *)
(* in (replayTo, skip] are never delivered to it: the client acked them to *)
(* the collector that exited and does not send them again.                 *)
CollectorAt(base, replayTo, skip) ==
    [up |-> TRUE, base |-> base, replayTo |-> replayTo, skip |-> skip,
     routed |-> base, done |-> base, published |-> base, boundary |-> base]

ShapeAt(f) ==
    [live |-> TRUE, registered |-> TRUE, writer |-> "running",
     handled |-> f, acked |-> f, step |-> "idle",
     pending |-> <<>>, log |-> <<>>, durable |-> <<>>,
     reported |-> 0, pins |-> {}]

Init ==
    /\ client = ClientAt(0)
    /\ collector = CollectorAt(0, 0, 0)
    /\ shape = [s \in Shapes |-> ShapeAt(0)]
    /\ journal = [synced |-> 0, reclaimed |-> 0]
    /\ slot = 0
    /\ proof = 0
    /\ failed = FALSE

Replaying == collector.done < collector.replayTo
InGap(f) == (f > collector.replayTo) /\ (f <= collector.skip)
Seen(f) == (f > collector.base) /\ ~InGap(f)
Window == IF (client.acked = 0) \/ (client.mode = "retry") THEN 1 ELSE PipelineWindow

(* shapes/fragment_assembler.ex assemble/3: a shape gets the commit of a   *)
(* transaction it has a change in, as a commit-only fragment, only when    *)
(* this assembler saw that change (shapes_in_txn).                         *)
Participates(s, f) ==
    \/ Matches(s, f)
    \/ /\ Commit(f) /\ Seen(First(f)) /\ Matches(s, First(f))
       /\ ~On("omitCommitOnly")

PendingFrame(s) == shape[s].handled + 1
Gathered(f) == \A s \in Shapes : shape[s].acked >= f
CrashedHeld(f) == \E s \in Shapes :
    Participates(s, f) /\ shape[s].writer = "crashed" /\ shape[s].live

\* postgres/replication_client.ex handle_data/2 -> convert_message/2, with journal_ingest/2:
\* in journal mode each event is appended and a commit is datasynced and reported at once.
Decode ==
    LET f == client.decoded + 1 IN
    /\ client.mode = "stream" /\ f <= Last
    /\ AckAfterJournal \/ (client.decoded - client.acked < Window)
    /\ client' = [client EXCEPT !.decoded = f,
                   !.status = IF AckAfterJournal /\ Commit(f) THEN Max(@, f) ELSE @]
    /\ journal' = IF AckAfterJournal /\ Commit(f)
                  THEN [journal EXCEPT !.synced = f] ELSE journal
    /\ UNCHANGED <<collector, shape, slot, proof, failed>>

\* postgres/replication_client.ex start_event/3 (handle_event_async), and the replay queue
\* collapse_in_flight/2 builds after a collector exit.
Dispatch ==
    LET f == client.sent + 1 IN
    /\ collector.up /\ client.mode \in {"stream", "retry"}
    /\ f <= client.decoded /\ (client.sent - client.acked < Window)
    /\ client' = [client EXCEPT !.sent = f]
    /\ UNCHANGED <<collector, shape, journal, slot, proof, failed>>

\* replication/shape_log_collector.ex handle_txn_fragment/3 -> publish/2 or
\* publish_scatter_ahead/3, and replay_event/2 for a journal frame. A frame at or below the
\* last processed offset never gets here (the drop clause): routed already passed it.
Route ==
    LET f == collector.routed + 1 IN
    /\ collector.up /\ ~InGap(f)
    /\ (f <= collector.replayTo) \/ (f <= client.sent)
    /\ (RouterShards = 2 /\ f > collector.replayTo) \/ (collector.routed = collector.done)
    /\ collector' = [collector EXCEPT !.routed = f]
    /\ UNCHANGED <<client, shape, journal, slot, proof, failed>>

\* The frames the client acked to the collector that exited: nothing routes them again.
SkipGap ==
    LET f == collector.routed + 1 IN
    /\ collector.up /\ InGap(f) /\ collector.routed = collector.done
    /\ \A s \in Shapes : shape[s].step = "idle" /\ shape[s].acked = collector.routed
    /\ collector' = [collector EXCEPT !.routed = collector.skip, !.done = collector.skip]
    /\ shape' = [s \in Shapes |->
         [shape[s] EXCEPT !.handled = collector.skip, !.acked = collector.skip]]
    /\ UNCHANGED <<client, journal, slot, proof, failed>>

\* shapes/router_worker.ex handle_cast/2, or ShapeLogCollector.route_and_assemble_own/2 with
\* fragment_assembler.ex assemble/3. The consumer call follows in Receive.
Offer(s) ==
    LET f == PendingFrame(s) IN
    /\ collector.up /\ f <= collector.routed /\ shape[s].step = "idle"
    /\ shape' = [shape EXCEPT ![s].step =
         IF shape[s].registered /\ Participates(s, f) THEN "receive" ELSE "reply"]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* shapes/consumer.ex handle_txn_fragment/2: skip what storage holds (latest_offset and the
\* straddle clause), else process_txn_fragment/2, which raises on a fragment without
\* has_begin? when no transaction is pending: the assembler marks has_begin? only on the
\* shape's first fragment after a begin it routed itself.
Receive(s) ==
    LET f == PendingFrame(s)
        old == \/ f <= LastOf(shape[s].log)
               \/ f <= LastOf(shape[s].pending)
               \/ (Len(shape[s].pending) = 0 /\ Txn(f) <= Txn(LastOf(shape[s].log)))
        orphan == ~Begin(f) /\ ~Seen(First(f)) /\ Len(shape[s].pending) = 0 /\ ~old
    IN
    /\ collector.up /\ shape[s].step = "receive"
    /\ shape' = [shape EXCEPT
         ![s].writer = IF orphan /\ @ = "running" THEN "crashed" ELSE @,
         ![s].step = CASE (shape[s].writer # "running") \/ orphan -> "reply"
                       [] old /\ ~On("noDedup") -> "reply"
                       [] Matches(s, f) -> "append"
                       [] OTHER -> "commit"]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* shapes/consumer.ex write_txn_fragment_to_storage/2 -> Storage.append_fragment_to_log!/2.
AppendFragment(s) ==
    LET f == PendingFrame(s) IN
    /\ collector.up /\ shape[s].step = "append" /\ shape[s].writer = "running"
    /\ shape' = [shape EXCEPT ![s].pending = Append(@, f),
                             ![s].step = IF Commit(f) THEN "commit" ELSE "reply"]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* shapes/consumer.ex maybe_complete_pending_txn/2 -> Storage.signal_txn_commit!/2.
CommitLog(s) ==
    /\ collector.up /\ shape[s].step = "commit" /\ shape[s].writer = "running"
    /\ shape' = [shape EXCEPT ![s].log = @ \o shape[s].pending,
                             ![s].pending = <<>>, ![s].step = "reply"]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* shapes/consumer_registry.ex publish/2 collects the consumer reply or its monitor DOWN.
DeliveryResult(s) ==
    /\ collector.up /\ shape[s].step = "reply"
    /\ shape' = [shape EXCEPT ![s].handled = @ + 1, ![s].step = "idle",
         ![s].acked = IF RouterShards = 1 THEN shape[s].handled + 1 ELSE @]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* replication/shape_log_collector.ex record_worker_ack/5. Worker messages are FIFO.
WorkerAck(s) ==
    /\ RouterShards = 2 /\ collector.up /\ shape[s].acked < shape[s].handled
    /\ shape' = [shape EXCEPT ![s].acked = @ + 1]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* replication/shape_log_collector.ex finalize_publish/6: handle_writer_down/3 invalidates a
\* crashed consumer's shape first, then LsnTracker.set_last_processed_lsn/2 at a commit.
PublishProof ==
    LET f == collector.done + 1 IN
    /\ collector.up /\ f <= collector.routed /\ collector.published < f /\ Commit(f)
    /\ Gathered(f) /\ ~CrashedHeld(f)
    /\ collector' = [collector EXCEPT !.published = f]
    /\ proof' = Max(proof, f)
    /\ UNCHANGED <<client, shape, journal, slot, failed>>

\* replication/shape_log_collector.ex finalize_completed_gathers/1, then
\* shape_log_collector/flush_tracker.ex handle_txn_fragment/4 pins the commit on each shape
\* it reached that has not reported a flush past it.
FinishGather ==
    LET f == collector.done + 1 IN
    /\ collector.up /\ f <= collector.routed /\ Gathered(f) /\ ~CrashedHeld(f)
    /\ (~Commit(f)) \/ (collector.published = f)
    /\ collector' = [collector EXCEPT !.done = f]
    /\ shape' = [s \in Shapes |->
         IF Commit(f) /\ shape[s].registered /\ Participates(s, f)
             /\ shape[s].writer # "crashed" /\ shape[s].reported < f
         THEN [shape[s] EXCEPT !.pins = @ \cup {f}]
         ELSE shape[s]]
    /\ UNCHANGED <<client, journal, slot, proof, failed>>

\* postgres/replication_client.ex handle_info({ref, :ok}, state).
ClientAck ==
    /\ client.acked < client.sent /\ client.acked < collector.done /\ collector.up
    /\ client' = [client EXCEPT !.acked = @ + 1, !.mode = "stream"]
    /\ UNCHANGED <<collector, shape, journal, slot, proof, failed>>

\* shape_cache/pure_file_storage.ex perform_scheduled_flush/2.
SyncLog(s) ==
    /\ shape[s].writer = "running" /\ shape[s].log # shape[s].durable
    /\ shape' = [shape EXCEPT ![s].durable = shape[s].log]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* replication/shape_log_collector.ex apply_or_buffer_flush_notification/4.
WriterFlush(s) ==
    LET f == 2 * Txn(LastOf(shape[s].durable)) IN
    /\ collector.up /\ shape[s].registered /\ shape[s].writer = "running"
    /\ f > shape[s].reported
    /\ shape' = [shape EXCEPT ![s].reported = f, ![s].pins = {p \in @ : p > f}]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* replication/shape_log_collector/flush_tracker.ex update_global_offset/1.
PinSet == UNION {shape[s].pins : s \in Shapes}
Boundary == IF PinSet = {} THEN CommitThrough(collector.done) ELSE Min(PinSet) - 2

\* postgres/replication_client.ex handle_info({:flush_boundary_updated, lsn}, state); in
\* journal mode the boundary drives only the journal's gc!/2.
ConfirmFlush ==
    /\ collector.up /\ Boundary > collector.boundary
    /\ collector' = [collector EXCEPT !.boundary = Boundary]
    /\ client' = IF AckAfterJournal THEN client
                 ELSE [client EXCEPT !.status = Max(@, Boundary)]
    /\ UNCHANGED <<shape, journal, slot, proof, failed>>

\* Control only: a primary keepalive that reports the last dispatched commit, without the
\* pending-commit fence of postgres/replication_client.ex handle_data/2.
UnfencedKeepalive ==
    /\ On("noFlushFence") /\ ~AckAfterJournal
    /\ CommitThrough(client.sent) > client.status
    /\ client' = [client EXCEPT !.status = CommitThrough(client.sent)]
    /\ UNCHANGED <<collector, shape, journal, slot, proof, failed>>

\* Postgres receives a standby status update; a lost one is resent by the next.
SlotAdvance ==
    /\ slot < client.status
    /\ slot' = client.status
    /\ UNCHANGED <<client, collector, shape, journal, proof, failed>>

\* replication/txn_journal.ex gc!/2 after the client hears of the consumer boundary.
ReclaimJournal ==
    LET f == Min({journal.synced, collector.boundary}) IN
    /\ AckAfterJournal /\ f > journal.reclaimed
    /\ journal' = [journal EXCEPT !.reclaimed = f]
    /\ UNCHANGED <<client, collector, shape, slot, proof, failed>>

\* A consumer exits while ConsumerRegistry.publish/2 waits on it. An exit whose reason may
\* be a teardown ("ambiguous") is held for the stall check instead of invalidated at once.
WriterExit(s, reason) ==
    /\ ~On("noWriterExit") /\ collector.up /\ shape[s].live /\ shape[s].writer = "running"
    /\ shape[s].step \in {"receive", "append", "commit"}
    /\ shape' = [shape EXCEPT ![s].writer = reason, ![s].step = "reply"]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* shape_cache/shape_cleaner.ex remove_shape_immediate/5 -> ShapeStatus.remove_shape/2, and
\* the invalidation of a crashed or stalled consumer's shape.
Detach(s) ==
    /\ shape[s].live
    /\ shape' = [shape EXCEPT ![s].live = FALSE]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* shape_cache/shape_cleaner.ex stop_consumer/4.
StopWriter(s) ==
    /\ ~shape[s].live /\ shape[s].writer # "stopped"
    /\ shape' = [shape EXCEPT ![s].writer = "stopped",
         ![s].step = IF @ \in {"receive", "append", "commit"} THEN "reply" ELSE @]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* replication/shape_log_collector.ex remove_subscription/2 -> FlushTracker.handle_shape_removed/2;
\* registration changes drain the pipeline first.
Unregister(s) ==
    /\ collector.up /\ shape[s].registered /\ shape[s].writer = "stopped"
    /\ collector.done = collector.routed
    /\ shape' = [shape EXCEPT ![s].registered = FALSE,
         ![s].pins = IF On("keepRemovedPin") THEN @ ELSE {}]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* replication/shape_log_collector.ex check_stalled_gather_head!/2 raises on a gather that
\* outlives gather_stall_timeout. One collector exit per behaviour, this or the next.
GatherTimeout ==
    /\ collector.up /\ ~failed /\ ~On("exitBetweenTxns")
    /\ RouterShards = 2 /\ collector.routed > collector.done
    /\ ~Gathered(collector.done + 1)
    /\ collector' = [collector EXCEPT !.up = FALSE]
    /\ failed' = TRUE
    /\ UNCHANGED <<client, shape, journal, slot, proof>>

\* Outside event: the collector exits (a raise while it handles an event, a kill).
CollectorExit ==
    /\ collector.up /\ ~failed
    /\ On("exitBetweenTxns") => (client.sent = client.acked /\ Commit(client.acked))
    /\ collector' = [collector EXCEPT !.up = FALSE]
    /\ failed' = TRUE
    /\ UNCHANGED <<client, shape, journal, slot, proof>>

\* postgres/replication_client.ex handle_info({:DOWN, ...}) -> collapse_in_flight/2, or a
\* dispatch that finds no collector: the head event is retried against the next collector
\* and later in-flight events queue behind it. Replies sent before the exit arrive first.
\* The client monitors the collector only for the calls it has in flight: with none, and
\* nothing to dispatch, it does not see the exit, and Restart may go first.
\* Fix candidate "replayFromSlot": drop the connection instead, so Postgres resends from the
\* slot, and start the next collector there (LsnTracker.request_replay_from_slot/1).
\* Fix candidate "restartFromSlot": the same, on every collector exit, and the next collector
\* takes no event before the reconnected client sends one.
ClientDown ==
    /\ ~collector.up /\ client.mode = "stream" /\ client.acked = collector.done
    /\ \/ client.sent > client.acked
       \/ client.decoded > client.sent
       \/ On("restartFromSlot")
    /\ client' = [client EXCEPT !.sent = client.acked,
         !.mode = IF On("replayFromSlot") \/ On("restartFromSlot")
                  THEN "disconnect" ELSE "retry"]
    /\ UNCHANGED <<collector, shape, journal, slot, proof, failed>>

\* postgres/replication_client.ex: @max_event_retry_time exhausted, exit(reason).
RetryExpired ==
    /\ ~collector.up /\ client.mode = "retry"
    /\ client' = [client EXCEPT !.mode = "disconnect"]
    /\ UNCHANGED <<collector, shape, journal, slot, proof, failed>>

\* shapes/consumer.ex terminate/2 as Shapes.Supervisor (one_for_all) stops its children:
\* the writer flushes what it wrote; an uncommitted transaction is discarded on reopen.
ShutdownWriter(s) ==
    /\ ~collector.up /\ shape[s].writer # "stopped"
    /\ shape' = [shape EXCEPT ![s].writer = "stopped", ![s].durable = shape[s].log,
         ![s].pending = <<>>, ![s].step = "idle"]
    /\ UNCHANGED <<client, collector, journal, slot, proof, failed>>

\* shapes/supervisor.ex (one_for_all) restarts the collector and the consumers, not its own
\* init/1: the replication client, under Connection.Manager, keeps its connection ("retry"),
\* or reconnects and Postgres resends from the slot ("disconnect"). The collector's drop
\* clause starts at the last processed LSN (mark_as_ready), which LsnTracker keeps across
\* both (initialize_last_processed_lsn/2 does not overwrite it); replay_journal/2 first
\* replays the synced journal generations. A client with nothing in flight keeps streaming
\* to the next collector ("stream"): it never saw the exit.
Restart ==
    LET full == client.mode = "disconnect"
        fromSlot == On("replayFromSlot") \/ On("restartFromSlot")
        dedup == IF full /\ fromSlot THEN slot ELSE proof
        journalData == AckAfterJournal /\ journal.synced > journal.reclaimed
        resume == IF full THEN dedup ELSE Max(dedup, client.acked)
        base == IF journalData THEN journal.reclaimed ELSE resume
        replayTo == IF journalData THEN journal.synced ELSE base
        skip == IF full THEN replayTo ELSE Max(replayTo, client.acked)
    IN
    /\ ~collector.up
    /\ \/ client.mode \in {"retry", "disconnect"}
       \/ client.mode = "stream" /\ client.sent = client.acked /\ ~On("restartFromSlot")
    /\ \A s \in Shapes : shape[s].writer = "stopped"
    /\ collector' = CollectorAt(base, replayTo, skip)
    /\ shape' = [s \in Shapes |->
         [ShapeAt(base) EXCEPT !.live = shape[s].live, !.registered = shape[s].live,
           !.writer = IF shape[s].live THEN "running" ELSE "stopped",
           !.log = shape[s].durable, !.durable = shape[s].durable]]
    /\ client' = IF full THEN ClientAt(slot) ELSE client
    /\ proof' = dedup
    /\ UNCHANGED <<journal, slot, failed>>

Next ==
    \/ Decode \/ Dispatch \/ Route \/ SkipGap
    \/ PublishProof \/ FinishGather \/ ClientAck \/ ConfirmFlush \/ SlotAdvance
    \/ UnfencedKeepalive \/ ReclaimJournal
    \/ GatherTimeout \/ CollectorExit \/ ClientDown \/ RetryExpired \/ Restart
    \/ \E s \in Shapes :
         \/ Offer(s) \/ Receive(s) \/ AppendFragment(s) \/ CommitLog(s)
         \/ DeliveryResult(s) \/ WorkerAck(s) \/ SyncLog(s) \/ WriterFlush(s)
         \/ Detach(s) \/ StopWriter(s) \/ Unregister(s) \/ ShutdownWriter(s)
         \/ \E reason \in {"crashed", "ambiguous"} : WriterExit(s, reason)

Spec == Init /\ [][Next]_vars

\* A shape still served that has handled a commit holds every change matching it up to that
\* commit: none is lost, and no transaction is applied in part.
Serving(s) == shape[s].live /\ shape[s].writer = "running"

NoLostChange ==
    \A s \in Shapes : Serving(s) =>
        \A f \in Frames :
            (Matches(s, f) /\ f <= CommitThrough(shape[s].handled)) => f \in Values(shape[s].log)

Expected(s, f) == {n \in 1..f : Matches(s, n)}

\* Postgres never resends what the slot passed: each change of a commit at or below it is
\* durable in every shape still served, or held by the journal.
RecoverableAck ==
    \A s \in Shapes : Serving(s) =>
        \A f \in Commits : f <= slot =>
            \/ (Expected(s, f) \ Expected(s, f - 2)) \subseteq Values(shape[s].durable)
            \/ (journal.reclaimed < f /\ f <= journal.synced)

\* A shape's log never holds a change twice or out of order.
IncreasingOffsets ==
    \A s \in Shapes :
        LET xs == shape[s].log \o shape[s].pending IN
        \A i \in 2..Len(xs) : xs[i - 1] < xs[i]

\* A removed shape holds no part of the flush boundary.
RemovedUnpinned ==
    \A s \in Shapes : ~shape[s].registered => shape[s].pins = {}
=============================================================================
