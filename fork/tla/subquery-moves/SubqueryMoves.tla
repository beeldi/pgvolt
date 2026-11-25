---------------------------- MODULE SubqueryMoves ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Fixture, Variant

Rows == {1, 2}
Deps == CASE Fixture = "replacement" -> 1..3
          [] Fixture \in {"lone-empty", "scale-commit", "replace-commit", "queued-self",
                          "queued-key", "queued-back"}
             -> 1..1
          [] OTHER -> 1..2
Length == CASE Fixture \in {"replacement", "lone-empty", "sibling-late", "scale", "queued-out",
                          "queued-negated", "queued-polarity"} -> 3
            [] Fixture \in {"same-commit", "switch-empty", "scale-commit", "replace-commit"} -> 1
            [] OTHER -> 2
LateRemoval == Fixture \in {"lone-empty", "sibling-late"}
Empty == [d \in Deps |-> {}]
NoMove == [dep |-> 0, kind |-> "", before |-> Empty, afterMove |-> Empty,
           snap |-> 0, phase |-> "idle"]
Reset == [NoMove EXCEPT !.phase = "reset"]
TextPair == Fixture \in {"text", "ambiguous"}
\* `scale`: Postgres finds row 2's key for dependency 1 equal to the moved key, but
\* writes it at another scale (1.00 for 1.0), so its tag hashes other text.
\* `scale-commit` has that dependency alone and inserts row 1 with the key in the
\* commit that removes it.
\* `replace-commit`: one commit replaces the negated dependency's only key 1
\* with 2 and inserts row 1 with key 2, which Postgres never selects, while row 2,
\* with key 1, enters the shape.
Scaled == Fixture \in {"scale", "scale-commit"}
ScaledRow(r, d) == Scaled /\ d = 1 /\ r = 2
\* The `queued` fixtures queue a removal behind a buffered move. A commit that
\* removes a key inserts a row holding it, which Postgres never selects. In
\* `queued-out` the key comes back in the next commit, and `queued-negated` does
\* the same with the key added to a negated view. In `queued-move-out` the
\* buffered move is a move-out on a numeric key. In `queued-self` the removal
\* takes back a key the move brings in, in `queued-key` another key of the
\* moved dependency, and in `queued-and` a key of the other dependency of
\* a conjunction. `queued-back` takes back the moved key with no row written.
\* In `queued-or` the removal takes a key of the other dependency of a
\* disjunction, and its commit inserts a row holding that key and the moved one.
\* In `queued-polarity` a negated dependency's removal (a closed division
\* reopening) queues behind a positive dependency's removal (a firm leaving)
\* that committed first, and the reopening commit inserts a row of that firm.
Queued == Fixture \in {"queued-out", "queued-negated", "queued-self", "queued-move-out",
                       "queued-key", "queued-and", "queued-back", "queued-or", "queued-polarity"}
Numeric == Scaled \/ Fixture = "queued-move-out"
RootKey(r, d) ==
    CASE Fixture = "replacement" -> IF d = 1 THEN 2 ELSE IF d = 2 THEN 1 ELSE 9
      [] TextPair -> IF d = 1 THEN r ELSE 9
      [] Fixture = "same-commit" /\ d = 1 -> IF r = 1 THEN 0 ELSE 1
      [] Fixture = "switch-empty" -> IF r = d THEN 2 ELSE 1
      [] LateRemoval -> IF d = 1 THEN (IF r = 1 THEN 0 ELSE 5) ELSE 7
      [] Scaled -> IF d = 1 THEN 1 ELSE r
      [] Fixture = "queued-or" -> IF d = 1 THEN 1 ELSE r
      [] Fixture = "queued-polarity" -> IF d = 1 THEN r ELSE 1
      [] Fixture = "replace-commit" -> IF r = 1 THEN 2 ELSE 1
      [] OTHER -> r
Negated(d) ==
    /\ ~Scaled
    /\ IF Queued THEN (Fixture = "queued-negated" /\ d = 2) \/ (Fixture = "queued-polarity" /\ d = 1)
       ELSE Fixture \in {"replacement", "switch-empty", "sibling-late", "replace-commit"} \/ d = 1
Conjunctive == TextPair \/ Fixture \in {"and", "same-commit", "queued-and", "queued-polarity"}
Matches(r, d, views) ==
    LET key == RootKey(r, d)
    IN IF key = 0 THEN Negated(d) /\ views[d] = {}
       ELSE (key \in views[d]) /= Negated(d)
Includes(r, tags) == IF Conjunctive THEN tags = Deps ELSE tags /= {}
Tags(r, views) == {d \in Deps : Matches(r, d, views)}
Visible(rows, views) == {r \in rows : Includes(r, Tags(r, views))}

RootsAt(t) ==
    CASE Fixture = "replacement" -> {1}
      [] TextPair -> IF t < 2 THEN {} ELSE Rows
      [] Fixture \in {"scale-commit", "replace-commit"} -> IF t = 0 THEN {2} ELSE Rows
      [] Fixture \in {"queued-self", "queued-key"} -> IF t < 2 THEN {2} ELSE Rows
      [] Fixture = "queued-polarity" -> IF t < 3 THEN {1} ELSE Rows
      [] Fixture = "queued-back" -> Rows
      [] Queued -> IF t < 2 THEN {1} ELSE Rows
      [] OTHER -> Rows

ViewsAt(t) ==
    CASE Fixture = "replacement" ->
           <<IF t < 2 THEN {1} ELSE IF t = 2 THEN {} ELSE {2},
             IF t < 2 THEN {1, 9} ELSE {9},
             IF t = 0 THEN {1, 9} ELSE {9}>>
      [] TextPair ->
           <<IF t = 0 THEN {} ELSE IF t = 1 THEN {3} ELSE {1, 3}, {9}>>
      [] Fixture = "and" ->
           <<IF t < 2 THEN {2} ELSE {}, IF t = 0 THEN {} ELSE {1, 2}>>
      [] Fixture = "same-commit" ->
           <<IF t = 0 THEN {1} ELSE {2}, {1, 2}>>
      [] Fixture = "switch-empty" ->
           [d \in Deps |-> IF t = 0 THEN {1} ELSE {2}]
      [] LateRemoval ->
           LET first == CASE t = 0 -> {1, 3} [] t = 1 -> {1} [] t = 2 -> {1, 2} [] OTHER -> {2}
           IN IF Fixture = "lone-empty" THEN <<first>>
              ELSE <<first, IF t < 2 THEN {7} ELSE {7, 8}>>
      [] Fixture = "scale" ->
           <<IF t \in {1, 2} THEN {1} ELSE {}, IF t < 2 THEN {2} ELSE {}>>
      [] Fixture = "scale-commit" ->
           <<IF t = 0 THEN {1} ELSE {}>>
      [] Fixture = "queued-out" ->
           <<IF t = 0 THEN {} ELSE {1}, IF t = 2 THEN {} ELSE {2}>>
      [] Fixture = "queued-negated" ->
           <<IF t = 0 THEN {} ELSE {1}, IF t = 2 THEN {1, 2} ELSE {1}>>
      [] Fixture = "queued-self" ->
           <<CASE t = 0 -> {} [] t = 1 -> {1, 2} [] OTHER -> {2}>>
      [] Fixture = "queued-move-out" ->
           <<IF t = 0 THEN {1} ELSE {}, IF t < 2 THEN {2} ELSE {}>>
      [] Fixture = "queued-key" ->
           <<CASE t = 0 -> {2} [] t = 1 -> {1, 2} [] OTHER -> {1}>>
      [] Fixture = "queued-and" ->
           <<IF t = 0 THEN {} ELSE {2}, IF t < 2 THEN {2} ELSE {}>>
      [] Fixture = "queued-back" ->
           <<IF t = 1 THEN {1} ELSE {}>>
      [] Fixture = "queued-or" ->
           <<IF t = 0 THEN {} ELSE {1}, IF t < 2 THEN {2} ELSE {}>>
      [] Fixture = "queued-polarity" ->
           <<IF t < 3 THEN {1} ELSE {}, CASE t = 0 -> {1} [] t = 1 -> {1, 2} [] OTHER -> {2}>>
      [] Fixture = "replace-commit" ->
           <<IF t = 0 THEN {1} ELSE {2}>>
      [] OTHER ->
           <<IF t = 0 THEN {1} ELSE {}, IF t < 2 THEN {} ELSE {1, 2}>>

Pg(t) == Visible(RootsAt(t), ViewsAt(t))
Replace(v, d, keys) == [v EXCEPT ![d] = keys]

VARIABLES db, routed, handled, seen, views, pending, origins, late, active, buffer,
          log, client, fence, clientAt, badInsert, badLate, badDelete, wrongBoundary
vars == <<db, routed, handled, seen, views, pending, origins, late, active, buffer,
          log, client, fence, clientAt, badInsert, badLate, badDelete, wrongBoundary>>

Init ==
    /\ db = 0
    /\ routed = 0
    /\ seen = 0
    /\ handled = {}
    /\ views = ViewsAt(0)
    /\ pending = views
    /\ origins = {}
    /\ late = {}
    /\ active = NoMove
    /\ buffer = <<>>
    /\ log = <<>>
    /\ client = [r \in Pg(0) |-> Tags(r, views)]
    /\ fence = 0
    /\ clientAt = 0
    /\ badLate = FALSE
    /\ badInsert = FALSE
    /\ badDelete = FALSE
    /\ wrongBoundary = FALSE

Out(q, v, d) == v[d] \ q[d]
In(q, v, d) == q[d] \ v[d]
Min(ds) == CHOOSE d \in ds : \A other \in ds : d <= other
\* consumer/subqueries/move_queue.ex: enqueue/4, pop_next/1, pair_with_move_in/2.
\* The head queue pairs net removals and additions. The fixed variant pairs
\* the two directions of a fresh payload from one transaction, or of any
\* payloads when no other dependency has a queued move. `round4` is the queue
\* before that second case. A removal queued after an addition that would
\* empty the view beside a queued sibling retires the handle
\* (event_handler/subqueries/steady.ex: drain_queue/3).
\* These controls remove a guard that keeps a move from buffering or from splicing
\* alone. The drained removals also cover what they break, so the controls run on
\* the splice before that change, which their guards were written for.
Earlier == Variant \in {"round4", "noLateReset", "queryFalseCollision", "queryAmbiguous",
                        "spliceSibling", "splitSameCommit"}
\* The queue pops every view's removals first, whatever its polarity, so a
\* negated view's removal lets rows in before another view's removal that
\* committed first takes them out (`queued-polarity`). `removalsFirst` pops the
\* moves that take rows out of the shape first, a positive view losing keys or a
\* negated one gaining them: a later commit's removal then goes before an
\* earlier commit's addition, and the client loses a row Postgres keeps
\* (`replacement`). Only the commits' order would keep both.
OldOrder == Variant /= "removalsFirst"
OuterOut(q, v, d) == IF Negated(d) THEN In(q, v, d) ELSE Out(q, v, d)
GetMove(q, v, ids, lateIds) ==
    LET removing == IF OldOrder THEN {d \in Deps : Out(q, v, d) /= {}}
                    ELSE {d \in Deps : OuterOut(q, v, d) /= {}}
        adding == {d \in Deps : q[d] /= v[d]}
        d == Min(IF removing /= {} THEN removing ELSE adding)
        out == Out(q, v, d)
        in == In(q, v, d)
        popOut == IF OldOrder THEN out /= {}
                  ELSE IF d \in removing THEN ~Negated(d) ELSE Negated(d)
        quiet == \A o \in Deps \ {d} : q[o] = v[o]
        both == Negated(d) /\ out /= {} /\ in /= {} /\ v[d] \ out = {}
        pair == both /\ Variant /= "splitSameCommit"
                /\ (Variant \in {"head", "pairAcrossCommits"} \/ d \in ids
                    \/ (Variant /= "round4" /\ quiet))
        lateReset == both /\ ~pair /\ popOut /\ d \in lateIds
                     /\ Variant \notin {"head", "round4", "noLateReset"}
        delta == IF pair THEN q[d] ELSE IF popOut THEN v[d] \ out ELSE v[d] \cup in
        kind == IF pair THEN "replace" ELSE
                  IF popOut = Negated(d) THEN "in" ELSE "out"
    IN IF lateReset THEN Reset
       ELSE [dep |-> d, kind |-> kind, before |-> v, afterMove |-> Replace(v, d, delta),
             snap |-> 0, phase |-> "starting"]

\* consumer/subqueries/move_broadcast.ex: scaled_tags?/3. Only a query names
\* the tag of a row whose key Postgres writes at another scale. A move-in queries
\* them only beside another disjunct: its snapshot sends the rows the client lacks.
\* Only dependency 1's key is numeric.
ScaleQuery(m) ==
    /\ Numeric
    /\ m.dep = 1
    /\ IF m.kind = "in"
       THEN Cardinality(Deps) > 1 /\ Variant \notin {"head", "directScaleIn"}
       ELSE Variant \notin {"head", "directScaleOut"}

\* The rows whose tag the moved key's own text reaches (move_broadcast.ex: make/6).
DirectRows(m) == {r \in Rows : ~ScaledRow(r, m.dep)}

\* consumer/subqueries/move_broadcast.ex: query_patterns?/5. 3 is the first
\* text pair. The scripts differ in whether its legacy input is ambiguous.
NeedsQuery(m) ==
    m.kind /= "out" \/ ScaleQuery(m) \/
        (TextPair /\ m.dep = 1 /\ 3 \in m.afterMove[1] \ m.before[1]
         /\ Variant \in {"head", "queryFalseCollision", "queryAmbiguous"})

\* consumer/event_handler/subqueries/steady.ex: drain_queue/3.
\* Every direct move can be applied in the same handler call. A Reset drops
\* the call's appends, as the error discards its effects.
RECURSIVE Drain(_, _, _, _)
Drain(q, v, ids, lateIds) ==
    IF q = v THEN [view |-> v, move |-> NoMove, append |-> <<>>] ELSE
    LET m == GetMove(q, v, ids, lateIds)
    IN IF m = Reset THEN [view |-> v, move |-> Reset, append |-> <<>>]
       ELSE IF NeedsQuery(m)
        THEN [view |-> v, move |-> m, append |-> <<>>]
        ELSE LET tail == Drain(q, m.afterMove, ids, lateIds)
             IN IF tail.move = Reset THEN tail
                ELSE [view |-> tail.view, move |-> tail.move,
                      append |-> <<[kind |-> "tags", rows |-> DirectRows(m), move |-> m, at |-> 0]>> \o tail.append]

\* An outside event in Postgres. Only commits contribute to the SQL oracle.
Commit ==
    /\ db < Length
    /\ db' = db + 1
    /\ UNCHANGED <<routed, handled, seen, views, pending, origins, late, active, buffer,
                   log, client, fence, clientAt, badInsert, badLate, badDelete, wrongBoundary>>

\* consumer/materializer.ex: maybe_flush_pending_events/2 sends to the subscriber;
\* consumer/event_handler/subqueries/{steady,buffering}.ex: handle_event/2.
\* shape_log_collector.ex: publish/2 waits for the dependencies' layer before root WAL.
DepsChanged(t) == {d \in Deps : ViewsAt(t)[d] /= ViewsAt(t - 1)[d]}
ClearUsed(q, ids, view, move) ==
    LET next == IF move = NoMove THEN view ELSE move.afterMove
    IN {d \in ids : Out(q, next, d) /= {} /\ In(q, next, d) /= {}}

Ready ==
    /\ active /= NoMove
    /\ active.phase = "complete"
    /\ (seen >= active.snap \/ routed > active.snap)

Materializer(d) ==
    /\ routed < db
    /\ active /= Reset
    /\ ~Ready
    /\ d \in DepsChanged(routed + 1) \ handled
    /\ LET t == routed + 1
           nextQ == Replace(pending, d, ViewsAt(t)[d])
           before == IF active = NoMove THEN views ELSE active.afterMove
           nextIds == IF pending[d] = before[d] /\
                         Out(nextQ, before, d) /= {} /\ In(nextQ, before, d) /= {}
                      THEN origins \cup {d} ELSE origins \ {d}
           \* move_queue.ex: enqueue/4 marks a removal that reaches a queued
           \* addition, or that its payload carries with an addition.
           removes == pending[d] \ ViewsAt(t)[d] /= {}
           adds == ViewsAt(t)[d] \ pending[d] /= {}
           nextLate == IF Out(nextQ, before, d) = {} \/ In(nextQ, before, d) = {} \/ d \in nextIds
                       THEN late \ {d}
                       ELSE IF removes /\ (In(pending, before, d) /= {} \/ adds)
                       THEN late \cup {d}
                       ELSE late
       IN /\ pending' = nextQ
          /\ handled' = handled \cup {d}
          /\ IF Fixture = "ambiguous" /\ t = 1 /\
                Variant \notin {"head", "queryAmbiguous"}
             THEN /\ active' = Reset
                  /\ origins' = nextIds
                  /\ late' = nextLate
                  /\ UNCHANGED <<views, log>>
             ELSE IF active = NoMove
             THEN LET next == Drain(nextQ, views, nextIds, nextLate)
                  IN /\ views' = next.view
                     /\ active' = next.move
                     /\ log' = log \o next.append
                     /\ origins' = ClearUsed(nextQ, nextIds, next.view, next.move)
                     /\ late' = ClearUsed(nextQ, nextLate, next.view, next.move)
             ELSE /\ origins' = nextIds
                  /\ late' = nextLate
                  /\ UNCHANGED <<views, active, log>>
    /\ UNCHANGED <<db, routed, seen, buffer, client, fence, clientAt, badInsert, badLate, badDelete, wrongBoundary>>

Wal(t, v) ==
    [kind |-> "wal", rows |-> RootsAt(t) \ RootsAt(t - 1), view |-> v, at |-> t]

\* subqueries/splice_plan.ex: pre_snapshot_refs/3. A move-out's snapshot only
\* names tags, so a commit before it writes its rows against the views after the
\* move. A replacement's commit before its snapshot writes them against the
\* views with the keys entering the negated dependency added, which still leave
\* the snapshot's rows to it. The model's commits only insert rows, so only that
\* side shows.
PreView(m) ==
    CASE m.kind = "out" /\ Variant \notin {"head", "preSnapshotBefore"} -> m.afterMove
      [] m.kind = "replace" /\ Variant \notin {"head", "preReplaceBefore"} ->
           [m.before EXCEPT ![m.dep] = @ \cup m.afterMove[m.dep]]
      [] OTHER -> m.before

\* event_handler/subqueries/buffering.ex: splice/1 drains the queue before it
\* builds the splice. The views after the moves the drain applies, before the
\* move it starts: that move's splice applies its own removals.
DrainTarget(next) == next.view

\* subqueries/splice_plan.ex: build/4. A buffered commit writes its rows against
\* the views with the drained removals applied: a positive view loses the keys
\* they remove, a negated view gains the keys they add. Additions wait for
\* their own query. `perTxnExclusion` takes each commit's own views instead.
Shrink(v, base, target) ==
    [d \in Deps |-> IF Negated(d) THEN v[d] \cup (target[d] \ base[d])
                    ELSE v[d] \ (base[d] \ target[d])]
WriteView(v, t, target) ==
    CASE Earlier \/ Variant \in {"head", "noQueuedExclusion"} -> v
      [] Variant = "perTxnExclusion" -> Shrink(v, active.afterMove, ViewsAt(t))
      [] OTHER -> Shrink(v, active.afterMove, target)
\* splice_plan.ex: pre_snapshot_refs/3. A move-in's commits before its snapshot
\* write a row against the views its snapshot leaves the row to (querying.ex:
\* move_in_where_clause/5), PreView/1's, while the views after the drained
\* removals keep it, and against those views with the removals applied
\* otherwise: Postgres had dropped it, or drops it within the splice. When no
\* drained removal changes the views after the move, every row goes against
\* PreView/1's views.
\* `preBefore` writes every row against PreView/1's views, `preDropped` every
\* row against them with the removals applied.
PreWrites(t, target) ==
    LET rows == RootsAt(t) \ RootsAt(t - 1)
        written == WriteView(active.afterMove, t, target)
        base == PreView(active)
        dropped == Shrink(base, base, written)
        kept == {r \in rows : r \in Visible({r}, written)}
    IN CASE active.kind = "out" -> <<Wal(t, WriteView(base, t, target))>>
         [] Earlier \/ Variant \in {"head", "noQueuedExclusion", "preBefore"}
              \/ written = active.afterMove ->
              <<Wal(t, base)>>
         [] Variant = "preDropped" -> <<Wal(t, dropped)>>
         [] OTHER -> <<[kind |-> "wal", rows |-> kept, view |-> base, at |-> t],
                      [kind |-> "wal", rows |-> rows \ kept, view |-> dropped, at |-> t]>>

RECURSIVE PreLog(_, _)
PreLog(ts, target) ==
    IF ts = <<>> THEN <<>> ELSE PreWrites(Head(ts), target) \o PreLog(Tail(ts), target)

\* splice_plan.ex: skip_rows/5 and effects.ex: skip_row_fun/1. The snapshot
\* leaves out a row a commit before it wrote that the drained removals take out,
\* as a client applying their move-out controls finds. It also leaves out another
\* row that a drained removal its snapshot sees takes out: Postgres had dropped
\* the row when the query read it, although the query binds the views without
\* that removal (splice_plan.ex: seen_by_snapshot?/2). The model reads a removal
\* as seen when the view at the snapshot's commit lacks it. `ownTxnSnapshot` and
\* the splices before it leave out only the rows of a removal in the move's own
\* commit, which no fixture has: they send such a row, and the drain's move-out
\* deletes it after. `noSnapshotSkip` sends every row.
SeenRemovals(target) ==
    [d \in Deps |->
        IF Negated(d)
        THEN active.afterMove[d] \cup ((target[d] \ active.afterMove[d]) \cap ViewsAt(active.snap)[d])
        ELSE active.afterMove[d] \ ((active.afterMove[d] \ target[d]) \ ViewsAt(active.snap)[d])]
OwnTxnSkip == Earlier \/ Variant \in {"head", "ownTxnSnapshot", "noQueuedExclusion"}
SnapshotRows(entering, pos, target) ==
    LET written == UNION {RootsAt(pos[i]) \ RootsAt(pos[i] - 1) : i \in DOMAIN pos}
        writtenRows == Visible(entering \cap written, WriteView(active.afterMove, active.snap, target))
    IN CASE Variant = "omitSnapshot" -> {}
         [] Variant = "noSnapshotSkip" -> entering
         [] OwnTxnSkip -> (entering \ written) \cup writtenRows
         [] OTHER -> Visible(entering \ written, SeenRemovals(target)) \cup writtenRows

\* consumer.ex: handle_txn/2; steady.handle_event/2 converts the transaction,
\* buffering.handle_event/2 instead records it in ActiveMove.buffer_txn/2.
DeliverTxn ==
    /\ routed < db
    /\ active /= Reset
    /\ ~Ready
    /\ handled = DepsChanged(routed + 1)
    /\ routed' = routed + 1
    /\ handled' = {}
    /\ IF active = NoMove
       THEN /\ log' = Append(log, Wal(routed', views))
            /\ UNCHANGED buffer
       ELSE /\ buffer' = Append(buffer, routed')
            /\ UNCHANGED log
    /\ UNCHANGED <<db, seen, views, pending, origins, late, active, client, fence, clientAt,
                   badInsert, badLate, badDelete, wrongBoundary>>

\* postgres/snapshot_query.ex: execute_for_shape/4 obtains the snapshot.
\* Arbitrary commits before Capture and between Capture and Complete represent
\* pausing before and after the repeatable-read snapshot.
Capture ==
    /\ active /= NoMove
    /\ active.phase = "starting"
    /\ active' = [active EXCEPT !.snap = db, !.phase = "captured"]
    /\ UNCHANGED <<db, routed, handled, seen, views, pending, origins, late, buffer,
                   log, client, fence, clientAt, badInsert, badLate, badDelete, wrongBoundary>>

\* consumer.ex and buffering.ex: handle_info/2 and handle_event/2 for snapshot known.
SnapshotKnown ==
    /\ active /= NoMove
    /\ active.phase = "captured"
    /\ active' = [active EXCEPT !.phase = "known"]
    /\ UNCHANGED <<db, routed, handled, seen, views, pending, origins, late, buffer,
                   log, client, fence, clientAt, badInsert, badLate, badDelete, wrongBoundary>>

\* consumer/effects.ex: query_move_in_async/4 sends completion after running
\* Querying.move_patterns/9 and/or Querying.query_move_in/6.
Complete ==
    /\ active /= NoMove
    /\ active.phase = "known"
    /\ active' = [active EXCEPT !.phase = "complete"]
    /\ UNCHANGED <<db, routed, handled, seen, views, pending, origins, late, buffer,
                   log, client, fence, clientAt, badInsert, badLate, badDelete, wrongBoundary>>

\* buffering.handle_event/2 for global_last_seen_lsn (a watermark after WAL).
Seen ==
    /\ seen < routed
    /\ ~Ready
    /\ seen' = routed
    /\ UNCHANGED <<db, routed, handled, views, pending, origins, late, active, buffer,
                   log, client, fence, clientAt, badInsert, badLate, badDelete, wrongBoundary>>

\* buffering.splice/1 and subqueries/splice_plan.ex: build/2.
\* Separate actions for readiness and its effects have no other consumer
\* event interposed in the model; Postgres can still commit independently.
Splice ==
    /\ Ready
    /\ IF Fixture = "switch-empty" /\ active.kind = "replace" /\
          (\E d \in Deps \ {active.dep} : pending[d] /= active.afterMove[d]) /\
          Variant \notin {"head", "spliceSibling"}
       THEN /\ active' = Reset
            /\ UNCHANGED <<views, origins, late, log>>
       ELSE
          LET pos == SelectSeq(buffer, LAMBDA t : t <= active.snap)
              later == SelectSeq(buffer, LAMBDA t : t > active.snap)
              snapRoots == RootsAt(active.snap)
              entering == Visible(snapRoots, active.afterMove) \ Visible(snapRoots, active.before)
              \* Querying.move_patterns/9 reads the rows at the snapshot.
              tagRows == IF (NeedsQuery(active) /\ active.kind = "out") \/ ScaleQuery(active)
                         THEN snapRoots ELSE DirectRows(active)
              next == Drain(pending, active.afterMove, origins, late)
          IN IF next.move = Reset
             \* A queued move the drain cannot apply discards the splice.
             THEN /\ active' = Reset
                  /\ UNCHANGED <<views, origins, late, log>>
             ELSE
             /\ log' = log \o PreLog(pos, DrainTarget(next))
                           \o <<[kind |-> "tags", rows |-> tagRows, move |-> active, at |-> 0]>>
                           \o <<[kind |-> "wal",
                                  rows |-> SnapshotRows(entering, pos, DrainTarget(next)),
                                  view |-> active.afterMove, at |-> active.snap]>>
                           \o [i \in 1..Len(later) |->
                                  Wal(later[i], WriteView(active.afterMove, later[i], DrainTarget(next)))]
                           \o next.append
             /\ views' = next.view
             /\ active' = next.move
             /\ origins' = ClearUsed(pending, origins, next.view, next.move)
             /\ late' = ClearUsed(pending, late, next.view, next.move)
    /\ buffer' = <<>>
    /\ UNCHANGED <<db, routed, seen, handled, pending, client, fence, clientAt,
                   badInsert, badLate, badDelete, wrongBoundary>>

Apply(e) ==
    IF e.kind = "wal" THEN
        [r \in DOMAIN client \cup Visible(e.rows, e.view) |->
            IF r \in Visible(e.rows, e.view) THEN Tags(r, e.view) ELSE client[r]]
    ELSE
        LET m == e.move
            updated(r) ==
                IF r \notin e.rows \/ Matches(r, m.dep, m.before) = Matches(r, m.dep, m.afterMove)
                THEN client[r]
                ELSE IF Matches(r, m.dep, m.afterMove) THEN client[r] \cup {m.dep}
                     ELSE client[r] \ {m.dep}
        IN [r \in {r \in DOMAIN client : Includes(r, updated(r))} |-> updated(r)]

\* The wire has tags and patterns. elixir-client/lib/electric/client/tag_tracker.ex
\* applies the controls, synthesizing deletes; client/shape.ts applies row messages.
\* A message that reflects commit `at` moves the client past the commits before
\* it. `NoLateInsert`: a row the client then inserts is one Postgres selects at
\* that commit or a later one, not only at some commit since the fence.
ReadMessage ==
    /\ Len(log) > 0
    /\ LET e == Head(log)
           new == Apply(e)
           from == IF clientAt > fence THEN clientAt ELSE fence
           possible == UNION {Pg(t) : t \in fence..db}
           current == UNION {Pg(t) : t \in from..db}
           always == {r \in Rows : \A t \in fence..db : r \in Pg(t)}
       IN /\ client' = new
          /\ clientAt' = IF e.at > clientAt THEN e.at ELSE clientAt
          /\ badInsert' = (badInsert \/ ((DOMAIN new \ DOMAIN client) \ possible /= {}))
          /\ badLate' = (badLate \/ ((DOMAIN new \ DOMAIN client) \ current /= {}))
          /\ badDelete' = (badDelete \/ ((DOMAIN client \ DOMAIN new) \cap always /= {}))
    /\ log' = Tail(log)
    /\ UNCHANGED <<db, routed, seen, handled, views, pending, origins, late, active, buffer,
                   fence, wrongBoundary>>

\* shapes/api.ex: determine_up_to_date/1 and maybe_up_to_date/2. Compare a
\* drained publication with the commit represented by its shape log, not db.
UpToDate ==
    /\ active = NoMove
    /\ handled = {}
    /\ Len(log) = 0
    /\ routed > fence
    /\ fence' = routed
    /\ wrongBoundary' = (wrongBoundary \/ (DOMAIN client /= Pg(routed)))
    /\ UNCHANGED <<db, routed, seen, handled, views, pending, origins, late, active, buffer,
                   log, client, clientAt, badInsert, badLate, badDelete>>

\* consumer.ex: handle_event_error/2 removes the old shape. api.ex serves
\* must-refetch for its handle; the fresh generation starts at a new snapshot.
Refetch ==
    /\ active = Reset
    /\ active' = NoMove
    /\ routed' = db
    /\ seen' = db
    /\ handled' = {}
    /\ pending' = ViewsAt(db)
    /\ views' = pending'
    /\ origins' = {}
    /\ late' = {}
    /\ client' = [r \in Pg(db) |-> Tags(r, views')]
    /\ fence' = db
    /\ clientAt' = db
    /\ buffer' = <<>>
    /\ log' = <<>>
    /\ UNCHANGED <<db, badInsert, badLate, badDelete, wrongBoundary>>

Next == Commit \/ (\E d \in Deps : Materializer(d)) \/ DeliverTxn \/ Capture \/
        SnapshotKnown \/ Complete \/ Seen \/ Splice \/ ReadMessage \/ UpToDate \/ Refetch

BoundaryRows == ~wrongBoundary
NoPhantomInsert == ~badInsert
NoLateInsert == ~badLate
NoPhantomDelete == ~badDelete
=============================================================================
