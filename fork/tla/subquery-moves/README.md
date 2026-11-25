# Subquery moves

`SubqueryMoves.tla` checks one outer shape from Postgres commits through dependency materializers, the move queue,
and snapshot splicing to the shape log. The `head-*` baseline models the unpatched consumer before this fork's
subquery-move fixes. The `fixed-*` variants add the fixes modeled here: direct tag broadcast for unambiguous
exclusions, single-transaction pairing and the replacement scope guard in `Buffering.splice/1`, pairing a later
removal with a queued addition when no sibling dependency is queued, querying Postgres for the tags of moves on a
numeric key, splicing a buffered move against the removals queued behind it, and leaving out of a move-in's snapshot
both those drained removals' rows and rows taken out by a removal the snapshot already sees. The
pre-pairing baseline (`round4-lone-empty`) models the queue before cross-commit addition/removal pairing.

## Instance

Each fixture has one to three dependencies, up to two parent keys per dependency, up to two child rows, and up
to three commits. TLC explores all enabled orderings in each script. The query can take its snapshot before,
between, or after later commits. The snapshot notification, query result, and replication watermark reach the
consumer separately. WAL order is preserved and the collector publishes dependency layers before the outer
layer. A parent-only commit's root WAL action advances the model cursor without adding a root change.

- `text` and `ambiguous` combine a negated text-pair condition with an unchanged positive condition using
  AND. The first pair has the extra column marker. The second commit adds an exclusion and inserts two
  children, one excluded in that same transaction.
- `replacement` ORs three negated dependencies. A removal starts a query. The second commit removes keys
  from two dependencies and the third adds a key back to one of them.
- `and` and `or` exercise changes to both a positive and a negated dependency.
- `same-commit` replaces the only key of one negated dependency. A NULL child stays excluded while a second
  child enters. Its positive conjunct stays true.
- `switch-empty` replaces the sole key in two negated dependencies in one commit. Both children remain visible
  through the other disjunct. The pending sibling change makes the separate splice retire the handle.
- `lone-empty` has one negated dependency whose view goes `{1, 3}`, `{1}`, `{1, 2}`, `{2}`. The first removal
  starts a query, and an addition and a later removal queue behind it. The NULL child never matches.
- `sibling-late` adds a second negated dependency whose addition shares the first one's addition commit, so
  the later removal has a queued sibling. The NULL child is excluded through both dependencies.
- `scale` ORs two positive dependencies. Both children hold the first dependency's key, the second at another
  scale (`1.00` for `1.0`): Postgres finds it equal, but its tag hashes other text. The key enters, the second
  dependency then drops the second child's key, and the first key leaves.
- `scale-commit` keeps the first dependency of `scale` alone. One commit removes its key and inserts a child
  holding it, which Postgres never selects.
- `queued-out` ORs two positive dependencies. The first gains a key and starts a query. The second commit
  removes the second dependency's key and inserts a child holding it, and the third brings the key back.
  `queued-negated` adds the key to a negated view instead, and `queued-move-out` queues the same removal
  behind a move-out on a numeric key.
- `queued-self` has one dependency. The move brings in two keys, and the next commit takes one back and
  inserts a child holding it. In `queued-key` the next commit removes a key the move does not bring in and
  inserts a child holding the moved one. In `queued-back` the key enters and leaves with no child written.
- `queued-and` ANDs two positive dependencies. The first gains a key, and the next commit removes the
  second's key and inserts a child holding both. `queued-or` ORs them instead, so the child stays through
  the moved key.
- `queued-polarity` ANDs a negated dependency, the closed divisions, with a positive one, the active firms.
  A firm becomes active and starts a query. The next commit deactivates the firm of every child, and the third
  reopens the first child's division and inserts a second child. Postgres never selects a child.
- `replace-commit` has one negated dependency. One commit replaces its only key with another and inserts a
  child holding the entering key, which Postgres never selects, while the child holding the leaving key enters.

Paths in the action comments are under `packages/sync-service/lib/electric/shapes` unless they name Postgres,
the replication collector, or a client package.

## Rules

The HTTP protocol describes `up-to-date` as reaching the end of the data the server can serve when it answers.
The model uses the commit represented by a drained shape log as that boundary.
[HTTP control messages](https://github.com/electric-sql/electric/blob/3f9809fb0bd894cb74470d434749673492ad7acf/website/docs/sync/api/http.md#control-messages)

| Rule              | Check                                                                                                          |
| ----------------- | -------------------------------------------------------------------------------------------------------------- |
| `BoundaryRows`    | At each modeled `up-to-date` the client's keys equal the SQL result for the represented commit.                |
| `NoPhantomInsert` | An inserted key occurs in a committed SQL result from the preceding boundary through message receipt.          |
| `NoLateInsert`    | An inserted key occurs in a committed SQL result from the last commit the client read through message receipt. |
| `NoPhantomDelete` | A deleted key is absent from at least one committed SQL result in the same interval.                           |

A change message reflects the commit that wrote it, and a snapshot row the commit its snapshot sees last.
`NoLateInsert` is stricter than `NoPhantomInsert`: a row Postgres selected only before a commit the client already
read never arrives after it.

`ShapeStream` delivers JSON response batches in [`client.ts`](../../../packages/typescript-client/src/client.ts#L1960-L1978).
SSE waits for `up-to-date` before delivery in [`client.ts`](../../../packages/typescript-client/src/client.ts#L2056-L2080).
`Shape` applies changes in order and notifies after processing the batch in
[`shape.ts`](../../../packages/typescript-client/src/shape.ts#L209-L282). Its stock materialization applies
row messages. The move controls in this model follow the tag-aware Elixir client in
[`tag_tracker.ex`](../../../packages/elixir-client/lib/electric/client/tag_tracker.ex#L138) and
[`poll.ex`](../../../packages/elixir-client/lib/electric/client/poll.ex). The message rules check operations a
tag-aware client receives even when a subscriber that sees only the final batch state would miss a transient
row.

## Runs

Checked with `fork/tla/run.sh subquery-moves`, TLC v1.7.4 with three workers. Counts for counterexamples can
vary with worker scheduling.

| Run                     | Rules                  | Result   | Distinct states | Control or counterexample                                                                                     |
| ----------------------- | ---------------------- | -------- | --------------: | ------------------------------------------------------------------------------------------------------------- |
| `head-text`             | `NoPhantomInsert`      | Violated |             109 | Inserts the child excluded by its own commit.                                                                 |
| `head-scale`            | `NoPhantomDelete`      | Violated |             376 | Deletes the scaled child its first dependency still selects.                                                  |
| `head-ambiguous`        | `NoPhantomInsert`      | Violated |             112 | The same insert with a genuine alternate split.                                                               |
| `head-replacement`      | `NoPhantomDelete`      | Violated |           2,387 | Deletes a child whose other negated disjunct should already be active.                                        |
| `head-switch-empty`     | `NoPhantomDelete`      | Violated |              51 | Splices one view before the other view of the same commit.                                                    |
| `head-queued-out`       | `NoPhantomInsert`      | Violated |             337 | Sends the child inserted in the removal's commit.                                                             |
| `head-queued-self`      | `NoPhantomInsert`      | Violated |             111 | Sends the child inserted with the key taken back.                                                             |
| `head-queued-and`       | `NoPhantomInsert`      | Violated |             115 | Sends the child inserted as the other key leaves.                                                             |
| `head-replace-commit`   | `NoPhantomInsert`      | Violated |              16 | Inserts the child holding the entering key.                                                                   |
| `round4-lone-empty`     | `NoPhantomInsert`      | Violated |           1,151 | Drains a later removal alone. The empty view shows the NULL child.                                            |
| `fixed-text`            | All four               | Holds    |              62 | See `ctl-insert`.                                                                                             |
| `fixed-ambiguous`       | All four               | Holds    |              17 | See `ctl-ambiguous`.                                                                                          |
| `fixed-replacement`     | All four               | Holds    |           3,621 | See `ctl-delete`.                                                                                             |
| `fixed-and`             | All four               | Holds    |             233 | See `ctl-boundary`.                                                                                           |
| `fixed-or`              | All four               | Holds    |             233 | Positive and negative disjuncts.                                                                              |
| `fixed-same-commit`     | All four               | Holds    |              19 | See `ctl-split-replacement`.                                                                                  |
| `fixed-switch-empty`    | All four               | Holds    |              37 | See `ctl-sibling-splice`.                                                                                     |
| `fixed-lone-empty`      | All four               | Holds    |           1,277 | See `round4-lone-empty`.                                                                                      |
| `fixed-sibling-late`    | All four               | Holds    |           1,382 | See `ctl-late-reset`.                                                                                         |
| `fixed-scale`           | All but `NoLateInsert` | Holds    |           1,288 | See `ctl-scale-in` and `ctl-scale-out`.                                                                       |
| `fixed-scale-commit`    | All four               | Holds    |              19 | See `ctl-scale-commit`.                                                                                       |
| `fixed-replace-commit`  | All four               | Holds    |              19 | See `ctl-replace-commit`.                                                                                     |
| `fixed-queued-out`      | All four               | Holds    |             951 | See `ctl-queued-out`, `ctl-queued-per-txn` and `ctl-queued-pre-write`.                                        |
| `fixed-queued-negated`  | All four               | Holds    |             951 | See `ctl-queued-negated`.                                                                                     |
| `fixed-queued-move-out` | All four               | Holds    |             132 | See `ctl-queued-move-out`.                                                                                    |
| `fixed-queued-self`     | All four               | Holds    |             140 | See `ctl-queued-snapshot`.                                                                                    |
| `fixed-queued-and`      | All four               | Holds    |             140 | See `ctl-queued-and`.                                                                                         |
| `fixed-queued-key`      | All four               | Holds    |             140 | A removed key the move does not bring in.                                                                     |
| `fixed-queued-back`     | All four               | Holds    |             141 | See `ctl-queued-seen`.                                                                                        |
| `fixed-queued-or`       | All four               | Holds    |             141 | See `ctl-queued-or`.                                                                                          |
| `ctl-insert`            | `NoPhantomInsert`      | Violated |             111 | Restores SQL fallback for a marker without a field namespace.                                                 |
| `ctl-ambiguous`         | `NoPhantomInsert`      | Violated |             111 | Restores SQL fallback for a genuine alternate split.                                                          |
| `ctl-delete`            | `NoPhantomDelete`      | Violated |           2,638 | Allows cross-commit replacement pairing.                                                                      |
| `ctl-boundary`          | `BoundaryRows`         | Violated |             160 | Drops move-in snapshot rows. The AND shape reports caught up with a row missing.                              |
| `ctl-split-replacement` | `NoPhantomInsert`      | Violated |              18 | Splits a same-commit replacement. The temporary empty view admits the NULL child.                             |
| `ctl-sibling-splice`    | `NoPhantomDelete`      | Violated |              52 | Splices two replaced views of one commit separately.                                                          |
| `ctl-late-reset`        | `NoPhantomInsert`      | Violated |           1,525 | Drains a later emptying removal alone beside a queued sibling.                                                |
| `ctl-scale-in`          | `NoPhantomDelete`      | Violated |             478 | Hashes the moved text for a move-in beside another disjunct.                                                  |
| `ctl-scale-out`         | `BoundaryRows`         | Violated |             634 | Hashes the moved text for a move-out. The scaled child stays.                                                 |
| `ctl-scale-commit`      | `NoPhantomInsert`      | Violated |              16 | Reads a move-out's earlier commit against the views before it.                                                |
| `ctl-replace-commit`    | `NoPhantomInsert`      | Violated |              16 | Reads a replacement's earlier commit against the views before it.                                             |
| `ctl-queued-out`        | `NoPhantomInsert`      | Violated |             304 | Writes the buffered commits without the queued removal.                                                       |
| `ctl-queued-negated`    | `NoPhantomInsert`      | Violated |             290 | The same with the key added to a negated view.                                                                |
| `ctl-queued-move-out`   | `NoPhantomInsert`      | Violated |             111 | The same behind a move-out.                                                                                   |
| `ctl-queued-per-txn`    | `BoundaryRows`         | Violated |             703 | Takes each commit's own queued moves. The netted queue loses a child.                                         |
| `ctl-queued-pre-write`  | `NoPhantomInsert`      | Violated |             345 | Writes a move-in's earlier commit against the views before it, removal ignored.                               |
| `ctl-queued-snapshot`   | `NoPhantomInsert`      | Violated |             141 | Sends every snapshot row. The removal takes the child's key back.                                             |
| `ctl-queued-and`        | `NoPhantomInsert`      | Violated |             141 | The same with the conjunction's other key.                                                                    |
| `ctl-queued-or`         | `BoundaryRows`         | Violated |             135 | Writes the earlier commit with the removal applied. A kept child is lost.                                     |
| `ctl-queued-seen`       | `NoLateInsert`         | Violated |             141 | Sends a snapshot row a drained removal the snapshot sees took out, then deletes it.                           |
| `open-scale-late`       | `NoLateInsert`         | Violated |             961 | Sends a snapshot row a numeric move-out the snapshot sees took out. The drain starts that move-out.           |
| `open-queued-polarity`  | `NoPhantomInsert`      | Violated |             748 | Pops the reopened division before the firm's earlier removal. The client gets a child Postgres never selects. |
| `alt-queued-polarity`   | All four               | Holds    |           1,162 | Pops first the moves that take rows out of the shape.                                                         |
| `alt-replacement`       | `NoPhantomDelete`      | Violated |           1,688 | The same order applies a later removal before an earlier addition.                                            |

The earlier controls (`round4-lone-empty`, `ctl-late-reset`, `ctl-insert`, `ctl-ambiguous`,
`ctl-sibling-splice` and `ctl-split-replacement`) remove a guard that keeps a move from buffering or from
splicing alone. The drained removals also cover what they break in these scripts, so they run on the splice
before queued-removal draining was added, which their guards were written for. That does not make the guards redundant beyond these
scripts, and they stay.

## Replays and fixes

Checked against Postgres and the streamed client in
[`subquery_move_out_test.exs`](../../../packages/sync-service/test/integration/subquery_move_out_test.exs).

- A text exclusion opens a snapshot while the next commit adds a second exclusion and inserts two children.
  The first child is absent from every SQL result, but the old splice emits its insert and delete around the
  visible child's insert. The fix broadcasts unambiguous tags directly. A genuine alternate split triggers
  `must-refetch` and the new snapshot contains only the visible child. The existing tag collision replay
  checks that the retained child still appears after a handle change.
- Combining the second commit's removal with the third commit's addition deactivates a retained child's
  disjunct before another dependency's removal activates its replacement. The client sees delete then
  insert for a child present at every commit. The fix pairs only a fresh two-direction payload that
  establishes one transaction. Both snapshot pause locations reproduce the original failure.
- One transaction replaces the sole key in each of two independent negated views. Each child stays visible
  in Postgres. The old code splices one view first and churns a retained child. The fixed consumer detects
  shared transaction provenance on the queued dependency before publishing the splice. The new handle
  snapshots both retained children.
- An addition and a later commit's removal queue behind a move query on one negated view that is never
  empty. The pre-pairing queue (`round4-lone-empty`) drains the removal alone, and the client sees an insert and a delete for the NULL
  child. The fix pairs the halves across commits when no other dependency has a queued move. With a queued
  sibling, a removal that reached a queued addition and would empty the view retires the handle, and the new
  snapshot has only the visible child.
- A numeric key enters a shape whose other disjunct holds a child keyed at another scale. The old move-in
  control hashes the moved text, so the client never activates that child's condition, and the other
  disjunct's move-out deletes a child Postgres still selects. A move-out of the key leaves the child on the
  client the same way. The fix queries the tags of every move-out on a numeric key, and of a move-in beside
  another disjunct, in
  [`subquery_numeric_key_test.exs`](../../../packages/sync-service/test/integration/subquery_numeric_key_test.exs),
  which also checks a negated key and `numeric[]` keys.
- That move-out buffers while Postgres names its tags, and its splice read the commits before its snapshot
  against the views before the move. A child inserted in the move-out's own commit reached the client, and the
  move-out's patterns deleted it right after. A negated dependency's queried move-out did the same. Those
  commits now write their rows against the views after the move, in the same file's same-transaction cases.
  A later change to a row one of them wrote reads the row it replaces against the views after the move too,
  so a child whose insert was dropped reaches the client as an insert once Postgres selects it, never as an
  update or a delete of a row it does not hold. Outside the model, which omits root updates, the same tests
  and the consumer's handler test check this case.
- A move-in buffers while a firm becomes active, and one transaction then deactivates a division and adds an
  employee to it. The old splice read the employee against the division view before the removal, sent it,
  and the drained move-out deleted it right after. The fix drains the queue before the splice and writes the
  buffered commits against the drained removals. When that transaction commits before the move's snapshot,
  the move's query leaves to it the rows the views before the move select, so it writes a row the drained
  removals keep against those views, and another against them with the removals applied. With the two
  subqueries ANDed, the employee is in the firm query's snapshot instead: the snapshot leaves out a row a
  commit before it wrote that the removals' move-out controls would take back, which it reads from the
  row's active conditions, so under `OR` a row another disjunct selects stays. Any other row of the snapshot
  is as the query read it, which Postgres selected unless a removal the snapshot sees took it out, the
  move's own transaction's among them: those rows stay out too. A parent that moves in and out again with no
  child written sends its rows, then their delete, only when the move-out commits after the snapshot, and the
  router's oscillation test checks both outcomes. When the removal committed after the move but before the
  query, the snapshot previously sent that row: the client got it after the commit that took it out, and the
  drain's move-out deleted it right after. `NoLateInsert` catches it (`ctl-queued-seen`), and the
  integration test pauses the query before and after its snapshot. A removal in the move's own transaction is
  outside the fixtures; the integration tests check it.
- A negated replacement's splice read the commits before its snapshot against the views before the move,
  where the entering key is not yet excluded. A child inserted with that key in the replacement's own commit
  reached the client, and the replacement's move-out deleted it right after; a child moved to that key reached
  it as an update. Those commits now write their rows against the views with the entering keys added, which
  still leave the leaving key's children to the snapshot; the consumer's handler test checks this.
- When a buffered move-out's commits wrote two rows under one key (identical rows of a table without a primary
  key or two rows of a deferred one), the second change was read against the views before the move and sent
  the client a delete of a row it never held. The rows they wrote now count by key and content. The model has
  no keys shared by two rows; the consumer's handler test checks it.
- A move query that completed with an ambiguous exclusion queued behind it crashed the consumer in the
  splice. The crash still removed the shape and the client still refetched, so the model's rules cannot tell
  it from the fix. The integration test checks the consumer's exit reason instead.

## Model corrections and limits

The first encoding kept the query's idle marker as a string and its active state as a record. TLC could not
compare them, so the idle marker now has the same record fields. The splice was initially schedulable after
another consumer message even when already ready. It now runs before another materializer, WAL or watermark
message can change its inputs. The first pairing candidate compared transaction ID sets by direction. The
checked fix tracks a fresh two-direction payload instead, since attribution in the queue is an accumulated
set and nested materializers can contribute multiple upstream transactions. A payload that carries both
directions, other than a fresh one from one transaction, counts as a late removal, since the queue cannot
order the two halves inside it. The first `scale` encoding sent the second dependency's moves through a query
too, though only the first dependency's key is numeric and the service applies its moves directly.

The merge of the replacement fix with the queued removals first wrote a move-in's earlier commit row by row
against the views after the drained removals, even with no removal queued. A replacement's row the entering
keys exclude then went against those views with the removals applied, which add the entering keys whatever
`PreView/1` gives, and `ctl-replace-commit` held. `pre_snapshot_refs/3` writes every row against the views its
snapshot leaves them to when no drained removal changes the views after the move, and `PreWrites` now does
too, so the control fails again.

The key scripts cover a finite set of transaction contents. The model omits root updates and deletes, duplicate
RHS rows, arbitrary NULL combinations, locale and type casting other than a numeric's scale, nested
materializers, crashes, and storage or network failures. The `scale` fixture has only positive dependencies:
a negated one reaches the same query, which the integration tests check. It groups each append with the consumer handler that publishes it and abstracts
`must-refetch` plus the replacement initial snapshot as one client transition. It checks the shape log at
drained commit boundaries. HTTP chunking, empty live responses and the relationship between the global WAL
watermark and an active shape buffer are outside the model. TLC checks safety here, and bounded exploration
gives no liveness or unbounded proof.

The drained removals apply to every buffered commit, including one that committed before the removal: a row
Postgres selected only between the two never reaches the client, which the rules allow. The drain applies only
the removals it does not fold into the move it starts, a replacement or a move-out on a numeric key: that
move's splice writes against the views before it, which still hold their rows. A row written with such a
removal still reaches the client and that move deletes it right after, as every queued removal did before
queued-removal draining was added; no script reaches it. A first version applied those removals in this splice too, and the next
move's control then deleted rows the client never got. The model's commits only insert rows, so it could not show
that, and the restart lanes of
[`oracle_property_test.exs`](../../../packages/sync-service/test/integration/oracle_property_test.exs) found it.

`NoPhantomInsert` accepts a row Postgres selected at any commit since the boundary, including one before a commit
the client already read, so it could not tell a snapshot row sent after the removal that took it out.
`NoLateInsert` reads the commit of each message. It holds on every fixed script but `scale`, where the snapshot
sends a row that a move-out on a numeric key took out before the query: the drain starts that move-out instead
of applying it, so the splice keeps its rows, and the move-out deletes the row right after (`open-scale-late`).
Leaving those rows out of the snapshot would make that move-out's splice read them as held.

The queue pops a removal before an addition, the lowest dependency first, whatever its polarity. In
`queued-polarity` the negated view's removal lets a child in before the positive view's removal that committed
first takes its firm out, and the client gets a child Postgres never selects (`open-queued-polarity`). Popping
first every move that takes rows out of the shape, a positive view's removal or a negated view's addition,
fixes that script (`alt-queued-polarity`) but breaks `replacement`: a later commit's removal then goes before an
earlier commit's addition, and the client deletes a child Postgres keeps (`alt-replacement`). Neither order
keeps both: that takes the commits' order, which the net queue does not keep, so the window stays open.

The fix keeps ordinary queue coalescing. Independent dependencies can still be processed in different handler
calls. The sibling guard sees net pending moves and the attribution they retained. It does not reconstruct
intermediate commits or a sibling delta canceled by coalescing. A transaction-ordered move queue with complete
dependency barriers would be needed to extend the guarantee beyond these scripts.
