defmodule Electric.Shapes.Consumer.EventHandler.SubqueriesTest do
  use ExUnit.Case, async: true

  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.Transaction
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Consumer.EventHandler
  alias Electric.Shapes.Consumer.EventHandler.Subqueries.Buffering
  alias Electric.Shapes.Consumer.EventHandler.Subqueries.Steady
  alias Electric.Shapes.Consumer.Subqueries.ActiveMove
  alias Electric.Shapes.Consumer.Subqueries.RefResolver
  alias Electric.Shapes.Consumer.Subqueries.ShapeInfo
  alias Electric.Shapes.DnfPlan
  alias Electric.Shapes.Shape

  @inspector Support.StubInspector.new(
               tables: ["parent", "child"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0, type_id: {20, 1}, not_null: true},
                 %{name: "value", type: "text", pk_position: nil, type_id: {28, 1}},
                 %{
                   name: "parent_id",
                   type: "int8",
                   pk_position: nil,
                   type_id: {20, 1},
                   not_null: true
                 },
                 %{name: "name", type: "text", pk_position: nil, type_id: {28, 1}}
               ]
             )

  describe "Subquery handler" do
    test "a negated move with non-null integer pairs broadcasts its tag without a snapshot" do
      shape =
        Shape.new!("child",
          where: "(parent_id, id) NOT IN (SELECT parent_id, id FROM parent)",
          inspector: @inspector
        )
        |> fill_handles()

      handler = new_handler(shape: shape, subquery_view: MapSet.new([{1, 1}]))
      values = [{{2, 2}, {"2", "2"}}]

      patterns =
        for key <- ["3:v:23:v:2", "4:NULL3:v:2"] do
          %{
            pos: 0,
            value:
              Electric.Shapes.SubqueryTags.make_value_hash_raw("stack-id", "shape-handle", key)
          }
        end

      assert {:ok, %Steady{},
              [
                %Effects.AppendControl{
                  message: %{headers: %{event: "move-out", patterns: ^patterns}}
                },
                %Effects.AddToSubqueryIndex{values: ^values}
              ]} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle(handler), %{move_in: values}}
               )
    end

    test "a positive move-out on a numeric key queries its tags and routes the value until the splice" do
      one = Decimal.new(1)
      handler = new_handler(shape: numeric_shape(), subquery_view: MapSet.new([one]))

      # A row may hold the key as 1.00, whose tag the parent's 1.0 does not hash to.
      assert {:ok,
              %Buffering{active_move: %ActiveMove{kind: :move_out, query_patterns?: true}} =
                handler,
              [
                %Effects.SubscribeGlobalLsn{},
                %Effects.StartMoveInQuery{kind: :move_out, query_patterns?: true}
              ]} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle(handler), %{move_out: [{one, "1.0"}]}}
               )

      assert {:ok, %Buffering{} = handler, []} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 150, []}})

      patterns = [%{pos: 0, value: "queried-tag"}]

      assert {:ok, %Buffering{} = handler, []} =
               EventHandler.handle_event(
                 handler,
                 {:query_move_in_complete, nil, 0, 0, lsn(10), patterns}
               )

      assert {:ok, %Steady{views: %{["$sublink", "0"] => view}}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(10))

      assert view == MapSet.new()

      assert [
               %Effects.AppendControl{
                 message: %{headers: %{event: "move-out", patterns: ^patterns}}
               },
               %Effects.RemoveFromSubqueryIndex{dep_index: 0, values: [{^one, "1.0"}]},
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "a numeric move-out's earlier commits never send a row the client does not hold" do
      # Rows 10, 11 and 13 arrive with the moved key, so the client never holds them. Row 12 had
      # it before the move, and leaves the shape with a key the move-out's snapshot no longer names.
      plan =
        splice_numeric_move_out([
          txn(120, [code_insert("10", "1.0"), code_insert("13", "1.0")]),
          txn(121, [code_update("10", "1.0", "2"), code_insert("11", "1.0")]),
          txn(122, [code_delete("11", "1.0"), code_update({"13", "14"}, "1.0", "2")]),
          txn(123, [code_update("12", "1.0", "3")])
        ])

      assert [
               %Changes.NewRecord{record: %{"id" => "10", "code" => "2"}},
               %Changes.NewRecord{record: %{"id" => "14", "code" => "2"}},
               %Changes.DeletedRecord{old_record: %{"id" => "12", "code" => "1.0"}}
             ] = appended_changes(plan)

      assert [
               %Effects.AppendChanges{},
               %Effects.AppendChanges{},
               %Effects.AppendChanges{},
               %Effects.AppendControl{} | _
             ] = plan
    end

    test "a numeric move-out's earlier commits still delete a held row whose key another row takes" do
      # The client holds rows 20, 21 and 22 with the moved key. Under a deferred primary key, a
      # transaction inserts a second row under each key and deletes both, in either order; rows 22
      # are identical, as two rows of a table without a primary key can be.
      plan =
        splice_numeric_move_out([
          txn(120, [
            code_insert("20", "1.00"),
            code_delete("20", "1.0"),
            code_delete("20", "1.00")
          ]),
          txn(121, [
            code_insert("21", "1.00"),
            code_delete("21", "1.00"),
            code_delete("21", "1.0")
          ]),
          txn(122, [code_insert("22", "1.0"), code_delete("22", "1.0"), code_delete("22", "1.0")])
        ])

      assert [
               %Changes.DeletedRecord{old_record: %{"id" => "20", "code" => "1.0"}},
               %Changes.DeletedRecord{old_record: %{"id" => "21", "code" => "1.0"}},
               %Changes.DeletedRecord{old_record: %{"id" => "22", "code" => "1.0"}}
             ] = appended_changes(plan)
    end

    test "a numeric move-out's earlier commits never delete a row they inserted beside another under its key" do
      # Every row here arrives with the moved key, so the client holds none of them. Rows 30 and 32
      # are identical, as two rows of a table without a primary key can be, and rows 31 share a
      # deferred primary key.
      plan =
        splice_numeric_move_out([
          txn(120, [
            code_insert("30", "1.0"),
            code_insert("30", "1.0"),
            code_delete("30", "1.0"),
            code_delete("30", "1.0")
          ]),
          txn(121, [
            code_insert("31", "1.0"),
            code_insert("31", "1.00"),
            code_delete("31", "1.0"),
            code_delete("31", "1.00")
          ]),
          txn(122, [
            code_insert("32", "1.0"),
            code_insert("32", "1.0"),
            code_delete("32", "1.0"),
            code_update("32", "1.0", "3")
          ])
        ])

      assert appended_changes(plan) == []
    end

    test "a negated replacement's earlier commits never send a row the entering value takes out" do
      handler = new_handler(shape: negated_shape(), subquery_view: MapSet.new([1]))

      assert {:ok, %Buffering{active_move: %ActiveMove{dep_move_kind: :replace}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle(handler),
                  %{move_in: [{2, "2"}], move_out: [{1, "1"}]}}
               )

      # xid 120 commits before the snapshot, with parent 2: it inserts child 9 under it, and moves
      # child 8, which the client holds, there.
      events = [
        txn(120, [child_insert("9", "2"), child_update("8", "3", "2")]),
        {:pg_snapshot_known, {100, 150, []}},
        move_in_complete(lsn(20))
      ]

      handler =
        Enum.reduce(events, handler, fn event, handler ->
          assert {:ok, %Buffering{} = handler, _plan} = EventHandler.handle_event(handler, event)
          handler
        end)

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(20))

      assert view_for(views) == MapSet.new([2])
      assert [%Changes.DeletedRecord{old_record: %{"id" => "8"}}] = appended_changes(plan)
    end

    test "broadcasts full transaction IDs for dependency move-outs" do
      handler = new_handler(subquery_view: MapSet.new([1]))
      dep_handle = dep_handle(handler)

      assert {:ok, %Steady{}, effects} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle,
                  %{move_out: [{1, "1"}], txids: [10], txids8: [4_294_967_306]}}
               )

      assert Enum.any?(effects, fn
               %Effects.AppendControl{
                 message: %{headers: %{event: "move-out", txids: [10], txids8: ["4294967306"]}}
               } ->
                 true

               _ ->
                 false
             end)
    end

    test "converts transactions against the current subquery view" do
      handler = new_handler(subquery_view: MapSet.new([1]))

      assert {:ok, %Steady{}, plan} =
               EventHandler.handle_event(
                 handler,
                 txn(50, [child_insert("1", "1"), child_insert("2", "2")])
               )

      assert [
               %Effects.AppendChanges{
                 changes: [%Changes.NewRecord{record: %{"id" => "1"}, last?: true}]
               },
               %Effects.NotifyFlushed{log_offset: _}
             ] = plan
    end

    test "negated subquery turns dependency move-in into an outer move-out" do
      handler = new_handler(shape: negated_shape(), subquery_view: MapSet.new([2]))
      dep_handle = dep_handle(handler)

      assert {:ok, %Steady{views: %{["$sublink", "0"] => view}} = _handler, plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert view == MapSet.new([1, 2])

      # Case D: negated move-in completes immediately — effects_for_complete
      # adds the value to the index (deferred to completion for NOT IN broadening)
      assert [
               %Effects.AppendControl{
                 message: %{headers: %{event: "move-out", patterns: [%{pos: 0}]}}
               },
               %Effects.AddToSubqueryIndex{dep_index: 0, values: [{1, "1"}]}
             ] = plan
    end

    test "negated subquery turns dependency move-out into a buffered outer move-in" do
      handler = new_handler(shape: negated_shape(), subquery_view: MapSet.new([1]))
      dep_handle = dep_handle(handler)

      # Case B: negated move-out → remove the value when buffering starts so the
      # negated index reflects the post-move exclusion set while buffering.
      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [], move_out: [{1, "1"}]}}
               )

      assert [
               %Effects.SubscribeGlobalLsn{},
               %Effects.RemoveFromSubqueryIndex{dep_index: 0, values: [{1, "1"}]},
               %Effects.StartMoveInQuery{}
             ] = plan

      assert %Buffering{
               active_move: %ActiveMove{
                 views_before_move: %{["$sublink", "0"] => before_view},
                 views_after_move: %{["$sublink", "0"] => after_view}
               }
             } = handler

      assert before_view == MapSet.new([1])
      assert after_view == MapSet.new()

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, []} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 150, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, []} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      # Case B: negated move-out → no further index effect at complete because
      # the buffering-start removal already matches the post-splice dependency view.
      assert {:ok, %Steady{views: %{["$sublink", "0"] => view}}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(10))

      assert view == MapSet.new()

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "splices buffered transactions around the snapshot visibility boundary" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler,
              [
                %Effects.SubscribeGlobalLsn{},
                %Effects.AddToSubqueryIndex{dep_index: 0, values: [{1, "1"}]},
                %Effects.StartMoveInQuery{}
              ]} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, txn(50, [child_insert("10", "1")]))

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 150, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, txn(150, [child_insert("11", "1")]))

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      assert view_for(views) == MapSet.new([1])

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.AppendChanges{
                 changes: [%Changes.NewRecord{record: %{"id" => "11"}, last?: true}]
               },
               %Effects.NotifyFlushed{log_offset: _},
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "splices move-in query rows between emitted pre and post boundary changes" do
      handler = new_handler(subquery_view: MapSet.new([1]))
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{2, "2"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, txn(50, [child_insert("10", "1")]))

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 150, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, txn(150, [child_insert("11", "2")]))

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      assert view_for(views) == MapSet.new([1, 2])

      assert [
               %Effects.AppendChanges{
                 changes: [%Changes.NewRecord{record: %{"id" => "10"}}]
               },
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.AppendChanges{
                 changes: [%Changes.NewRecord{record: %{"id" => "11"}, last?: true}]
               },
               %Effects.NotifyFlushed{log_offset: _},
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "splices updates that become a delete before the boundary and an insert after it" do
      handler = new_handler(subquery_view: MapSet.new([1]))
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{2, "2"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, txn(50, [child_update("10", "1", "2")]))

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, txn(150, [child_update("11", "3", "2")]))

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 150, []}})

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      assert view_for(views) == MapSet.new([1, 2])

      assert [
               %Effects.AppendChanges{
                 changes: [%Changes.DeletedRecord{old_record: %{"id" => "10"}}]
               },
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.AppendChanges{
                 changes: [%Changes.NewRecord{record: %{"id" => "11"}, last?: true}]
               },
               %Effects.NotifyFlushed{log_offset: _},
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "uses lsn updates to splice at the current buffer tail" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, txn(120, [child_insert("10", "1")]))

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 300, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(20))
               )

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(20))

      assert view_for(views) == MapSet.new([1])

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.NotifyFlushed{log_offset: _},
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "waits for an lsn update even when the move-in query completes with an empty buffer" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 300, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(20))
               )

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(20))

      assert view_for(views) == MapSet.new([1])

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "keeps an empty stored move-in snapshot as an effect so execution can clean it up" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 300, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(20), row_count: 0, row_bytes: 0)
               )

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(20))

      assert view_for(views) == MapSet.new([1])

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 0,
                 row_bytes: 0
               },
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "uses an lsn update that arrived before the move-in query completed" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 300, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(20))

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(20))
               )

      assert view_for(views) == MapSet.new([1])

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "keeps the newest seen lsn when an older update arrives later" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 300, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(20))

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(10))

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(20))
               )

      assert view_for(views) == MapSet.new([1])

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "defers queued move outs until after splice and starts the next move in" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, []} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{2, "2"}], move_out: [{1, "1"}]}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 200, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(10))

      assert %Buffering{
               active_move: %ActiveMove{
                 values: [{2, "2"}],
                 views_before_move: views_before,
                 views_after_move: views_after
               }
             } = handler

      assert view_for(views_before) == MapSet.new()
      assert view_for(views_after) == MapSet.new([2])

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.AppendControl{
                 message: %{headers: %{event: "move-out", patterns: [%{pos: 0}]}}
               },
               %Effects.RemoveFromSubqueryIndex{dep_index: 0, values: [{1, "1"}]},
               %Effects.AddToSubqueryIndex{dep_index: 0, values: [{2, "2"}]},
               %Effects.StartMoveInQuery{}
             ] = plan
    end

    test "queued second move-in emits buffering effects only after it is dequeued" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler,
              [
                %Effects.SubscribeGlobalLsn{},
                %Effects.AddToSubqueryIndex{dep_index: 0, values: [{1, "1"}]},
                %Effects.StartMoveInQuery{}
              ]} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}, queue: queue} = handler, []} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{2, "2"}], move_out: []}}
               )

      assert {[{2, "2"}], _, _} = Map.fetch!(queue.move_in, 0)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, []} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 200, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, []} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{values: [{2, "2"}]}} = _handler, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(10))

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.AddToSubqueryIndex{dep_index: 0, values: [{2, "2"}]},
               %Effects.StartMoveInQuery{}
             ] = plan
    end

    test "chained move-in resolves without needing a new lsn broadcast" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, []} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{2, "2"}], move_out: [{1, "1"}]}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 200, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      # First splice completes, second move-in starts
      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(10))

      # Second move-in resolves with no further lsn broadcasts
      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {200, 300, []}})

      assert {:ok, %Steady{views: views}, _plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      assert view_for(views) == MapSet.new([2])
    end

    test "applies a queued move out for the active move-in value after splice" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, []} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [], move_out: [{1, "1"}]}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 200, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(10))

      assert view_for(views) == MapSet.new()

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.AppendControl{
                 message: %{headers: %{event: "move-out", patterns: [%{pos: 0}]}}
               },
               %Effects.RemoveFromSubqueryIndex{dep_index: 0, values: [{1, "1"}]},
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "a change buffered with a queued removal never sends a row the removal leaves out" do
      handler = start_move(new_handler(), %{move_in: [{1, "1"}]})

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler), %{move_out: [{1, "1"}], txids: [300]}},
          txn(300, [child_insert("5", "1")]),
          {:pg_snapshot_known, {100, 200, []}}
        ])

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, move_in_complete(lsn(10)))

      assert view_for(views) == MapSet.new()
      assert appended_changes(plan) == []
    end

    test "a queued removal that a later commit cancels keeps the rows it brings back" do
      handler = start_move(new_handler(), %{move_in: [{1, "1"}]})

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler), %{move_out: [{1, "1"}], txids: [300]}},
          txn(300, [child_insert("5", "1")]),
          {:materializer_changes, dep_handle(handler), %{move_in: [{1, "1"}], txids: [301]}},
          {:pg_snapshot_known, {100, 200, []}}
        ])

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, move_in_complete(lsn(10)))

      assert view_for(views) == MapSet.new([1])
      assert [%Changes.NewRecord{record: %{"id" => "5"}}] = appended_changes(plan)
    end

    test "a move-in's snapshot leaves out the rows of a key a removal queued behind it takes back" do
      handler = start_move(new_handler(), %{move_in: [{1, "1"}, {2, "2"}], txids: [90]})

      handler =
        feed(handler, [
          txn(150, [child_insert("5", "1")]),
          {:materializer_changes, dep_handle(handler), %{move_out: [{1, "1"}], txids: [250]}},
          {:pg_snapshot_known, {100, 200, []}},
          move_in_complete(lsn(10))
        ])

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(400))

      assert view_for(views) == MapSet.new([2])
      assert appended_changes(plan) == []

      assert [%{pos: 0, value: removed}] =
               for(
                 %Effects.AppendControl{message: %{headers: %{event: "move-out"} = headers}} <-
                   plan,
                 pattern <- headers.patterns,
                 do: pattern
               )

      assert [%Effects.AppendMoveInSnapshot{skip_rows: skip_rows}] =
               for(%Effects.AppendMoveInSnapshot{} = effect <- plan, do: effect)

      assert skip_rows ==
               {nil, %{0 => MapSet.new([removed])}, MapSet.new([child_insert("5", "1").key])}
    end

    test "a move-in's snapshot leaves out each row an earlier commit wrote when a removal queued behind it takes back every key it brings in" do
      handler = start_move(new_handler(), %{move_in: [{1, "1"}], txids: [90]})

      handler =
        feed(handler, [
          txn(150, [child_insert("5", "1")]),
          {:materializer_changes, dep_handle(handler), %{move_out: [{1, "1"}], txids: [250]}},
          {:pg_snapshot_known, {100, 200, []}},
          move_in_complete(lsn(10))
        ])

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(400))

      assert view_for(views) == MapSet.new()

      assert [%Effects.AppendMoveInSnapshot{skip_rows: {nil, :all, keys}}] =
               for(%Effects.AppendMoveInSnapshot{} = effect <- plan, do: effect)

      assert keys == MapSet.new([child_insert("5", "1").key])
    end

    # No commit before the snapshot wrote its rows, and the snapshot does not see the removal, so
    # Postgres selected them when the query read it: the drain's move-out takes them back after.
    test "a move-in's snapshot sends the rows no earlier commit wrote, which a removal queued behind it takes back" do
      handler = start_move(new_handler(), %{move_in: [{1, "1"}], txids: [90]})

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler), %{move_out: [{1, "1"}], txids: [250]}},
          {:pg_snapshot_known, {100, 200, []}},
          move_in_complete(lsn(10))
        ])

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(400))

      assert view_for(views) == MapSet.new()

      assert [
               %Effects.AppendMoveInSnapshot{skip_rows: nil},
               %Effects.AppendControl{message: %{headers: %{event: "move-out"}}}
             ] =
               for(
                 effect <- plan,
                 match?(%Effects.AppendMoveInSnapshot{}, effect) or
                   match?(
                     %Effects.AppendControl{message: %{headers: %{event: "move-out"}}},
                     effect
                   ),
                 do: effect
               )
    end

    # The removal of the move's own transaction: Postgres never selected the rows it takes out.
    test "a move-in's snapshot leaves out every row a removal of its own transaction takes back" do
      handler = start_move(new_handler(), %{move_in: [{1, "1"}], txids: [90]})

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler), %{move_out: [{1, "1"}], txids: [90]}},
          {:pg_snapshot_known, {100, 200, []}},
          move_in_complete(lsn(10))
        ])

      assert {:ok, %Steady{}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(400))

      assert [%Effects.AppendMoveInSnapshot{skip_rows: {:all, nil, keys}}] =
               for(%Effects.AppendMoveInSnapshot{} = effect <- plan, do: effect)

      assert keys == MapSet.new()
    end

    # A removal the snapshot sees committed before the query read Postgres, which no longer selected
    # the rows it took out: they stay out of the snapshot, as those of the move's own transaction.
    # One naming a transaction the snapshot does not see is taken as later.
    for {name, txids, snapshot, skips?} <- [
          {"a transaction it sees", %{txids: [150]}, {100, 200, []}, true},
          {"a transaction it sees and one after it", %{txids: [150, 250]}, {100, 200, []}, false},
          {"a transaction in progress when it was taken", %{txids: [150]}, {100, 200, [150]},
           false},
          {"a 64-bit transaction it sees", %{txids: [150], txids8: [4_294_967_446]},
           {4_294_967_396, 4_294_967_496, []}, true},
          {"a 64-bit transaction of a later epoch", %{txids: [150], txids8: [8_589_934_742]},
           {4_294_967_396, 4_294_967_496, []}, false},
          {"a 64-bit transaction it sees and a 32-bit one after it",
           %{txids: [150, 250], txids8: [4_294_967_446]}, {4_294_967_396, 4_294_967_496, []},
           false}
        ] do
      test "a move-in's snapshot and a queued removal naming #{name}" do
        handler = start_move(new_handler(), %{move_in: [{1, "1"}, {2, "2"}], txids: [90]})
        removal = Map.put(unquote(Macro.escape(txids)), :move_out, [{1, "1"}])

        handler =
          feed(handler, [
            {:materializer_changes, dep_handle(handler), removal},
            {:pg_snapshot_known, unquote(Macro.escape(snapshot))},
            move_in_complete(lsn(10))
          ])

        assert {:ok, %Steady{views: views}, plan} =
                 EventHandler.handle_event(handler, global_last_seen_lsn(400))

        assert view_for(views) == MapSet.new([2])

        assert [%{pos: 0, value: removed}] =
                 for(
                   %Effects.AppendControl{message: %{headers: %{event: "move-out"} = headers}} <-
                     plan,
                   pattern <- headers.patterns,
                   do: pattern
                 )

        assert [%Effects.AppendMoveInSnapshot{skip_rows: skip_rows}] =
                 for(%Effects.AppendMoveInSnapshot{} = effect <- plan, do: effect)

        assert skip_rows ==
                 if(unquote(skips?), do: {%{0 => MapSet.new([removed])}, nil, MapSet.new()})
      end
    end

    test "a move-in's later commit never deletes a row a removal of its own transaction left out of its snapshot" do
      handler = start_move(new_handler(), %{move_in: [{1, "1"}, {2, "2"}], txids: [90]})

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler), %{move_out: [{1, "1"}], txids: [90]}},
          txn(301, [child_update("7", "1", "3")]),
          {:pg_snapshot_known, {100, 200, []}}
        ])

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, move_in_complete(lsn(10)))

      assert view_for(views) == MapSet.new([2])
      assert appended_changes(plan) == []

      assert [%Effects.AppendMoveInSnapshot{skip_rows: {%{0 => _}, nil, keys}}] =
               for(%Effects.AppendMoveInSnapshot{} = effect <- plan, do: effect)

      assert keys == MapSet.new()
    end

    # A dependency's own splice reports its moves with no transaction: such a removal is taken as
    # later than the move, so the drain takes back the rows the snapshot sends.
    test "a move-in's snapshot sends the rows a removal naming no transaction takes back" do
      handler = start_move(new_handler(), %{move_in: [{1, "1"}], txids: [90]})

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler), %{move_out: [{1, "1"}]}},
          {:pg_snapshot_known, {100, 200, []}},
          move_in_complete(lsn(10))
        ])

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(400))

      assert view_for(views) == MapSet.new()

      assert [%Effects.AppendMoveInSnapshot{skip_rows: nil}] =
               for(%Effects.AppendMoveInSnapshot{} = effect <- plan, do: effect)
    end

    test "a move-in's later commit inserts a row its snapshot left out rather than update it" do
      handler = start_move(new_handler(), %{move_in: [{1, "1"}, {2, "2"}], txids: [90]})
      inserted = child_insert("7", "1")

      moved =
        Changes.UpdatedRecord.new(
          relation: inserted.relation,
          old_record: inserted.record,
          record: %{inserted.record | "parent_id" => "2"}
        )
        |> Changes.fill_key(["id"])

      handler =
        feed(handler, [
          txn(150, [inserted]),
          {:materializer_changes, dep_handle(handler), %{move_out: [{1, "1"}], txids: [250]}},
          txn(301, [moved]),
          {:pg_snapshot_known, {100, 200, []}}
        ])

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, move_in_complete(lsn(10)))

      assert view_for(views) == MapSet.new([2])
      assert [%Changes.NewRecord{record: %{"id" => "7"}}] = appended_changes(plan)

      assert [%Effects.AppendMoveInSnapshot{skip_rows: {nil, _patterns, keys}}] =
               for(%Effects.AppendMoveInSnapshot{} = effect <- plan, do: effect)

      assert keys == MapSet.new([inserted.key])
    end

    test "a move-in splices a removal committed by its snapshot of a key it does not bring in" do
      handler =
        start_move(new_handler(subquery_view: MapSet.new([2])), %{move_in: [{1, "1"}]})

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler), %{move_out: [{2, "2"}], txids: [150]}},
          txn(150, [child_insert("5", "1")]),
          {:pg_snapshot_known, {100, 200, []}},
          move_in_complete(lsn(10))
        ])

      assert {:ok, %Steady{views: views}, _plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(400))

      assert view_for(views) == MapSet.new([1])
    end

    test "a move-in's earlier commit never sends a row a removal committed by its snapshot leaves out" do
      handler =
        start_move(new_handler(subquery_view: MapSet.new([2])), %{move_in: [{1, "1"}]})

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler), %{move_out: [{2, "2"}], txids: [150]}},
          txn(150, [child_insert("5", "2")]),
          {:pg_snapshot_known, {100, 200, []}},
          move_in_complete(lsn(10))
        ])

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(400))

      assert view_for(views) == MapSet.new([1])
      assert appended_changes(plan) == []
    end

    test "a move-out's earlier commits write their rows against a removal queued behind it" do
      one = Decimal.new(1)
      handler = new_two_dependency_handler(MapSet.new([one, Decimal.new(2)]), MapSet.new([7]))

      assert {:ok, %Buffering{active_move: %ActiveMove{kind: :move_out}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle(handler, 0), %{move_out: [{one, "1"}]}}
               )

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler, 1), %{move_out: [{7, "7"}], txids: [120]}},
          txn(120, [code_parent_insert("10", "9", "7")]),
          txn(160, [code_update("10", "9", "2")]),
          {:pg_snapshot_known, {100, 150, []}}
        ])

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(
                 handler,
                 {:query_move_in_complete, nil, 0, 0, lsn(20), [%{pos: 0, value: "tag"}]}
               )

      assert view_for(views, ["$sublink", "1"]) == MapSet.new()
      assert [%Changes.NewRecord{record: %{"id" => "10", "code" => "2"}}] = appended_changes(plan)
    end

    # A numeric move-out queries its tags, so the drain starts the queued one as the next move,
    # which takes its rows out at its own splice: until then the client holds them.
    test "a move-out leaves a numeric removal queued behind it to the move-out the drain starts" do
      [one, two, three] = Enum.map([1, 2, 3], &Decimal.new/1)

      handler =
        start_move(
          new_handler(shape: numeric_shape(), subquery_view: MapSet.new([one, two, three])),
          %{move_out: [{one, "1"}]}
        )

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler), %{move_out: [{two, "2"}], txids: [120]}},
          txn(120, [code_insert("10", "2")]),
          txn(160, [code_update("10", "2", "3")]),
          {:pg_snapshot_known, {100, 150, []}}
        ])

      assert {:ok,
              %Buffering{
                active_move: %ActiveMove{
                  kind: :move_out,
                  values: [{^two, "2"}],
                  views_before_move: before
                }
              }, plan} =
               EventHandler.handle_event(
                 handler,
                 {:query_move_in_complete, nil, 0, 0, lsn(20), [%{pos: 0, value: "tag"}]}
               )

      assert view_for(before) == MapSet.new([two, three])

      assert [
               %Changes.NewRecord{record: %{"id" => "10", "code" => "2"}},
               %Changes.UpdatedRecord{record: %{"id" => "10", "code" => "3"}}
             ] = appended_changes(plan)
    end

    test "a change buffered with a key queued into a negated view never sends the row it excludes" do
      handler =
        start_move(new_handler(shape: negated_shape(), subquery_view: MapSet.new([1])), %{
          move_out: [{1, "1"}]
        })

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler), %{move_in: [{7, "7"}], txids: [300]}},
          txn(300, [child_insert("5", "7"), child_insert("6", "8")]),
          {:pg_snapshot_known, {100, 200, []}}
        ])

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, move_in_complete(lsn(10)))

      assert view_for(views) == MapSet.new([7])
      assert [%Changes.NewRecord{record: %{"id" => "6"}}] = appended_changes(plan)
    end

    # The drain pairs the queued halves into a replacement it queries next, which takes key 3's
    # rows out at its own splice: until then the client holds them.
    test "a removal the drain folds into the move it starts stays that move's to apply" do
      handler =
        start_move(new_handler(shape: negated_shape(), subquery_view: MapSet.new([1, 2])), %{
          move_out: [{1, "1"}]
        })

      handler =
        feed(handler, [
          {:materializer_changes, dep_handle(handler),
           %{move_out: [{2, "2"}], move_in: [{3, "3"}], txids: [300]}},
          txn(300, [child_insert("5", "3")]),
          {:pg_snapshot_known, {100, 200, []}}
        ])

      assert {:ok,
              %Buffering{
                active_move: %ActiveMove{
                  kind: :replace,
                  views_before_move: before,
                  views_after_move: after_move
                }
              }, plan} = EventHandler.handle_event(handler, move_in_complete(lsn(10)))

      assert view_for(before) == MapSet.new([2])
      assert view_for(after_move) == MapSet.new([3])
      assert [%Changes.NewRecord{record: %{"id" => "5"}}] = appended_changes(plan)
    end

    test "batches consecutive move ins into a single active move in" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle,
                  %{move_in: [{1, "1"}, {2, "2"}], move_out: []}}
               )

      assert %Buffering{
               active_move: %ActiveMove{
                 values: [{1, "1"}, {2, "2"}],
                 views_before_move: views_before,
                 views_after_move: views_after
               }
             } = handler

      assert view_for(views_before) == MapSet.new()
      assert view_for(views_after) == MapSet.new([1, 2])
    end

    test "cancels pending inverse ops while buffering" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, []} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{2, "2"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, []} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [], move_out: [{2, "2"}]}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 200, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(10))

      assert view_for(views) == MapSet.new([1])

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan
    end

    test "merges queued move outs into a single control message after splice" do
      handler = new_handler(subquery_view: MapSet.new([2]))
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, []} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [], move_out: [{1, "1"}]}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, []} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [], move_out: [{2, "2"}]}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 200, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      assert {:ok, %Steady{views: views}, plan} =
               EventHandler.handle_event(handler, global_last_seen_lsn(10))

      assert view_for(views) == MapSet.new()

      assert [
               %Effects.AppendControl{message: %{headers: %{event: "move-in"}}},
               %Effects.AppendMoveInSnapshot{
                 snapshot_name: "move-in-snapshot",
                 row_count: 1,
                 row_bytes: 100
               },
               %Effects.AppendControl{
                 message: %{headers: %{event: "move-out", patterns: patterns}}
               },
               %Effects.RemoveFromSubqueryIndex{values: values},
               %Effects.UnsubscribeGlobalLsn{}
             ] = plan

      assert length(patterns) == 2
      assert length(values) == 2
    end

    test "returns {:error, :buffer_overflow} when buffered transactions exceed the limit" do
      handler = new_handler(buffer_max_transactions: 3)
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, txn(50, [child_insert("1", "1")]))

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, txn(51, [child_insert("2", "1")]))

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, txn(52, [child_insert("3", "1")]))

      assert {:error, :buffer_overflow} =
               EventHandler.handle_event(handler, txn(53, [child_insert("4", "1")]))
    end

    test "returns truncate error on TruncatedRelation while steady" do
      handler = new_handler(subquery_view: MapSet.new([1]))

      assert {:error, {:truncate, 1}} =
               EventHandler.handle_event(handler, txn(1, [child_truncate()]))
    end

    test "returns truncate error while buffering once splice completes" do
      handler = new_handler()
      dep_handle = dep_handle(handler)

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 {:materializer_changes, dep_handle, %{move_in: [{1, "1"}], move_out: []}}
               )

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, txn(50, [child_truncate()]))

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 150, []}})

      assert {:ok, %Buffering{active_move: %ActiveMove{}} = handler, _plan} =
               EventHandler.handle_event(
                 handler,
                 move_in_complete(lsn(10))
               )

      assert {:error, {:truncate, 50}} =
               EventHandler.handle_event(handler, global_last_seen_lsn(10))
    end

    test "raises on dependency handle mismatch" do
      assert_raise ArgumentError, ~r/unexpected dependency handle/, fn ->
        new_handler()
        |> EventHandler.handle_event(
          {:materializer_changes, "wrong", %{move_in: [], move_out: []}}
        )
      end
    end

    test "raises on query callbacks while steady" do
      handler = new_handler()

      assert_raise ArgumentError, ~r/no move-in is buffering/, fn ->
        EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 200, []}})
      end

      assert_raise ArgumentError, ~r/no move-in is buffering/, fn ->
        EventHandler.handle_event(handler, move_in_complete(lsn(1), row_count: 0, row_bytes: 0))
      end
    end
  end

  # -- Helpers --

  defp new_handler(opts \\ []) do
    shape = Keyword.get(opts, :shape, shape())
    {:ok, dnf_plan} = DnfPlan.compile(shape)
    dep_handle = hd(shape.shape_dependencies_handles)

    %Steady{
      shape_info: %ShapeInfo{
        shape: shape,
        stack_id: "stack-id",
        shape_handle: "shape-handle",
        dnf_plan: dnf_plan,
        ref_resolver:
          RefResolver.new(%{dep_handle => {0, ["$sublink", "0"]}}, %{0 => ["$sublink", "0"]}),
        buffer_max_transactions: Keyword.get(opts, :buffer_max_transactions, 1000)
      },
      views: %{["$sublink", "0"] => Keyword.get(opts, :subquery_view, MapSet.new())}
    }
  end

  defp start_move(handler, payload) do
    assert {:ok, %Buffering{} = handler, _plan} =
             EventHandler.handle_event(
               handler,
               {:materializer_changes, dep_handle(handler), payload}
             )

    handler
  end

  defp feed(handler, events) do
    Enum.reduce(events, handler, fn event, handler ->
      assert {:ok, %Buffering{} = handler, _effects} = EventHandler.handle_event(handler, event)
      handler
    end)
  end

  defp dep_handle(handler) do
    handler.shape_info.ref_resolver.handle_to_ref |> Map.keys() |> hd()
  end

  defp dep_handle(handler, dep_index) do
    Enum.find_value(handler.shape_info.ref_resolver.handle_to_ref, fn
      {handle, {^dep_index, _ref}} -> handle
      _ -> nil
    end)
  end

  # `code IN (SELECT code ...) OR parent_id IN (SELECT id ...)` with a numeric code, and these
  # views of its two dependencies.
  defp new_two_dependency_handler(code_view, parent_view) do
    inspector =
      Support.StubInspector.new(
        tables: ["parent", "child"],
        columns: [
          %{name: "id", type: "int8", pk_position: 0, type_id: {20, 1}, not_null: true},
          %{name: "code", type: "numeric", pk_position: nil, type_id: {1700, -1}},
          %{name: "parent_id", type: "int8", pk_position: nil, type_id: {20, 1}},
          %{name: "value", type: "text", pk_position: nil, type_id: {28, 1}}
        ]
      )

    shape =
      Shape.new!("child",
        where:
          "code IN (SELECT code FROM public.parent) OR " <>
            "parent_id IN (SELECT id FROM public.parent WHERE value = 'keep')",
        inspector: inspector
      )
      |> fill_handles()

    {:ok, dnf_plan} = DnfPlan.compile(shape)
    [code_handle, parent_handle] = shape.shape_dependencies_handles
    code_ref = ["$sublink", "0"]
    parent_ref = ["$sublink", "1"]

    %Steady{
      shape_info: %ShapeInfo{
        shape: shape,
        stack_id: "stack-id",
        shape_handle: "shape-handle",
        dnf_plan: dnf_plan,
        ref_resolver:
          RefResolver.new(
            %{code_handle => {0, code_ref}, parent_handle => {1, parent_ref}},
            %{0 => code_ref, 1 => parent_ref}
          ),
        buffer_max_transactions: 1000
      },
      views: %{code_ref => code_view, parent_ref => parent_view}
    }
  end

  defp view_for(views, ref \\ ["$sublink", "0"]) when is_map(views) do
    views[ref]
  end

  defp shape do
    Shape.new!("child",
      where: "parent_id IN (SELECT id FROM public.parent WHERE value = 'keep')",
      inspector: @inspector
    )
    |> fill_handles()
  end

  # Moves the key 1 out of a view of 1 and 2 on `numeric_shape/0`, buffers `txns` before the
  # move-out's snapshot, and returns the splice's effects.
  defp splice_numeric_move_out(txns) do
    one = Decimal.new(1)

    handler =
      new_handler(shape: numeric_shape(), subquery_view: MapSet.new([one, Decimal.new(2)]))

    assert {:ok, %Buffering{active_move: %ActiveMove{kind: :move_out}} = handler, _plan} =
             EventHandler.handle_event(
               handler,
               {:materializer_changes, dep_handle(handler), %{move_out: [{one, "1.0"}]}}
             )

    handler =
      Enum.reduce(txns, handler, fn txn, handler ->
        assert {:ok, %Buffering{} = handler, []} = EventHandler.handle_event(handler, txn)
        handler
      end)

    assert {:ok, %Buffering{} = handler, []} =
             EventHandler.handle_event(handler, {:pg_snapshot_known, {100, 150, []}})

    assert {:ok, %Buffering{} = handler, []} =
             EventHandler.handle_event(
               handler,
               {:query_move_in_complete, nil, 0, 0, lsn(20), [%{pos: 0, value: "tag"}]}
             )

    assert {:ok, %Steady{}, plan} = EventHandler.handle_event(handler, global_last_seen_lsn(20))
    plan
  end

  defp appended_changes(plan) do
    for %Effects.AppendChanges{changes: changes} <- plan, change <- changes, do: change
  end

  defp numeric_shape do
    inspector =
      Support.StubInspector.new(
        tables: ["parent", "child"],
        columns: [
          %{name: "id", type: "int8", pk_position: 0, type_id: {20, 1}, not_null: true},
          %{name: "code", type: "numeric", pk_position: nil, type_id: {1700, -1}}
        ]
      )

    Shape.new!("child", where: "code IN (SELECT code FROM public.parent)", inspector: inspector)
    |> fill_handles()
  end

  defp negated_shape do
    Shape.new!("child",
      where: "parent_id NOT IN (SELECT id FROM public.parent WHERE value = 'keep')",
      inspector: @inspector
    )
    |> fill_handles()
  end

  defp fill_handles(shape) do
    filled_deps = Enum.map(shape.shape_dependencies, &fill_handles/1)
    handles = Enum.map(filled_deps, &Shape.generate_id/1)
    %{shape | shape_dependencies: filled_deps, shape_dependencies_handles: handles}
  end

  defp txn(xid, changes) do
    %Transaction{
      xid: xid,
      changes: changes,
      num_changes: length(changes),
      lsn: lsn(xid),
      last_log_offset: Electric.Replication.LogOffset.new(lsn(xid), max(length(changes) - 1, 0))
    }
  end

  defp lsn(value), do: Lsn.from_integer(value)
  defp global_last_seen_lsn(value), do: {:global_last_seen_lsn, value}

  defp move_in_complete(lsn, opts \\ []) do
    {:query_move_in_complete, Keyword.get(opts, :snapshot_name, "move-in-snapshot"),
     Keyword.get(opts, :row_count, 1), Keyword.get(opts, :row_bytes, 100), lsn, nil}
  end

  defp child_insert(id, parent_id) do
    %Changes.NewRecord{
      relation: {"public", "child"},
      record: %{"id" => id, "parent_id" => parent_id, "name" => "child-#{id}"}
    }
    |> Changes.fill_key(["id"])
  end

  defp code_insert(id, code) do
    %Changes.NewRecord{relation: {"public", "child"}, record: %{"id" => id, "code" => code}}
    |> Changes.fill_key(["id"])
  end

  defp code_parent_insert(id, code, parent_id) do
    %Changes.NewRecord{
      relation: {"public", "child"},
      record: %{"id" => id, "code" => code, "parent_id" => parent_id}
    }
    |> Changes.fill_key(["id"])
  end

  defp code_update({old_id, new_id}, old_code, new_code) do
    Changes.UpdatedRecord.new(
      relation: {"public", "child"},
      old_record: %{"id" => old_id, "code" => old_code},
      record: %{"id" => new_id, "code" => new_code}
    )
    |> Changes.fill_key(["id"])
  end

  defp code_update(id, old_code, new_code), do: code_update({id, id}, old_code, new_code)

  defp code_delete(id, code) do
    %Changes.DeletedRecord{
      relation: {"public", "child"},
      old_record: %{"id" => id, "code" => code}
    }
    |> Changes.fill_key(["id"])
  end

  defp child_truncate do
    %Changes.TruncatedRelation{relation: {"public", "child"}}
  end

  defp child_update(id, old_parent_id, new_parent_id) do
    Changes.UpdatedRecord.new(
      relation: {"public", "child"},
      old_record: %{"id" => id, "parent_id" => old_parent_id, "name" => "child-#{id}-old"},
      record: %{"id" => id, "parent_id" => new_parent_id, "name" => "child-#{id}-new"}
    )
    |> Changes.fill_key(["id"])
  end
end
