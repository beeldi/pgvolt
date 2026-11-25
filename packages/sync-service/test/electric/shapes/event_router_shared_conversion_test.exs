defmodule Electric.Shapes.EventRouterSharedConversionTest do
  @moduledoc """
  The router converts and encodes once for a whole fan-out class. This asserts the two things that
  makes safe: that the shared result is byte-identical to what each shape would have produced on
  its own, and that the sharing is actually engaging -- a test that silently prepared nothing would
  pass every equality check while proving nothing.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog, only: [with_log: 1]

  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.Commit
  alias Electric.Replication.Changes.PreparedChange
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.LogOffset
  alias Electric.Shapes.EventRouter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  import Support.EmployeesFixtures,
    only: [inspector: 1, insert: 1, insert: 2, relation: 0, shape!: 2]

  @inspector inspector(~w[id firmId divisionId note])

  defp shape!(opts), do: shape!(@inspector, opts)

  # Same table, same columns, same replica -- one class, differing only in the where clause, which
  # decides membership and nothing else about the output.
  defp class_shapes(count) do
    Map.new(1..count, fn i ->
      {"shared-#{i}", shape!(where: ~s|"firmId" = 'F0' AND "divisionId" IN ('D0','B#{i}')|)}
    end)
  end

  defp record, do: %{"id" => "eq-1", "firmId" => "F0", "divisionId" => "D0", "note" => "n"}

  defp insert, do: insert(record())

  defp fragment(changes) do
    %TransactionFragment{
      xid: 100,
      has_begin?: true,
      commit: %Commit{commit_timestamp: ~U[2024-01-01 00:00:00Z]},
      changes: changes,
      affected_relations: MapSet.new([relation()]),
      change_count: length(changes),
      last_log_offset: LogOffset.new(Lsn.from_integer(1000), 1)
    }
  end

  defp router_with(shapes) do
    Enum.reduce(shapes, EventRouter.new(shared_conversion: true), fn {id, shape}, router ->
      EventRouter.add_shape(router, id, shape)
    end)
  end

  defp route(shapes, changes) do
    {result, _router} = EventRouter.event_by_shape_handle(router_with(shapes), fragment(changes))
    result
  end

  # What the consumer would have produced for this shape with no sharing at all.
  defp encode_independently(shape, change, xid) do
    shape
    |> Shape.convert_change(change, stack_id: "shared-test", shape_handle: "h")
    |> Enum.flat_map(
      &Electric.LogItems.from_change(&1, xid, Shape.pk(shape, &1.relation), shape.replica)
    )
    |> Enum.map(fn {offset, %{key: key, headers: %{operation: op}} = item} ->
      {offset, key, op, Jason.encode!(item)}
    end)
  end

  defp prepared_changes(fragment) do
    Enum.filter(fragment.changes, &is_struct(&1, PreparedChange))
  end

  test "every shape in a class gets a prepared change whose bytes it would have produced itself" do
    shapes = class_shapes(4)
    change = insert()

    result = route(shapes, [change])

    assert map_size(result) == 4, "every shape's where clause matches the probe record"

    prepared_per_shape =
      Map.new(result, fn {shape_id, fragment} -> {shape_id, prepared_changes(fragment)} end)

    assert Enum.all?(prepared_per_shape, fn {_id, prepared} -> length(prepared) == 1 end),
           "sharing did not engage: #{inspect(prepared_per_shape)}"

    Enum.each(result, fn {shape_id, fragment} ->
      [%PreparedChange{log_items: log_items, total_size: total_size}] = prepared_changes(fragment)

      expected = encode_independently(Map.fetch!(shapes, shape_id), change, 100)

      assert log_items == expected,
             "shared encoding differs from #{shape_id}'s own encoding"

      assert total_size ==
               Enum.reduce(expected, 0, fn {_, _, _, json}, acc -> acc + byte_size(json) end)
    end)
  end

  test "with the feature off every shape is handed the raw change, as before it existed" do
    shapes = class_shapes(4)
    change = insert()

    router =
      Enum.reduce(shapes, EventRouter.new(), fn {id, shape}, router ->
        EventRouter.add_shape(router, id, shape)
      end)

    {result, _router} = EventRouter.event_by_shape_handle(router, fragment([change]))

    assert map_size(result) == 4, "routing must be unaffected by the switch"

    Enum.each(result, fn {shape_id, fragment} ->
      assert [] == prepared_changes(fragment),
             "#{shape_id} got a prepared change with shared conversion off"

      assert [%Changes.NewRecord{}] = fragment.changes
    end)
  end

  test "shared encoding preserves full transaction positions for every shape" do
    shapes =
      Map.new(class_shapes(2), fn {id, shape} ->
        {id, %{shape | source_positions: :full_xid, source_generation: "generation-a"}}
      end)

    transaction = %{fragment([insert()]) | full_xid: 4_294_967_396}
    {result, _router} = EventRouter.event_by_shape_handle(router_with(shapes), transaction)

    assert map_size(result) == 2

    for {_id, fragment} <- result do
      assert fragment.full_xid == 4_294_967_396
      assert [%PreparedChange{log_items: [{_, _, _, json}]}] = fragment.changes
      assert %{"headers" => %{"txids" => [100], "txids8" => ["4294967396"]}} = Jason.decode!(json)
    end
  end

  test "shared conversion separates source modes and source generations" do
    legacy = shape!(where: ~s|"firmId" = 'F0'|)
    full = %{legacy | source_positions: :full_xid, source_generation: "generation-a"}
    next_generation = %{full | source_generation: "generation-b"}

    refute Shape.fanout_class(legacy) == Shape.fanout_class(full)
    refute Shape.fanout_class(full) == Shape.fanout_class(next_generation)
  end

  test "one shape in a class is left for its own consumer to convert" do
    shapes = class_shapes(1)

    result = route(shapes, [insert()])

    assert [fragment] = Map.values(result)
    assert prepared_changes(fragment) == []
    assert [%Changes.NewRecord{}] = fragment.changes
  end

  test "shapes selecting different columns are different classes and are not shared" do
    shapes = %{
      "wide" => shape!(where: ~s|"firmId" = 'F0'|),
      "narrow" => shape!(where: ~s|"firmId" = 'F0'|, columns: ["id", "firmId"])
    }

    result = route(shapes, [insert()])

    assert map_size(result) == 2

    Enum.each(result, fn {shape_id, fragment} ->
      assert prepared_changes(fragment) == [],
             "#{shape_id} was shared with a shape projecting different columns"
    end)
  end

  test "updates are never shared, because routing unions old-record and new-record matches" do
    # "leaving" matches only the old record, "arriving" only the new one: same class, but one must
    # emit a move-out and the other a move-in.
    shapes = %{
      "leaving" => shape!(where: ~s|"divisionId" = 'D0'|),
      "arriving" => shape!(where: ~s|"divisionId" = 'D9'|)
    }

    update = %Changes.UpdatedRecord{
      relation: relation(),
      old_record: record(),
      record: Map.put(record(), "divisionId", "D9"),
      changed_columns: MapSet.new(["divisionId"]),
      log_offset: LogOffset.new(Lsn.from_integer(1000), 1),
      key: ~s|"public"."employees"/"eq-1"|
    }

    result = route(shapes, [update])

    assert map_size(result) == 2

    Enum.each(result, fn {shape_id, fragment} ->
      assert prepared_changes(fragment) == [], "#{shape_id} shared an update"
    end)
  end

  test "truncations are never shared" do
    shapes = class_shapes(3)

    truncate = %Changes.TruncatedRelation{relation: relation()}

    result = route(shapes, [truncate])

    assert map_size(result) == 3

    Enum.each(result, fn {shape_id, fragment} ->
      assert prepared_changes(fragment) == [], "#{shape_id} shared a truncation"
      assert [%Changes.TruncatedRelation{}] = fragment.changes
    end)
  end

  # Routing fails open: when it cannot evaluate a predicate on a record it returns the shapes it
  # cannot rule out. Before sharing, the consumer re-evaluated the where clause and dropped what
  # did not belong; sharing removes that second pass, so an inexact decision must not be shared.
  describe "inexact routing" do
    # A record whose int8 column holds something that is not an integer. Real sources: a column
    # whose Postgres type changed under a cached inspector entry, a value the converter could not
    # decode, a domain/enum the caster does not know.
    @typed_inspector inspector(["id", {"num", "int8"}, "note"])

    defp typed_shape!(where), do: shape!(@typed_inspector, where: where)

    defp unparseable_insert do
      insert(%{"id" => "eq-1", "num" => "not-a-number", "note" => "n"})
    end

    defp rows_appended(result, shapes) do
      Map.new(result, fn {shape_id, fragment} ->
        shape = Map.fetch!(shapes, shape_id)

        {shape_id,
         Enum.flat_map(
           fragment.changes,
           &Shape.convert_change(shape, &1, stack_id: "inexact", shape_handle: shape_id)
         )}
      end)
    end

    test "an unevaluable where clause leaves the whole change unprepared" do
      # `num > 5` does not optimise, so it is evaluated directly and the comparison against a
      # non-integer cannot be decided.
      shapes = %{
        "matches" => typed_shape!(~s|"note" != 'x'|),
        "cannot-evaluate" => typed_shape!(~s|"num" > 5|)
      }

      change = unparseable_insert()

      assert Shape.fanout_class(shapes["matches"]) ==
               Shape.fanout_class(shapes["cannot-evaluate"]),
             "the two shapes must share a class, otherwise nothing would be prepared anyway"

      result = route(shapes, [change])

      Enum.each(result, fn {shape_id, fragment} ->
        assert prepared_changes(fragment) == [],
               "#{shape_id} was handed a prepared change derived from an inexact routing decision"
      end)

      assert %{"cannot-evaluate" => []} = rows_appended(result, shapes)
    end

    test "an unparseable indexed field widens routing to the shapes reading it and no further" do
      # The indexed equality on `num` cannot be decided, so routing returns the shapes indexed on
      # `num` rather than ruling any of them out. `note` is a second index key on the same
      # condition and its value is readable, so that shape is decided exactly and must not be
      # dragged along.
      shapes = %{
        "eq-5" => typed_shape!(~s|"num" = 5|),
        "eq-6" => typed_shape!(~s|"num" = 6|),
        "other-field" => typed_shape!(~s|"note" = 'x'|)
      }

      assert Shape.fanout_class(shapes["eq-5"]) == Shape.fanout_class(shapes["eq-6"]),
             "the two shapes must share a class, otherwise nothing would be prepared anyway"

      result = route(shapes, [unparseable_insert()])

      assert Enum.sort(Map.keys(result)) == ["eq-5", "eq-6"],
             "routing widened past the column it could not read"

      Enum.each(result, fn {shape_id, fragment} ->
        assert prepared_changes(fragment) == [],
               "#{shape_id} was handed a prepared change derived from an inexact routing decision"
      end)

      assert %{"eq-5" => [], "eq-6" => []} = rows_appended(result, shapes)
    end

    test "a shape never converts a change belonging to another relation" do
      orders =
        Shape.new!("public.orders",
          where: ~s|"note" != 'x'|,
          inspector:
            StubInspector.new(
              tables: ["orders"],
              columns: [
                %{name: "id", type: "text", pk_position: 0},
                %{name: "note", type: "text"}
              ]
            )
        )

      assert Shape.convert_routed_change(orders, unparseable_insert()) == []
    end

    test "a dependency shape falls back to per-shape conversion instead of skipping membership" do
      # `convert_routed_change/2` skips the where clause on the strength of routing having decided
      # membership exactly. Routing over-approximates for dependency shapes by design, which is
      # why `fanout_class/1` refuses them -- so reaching the shared path with one would emit a row
      # nobody decided belonged.
      shape = shape!(where: ~s|"firmId" = 'F0'|)
      with_dependency = %{shape | shape_dependencies: [shape]}

      assert Shape.fanout_class(with_dependency) == nil,
             "the router would never group this shape, so the guard is the only thing left"

      {result, log} =
        with_log(fn ->
          EventRouter.prepare_change_for_shape(
            with_dependency,
            insert(),
            %TransactionFragment{xid: 100},
            nil
          )
        end)

      assert result == nil,
             "a dependency shape was converted as if routing had decided its membership"

      assert log =~ "falling back to per-shape conversion"
    end

    test "the fallback warning names the exception, not the row it failed on" do
      # Not valid UTF-8, so the encoder raises, and its message quotes the whole string as bytes.
      note = "row-value-7f3a" <> <<0xFF>>
      change = insert(%{record() | "note" => note})

      {result, log} =
        with_log(fn ->
          EventRouter.prepare_change_for_shape(
            shape!(where: ~s|"firmId" = 'F0'|),
            change,
            %TransactionFragment{xid: 100},
            nil
          )
        end)

      assert result == nil
      assert log =~ "Shared conversion failed"
      refute log =~ inspect(note)
      refute log =~ "row-value-7f3a"
      assert log =~ "Jason.EncodeError in "
    end
  end

  describe "framed blocks" do
    defp block_router(shapes) do
      Enum.reduce(
        shapes,
        EventRouter.new(shared_conversion: true, fragment_blocks: true),
        fn {id, shape}, router -> EventRouter.add_shape(router, id, shape) end
      )
    end

    defp inserts(n) do
      Enum.map(1..n, fn i ->
        insert(%{"id" => "eq-#{i}", "firmId" => "F0", "divisionId" => "D0", "note" => "n"},
          log_offset: LogOffset.new(Lsn.from_integer(1000), i)
        )
      end)
    end

    test "a class run reaches every member as one shared block plus the trailing change" do
      shapes = class_shapes(3)
      changes = inserts(6)

      {result, _router} =
        EventRouter.event_by_shape_handle(block_router(shapes), fragment(changes))

      assert map_size(result) == 3

      blocks =
        Enum.map(result, fn {_id, frag} ->
          assert [%Changes.FramedBlock{count: 5} = block, %PreparedChange{}] = frag.changes
          assert frag.change_count == 6
          block
        end)

      assert [_one_shared_struct] = Enum.uniq(blocks)

      # The framed bytes must equal the per-entry encoding each member would have produced.
      [{_, some_shape} | _] = Enum.to_list(shapes)

      expected_frame =
        changes
        |> Enum.take(5)
        |> Enum.flat_map(&encode_independently(some_shape, &1, 100))
        |> Enum.map(fn {offset, key, :insert, json} ->
          {iodata, _size} =
            Electric.ShapeCache.PureFileStorage.LogFile.make_entry(
              {offset, byte_size(key), key, ?i, 0, byte_size(json), json}
            )

          IO.iodata_to_binary(iodata)
        end)
        |> IO.iodata_to_binary()

      assert hd(blocks).frame == expected_frame
    end

    test "with blocks disabled the same run stays per-change" do
      shapes = class_shapes(3)

      {result, _router} =
        EventRouter.event_by_shape_handle(router_with(shapes), fragment(inserts(6)))

      Enum.each(result, fn {_id, frag} ->
        refute Enum.any?(frag.changes, &is_struct(&1, Changes.FramedBlock))
        assert length(prepared_changes(frag)) == 6
      end)
    end
  end

  describe "payload arena" do
    @describetag :tmp_dir

    alias Electric.ShapeCache.PureFileStorage.PayloadArena

    defp arena_router(shapes, tmp_dir) do
      Enum.reduce(
        shapes,
        EventRouter.new(
          shared_conversion: true,
          payload_arena: PayloadArena.open_writer!(tmp_dir)
        ),
        fn {id, shape}, router -> EventRouter.add_shape(router, id, shape) end
      )
    end

    test "prepared log items carry refs that resolve to the shared bytes, synced at commit",
         %{tmp_dir: tmp_dir} do
      shapes = class_shapes(4)
      change = insert()

      {result, router} =
        EventRouter.event_by_shape_handle(arena_router(shapes, tmp_dir), fragment([change]))

      assert map_size(result) == 4

      Enum.each(result, fn {shape_id, fragment} ->
        [%PreparedChange{log_items: log_items}] = prepared_changes(fragment)
        expected = encode_independently(Map.fetch!(shapes, shape_id), change, 100)

        without_refs =
          Enum.map(log_items, fn {offset, key, op, json, ref} when byte_size(ref) == 36 ->
            {offset, key, op, json}
          end)

        assert without_refs == expected,
               "arena-mode encoding differs from #{shape_id}'s own encoding"

        resolved =
          log_items
          |> Enum.map(fn {_, _, _, _, ref} -> {:ref, ref} end)
          |> PayloadArena.resolve_refs(tmp_dir)
          |> Enum.to_list()

        assert resolved == Enum.map(expected, fn {_, _, _, json} -> json end),
               "arena payloads differ from #{shape_id}'s own encoding"
      end)

      assert %PayloadArena.Writer{dirty?: false} = router.payload_arena,
             "arena was not synced at the commit fragment"
    end

    test "a fragment without a commit does not sync the arena", %{tmp_dir: tmp_dir} do
      shapes = class_shapes(3)
      no_commit = %{fragment([insert()]) | commit: nil}

      {_result, router} =
        EventRouter.event_by_shape_handle(arena_router(shapes, tmp_dir), no_commit)

      assert %PayloadArena.Writer{dirty?: true} = router.payload_arena,
             "mid-transaction fragments must not pay a datasync"
    end

    # The rescue in `prepare_change_for_shape/4` is the reason one unencodable change cannot take
    # down every shape on the node, and nothing exercised it. A dead arena handle is a real IO
    # failure raised from inside the encode, which is the shape of the thing it has to survive --
    # if that arrived as an exit instead, the rescue would not catch it and the blast radius the
    # comment promises would be wrong.
    test "an arena write failure falls the class back to per-shape conversion",
         %{tmp_dir: tmp_dir} do
      shapes = class_shapes(4)
      router = arena_router(shapes, tmp_dir)
      File.close(router.payload_arena.file)

      # Big enough to pass the writer's buffer threshold, so the append flushes to the dead handle
      # rather than sitting in memory.
      change = insert(%{record() | "note" => String.duplicate("x", 600 * 1024)})

      {{result, router}, log} =
        with_log(fn -> EventRouter.event_by_shape_handle(router, fragment([change])) end)

      assert map_size(result) == 4, "the fallback must still deliver the change to every shape"

      assert log =~ "Shared conversion failed",
             "a dead arena handle never raises again once the writer is retained, so this line is " <>
               "the only report that the fork stopped doing what it exists to do"

      Enum.each(result, fn {shape_id, fragment} ->
        assert prepared_changes(fragment) == [],
               "#{shape_id} kept a prepared change built on a failed arena write"

        assert [%Changes.NewRecord{}] = fragment.changes
      end)

      assert %PayloadArena.Writer{position: 0} = router.payload_arena,
             "the partially-appended writer was retained, so later refs would be positioned past " <>
               "the bytes actually on disk"
    end

    test "a single-shape class stays unprepared and appends nothing to the arena",
         %{tmp_dir: tmp_dir} do
      shapes = class_shapes(1)

      {result, router} =
        EventRouter.event_by_shape_handle(arena_router(shapes, tmp_dir), fragment([insert()]))

      assert [fragment] = Map.values(result)
      assert prepared_changes(fragment) == []
      assert %PayloadArena.Writer{position: 0} = router.payload_arena
    end
  end
end
