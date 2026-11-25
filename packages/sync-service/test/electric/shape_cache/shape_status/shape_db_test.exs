defmodule Electric.ShapeCache.ShapeStatus.ShapeDbTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit

  alias Electric.Shapes.Shape
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer

  import ExUnit.CaptureLog, only: [with_log: 1]
  import Support.ComponentSetup

  @moduletag :tmp_dir

  @stub_inspector Support.StubInspector.new(
                    tables: [{1, {"public", "items"}}, {2, {"public", "other_table"}}],
                    columns: [
                      %{
                        name: "id",
                        type: "int8",
                        type_id: {20, 1},
                        pk_position: 0,
                        is_generated: false
                      },
                      %{name: "value", type: "text", type_id: {25, 1}, is_generated: false}
                    ]
                  )

  setup [:with_stack_id_from_test, :with_shape_db]

  test "add_shape inserts persistent shape data", ctx do
    assert {:ok, []} = ShapeDb.list_shapes(ctx.stack_id)
    shape1 = Shape.new!("items", inspector: @stub_inspector)
    handle1 = "handle-1"

    {:ok, _hash1} = ShapeDb.add_shape(ctx.stack_id, shape1, handle1)

    assert {:ok, [{^handle1, ^shape1}]} = ShapeDb.list_shapes(ctx.stack_id)

    shape2 = Shape.new!("items", inspector: @stub_inspector, where: "id = 1")
    handle2 = "handle-2"

    {:ok, _hash2} = ShapeDb.add_shape(ctx.stack_id, shape2, handle2)

    assert {:ok, [{^handle1, ^shape1}, {^handle2, ^shape2}]} = ShapeDb.list_shapes(ctx.stack_id)
  end

  test "flushing a removed handle's add leaves the comparable index on the handle that holds it",
       ctx do
    # second add is in flight: claimed by a drain that has not written it yet, which is the only
    # state where the index entry is the sole way to find the handle.
    shape = Shape.new!("items", inspector: @stub_inspector)
    ops_table = WriteBuffer.operations_table_name(ctx.stack_id)

    {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-removed")
    :ok = ShapeDb.remove_shape(ctx.stack_id, "handle-removed")
    {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-current")
    true = :ets.update_element(ops_table, :ets.last(ops_table), {3, true})

    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    assert {:ok, "handle-current"} = ShapeDb.handle_for_shape(ctx.stack_id, shape)
  end

  test "handle_exists?", ctx do
    assert {:ok, []} = ShapeDb.list_shapes(ctx.stack_id)
    shape1 = Shape.new!("items", inspector: @stub_inspector)
    handle1 = "handle-1"

    refute ShapeDb.handle_exists?(ctx.stack_id, handle1)
    {:ok, _hash1} = ShapeDb.add_shape(ctx.stack_id, shape1, handle1)
    assert ShapeDb.handle_exists?(ctx.stack_id, handle1)
  end

  test "shape_handles_for_relations", ctx do
    shape1 = Shape.new!("items", inspector: @stub_inspector)
    handle1 = "handle-1"
    {:ok, _hash1} = ShapeDb.add_shape(ctx.stack_id, shape1, handle1)

    shape2 = Shape.new!("items", inspector: @stub_inspector, where: "id = 1")
    handle2 = "handle-2"
    {:ok, _hash2} = ShapeDb.add_shape(ctx.stack_id, shape2, handle2)

    shape3 = Shape.new!("other_table", inspector: @stub_inspector)
    handle3 = "handle-3"
    {:ok, _hash3} = ShapeDb.add_shape(ctx.stack_id, shape3, handle3)

    assert {:ok, [^handle1, ^handle2]} =
             ShapeDb.shape_handles_for_relations(ctx.stack_id, [{1, {"public", "items"}}])

    assert {:ok, [^handle3]} =
             ShapeDb.shape_handles_for_relations(ctx.stack_id, [{2, {"public", "other_table"}}])

    assert {:ok, [^handle1, ^handle2, ^handle3]} =
             ShapeDb.shape_handles_for_relations(ctx.stack_id, [
               {1, {"public", "items"}},
               {2, {"public", "other_table"}}
             ])
  end

  test "remove_shape", ctx do
    shape1 = Shape.new!("items", inspector: @stub_inspector)
    handle1 = "handle-1"
    {:ok, _hash1} = ShapeDb.add_shape(ctx.stack_id, shape1, handle1)

    shape2 = Shape.new!("items", inspector: @stub_inspector, where: "id = 1")
    handle2 = "handle-2"
    {:ok, _hash2} = ShapeDb.add_shape(ctx.stack_id, shape2, handle2)

    shape3 = Shape.new!("other_table", inspector: @stub_inspector)
    handle3 = "handle-3"
    {:ok, _hash3} = ShapeDb.add_shape(ctx.stack_id, shape3, handle3)

    assert {:ok, [{^handle1, ^shape1}, {^handle2, ^shape2}, {^handle3, ^shape3}]} =
             ShapeDb.list_shapes(ctx.stack_id)

    :ok = ShapeDb.remove_shape(ctx.stack_id, handle1)

    assert {:ok, [{^handle2, ^shape2}, {^handle3, ^shape3}]} = ShapeDb.list_shapes(ctx.stack_id)

    assert {:ok, [^handle2, ^handle3]} =
             ShapeDb.shape_handles_for_relations(ctx.stack_id, [
               {1, {"public", "items"}},
               {2, {"public", "other_table"}}
             ])

    :ok = ShapeDb.remove_shape(ctx.stack_id, handle3)

    assert {:ok, [{^handle2, ^shape2}]} = ShapeDb.list_shapes(ctx.stack_id)

    assert {:ok, [^handle2]} =
             ShapeDb.shape_handles_for_relations(ctx.stack_id, [
               {1, {"public", "items"}},
               {2, {"public", "other_table"}}
             ])

    :ok = ShapeDb.remove_shape(ctx.stack_id, handle2)

    assert {:ok, []} =
             ShapeDb.shape_handles_for_relations(ctx.stack_id, [
               {1, {"public", "items"}},
               {2, {"public", "other_table"}}
             ])

    assert {:ok, []} = ShapeDb.list_shapes(ctx.stack_id)
  end

  test "remove non-existing shape", ctx do
    shape1 = Shape.new!("items", inspector: @stub_inspector)
    handle1 = "handle-1"
    {:ok, _hash1} = ShapeDb.add_shape(ctx.stack_id, shape1, handle1)
    assert {:ok, 1} = ShapeDb.count_shapes(ctx.stack_id)

    assert {:error, {:enoshape, "no-such-handle"}} =
             ShapeDb.remove_shape(ctx.stack_id, "no-such-handle")

    assert {:ok, 1} = ShapeDb.count_shapes(ctx.stack_id)
  end

  test "handle_for_shape/2", ctx do
    shape1 = Shape.new!("items", inspector: @stub_inspector)
    handle1 = "handle-1"
    {:ok, _hash1} = ShapeDb.add_shape(ctx.stack_id, shape1, handle1)
    shape2 = Shape.new!("items", inspector: @stub_inspector, where: "id = 99")

    assert {:ok, ^handle1} = ShapeDb.handle_for_shape(ctx.stack_id, shape1)

    assert :error = ShapeDb.handle_for_shape(ctx.stack_id, shape2)
  end

  test "handle_for_shape_critical/2", ctx do
    shape1 = Shape.new!("items", inspector: @stub_inspector)
    handle1 = "handle-1"
    {:ok, _hash1} = ShapeDb.add_shape(ctx.stack_id, shape1, handle1)
    shape2 = Shape.new!("items", inspector: @stub_inspector, where: "id = 99")

    assert {:ok, ^handle1} = ShapeDb.handle_for_shape_critical(ctx.stack_id, shape1)

    assert :error = ShapeDb.handle_for_shape_critical(ctx.stack_id, shape2)
  end

  test "shape_for_handle", ctx do
    shape1 = Shape.new!("items", inspector: @stub_inspector)
    handle1 = "handle-1"
    {:ok, _hash1} = ShapeDb.add_shape(ctx.stack_id, shape1, handle1)

    assert {:ok, ^shape1} = ShapeDb.shape_for_handle(ctx.stack_id, handle1)
    assert :error = ShapeDb.shape_for_handle(ctx.stack_id, "no-such-handle")
  end

  # A shape is stored compressed, and a shape stored before, as is, reads back the same.
  test "stores a shape compressed and reads shapes stored either way", ctx do
    shape1 = Shape.new!("items", inspector: @stub_inspector, where: "id = 1")
    shape2 = Shape.new!("items", inspector: @stub_inspector, where: "id = 2")
    {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape1, "handle-1")
    {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape2, "handle-2")
    WriteBuffer.flush_sync(ctx.stack_id)

    # the external term format's version, then 80 for a compressed term
    assert [<<131, 80, _::binary>>, <<131, 80, _::binary>>] = stored_blobs(ctx)

    stored_before = :erlang.term_to_binary(shape2, [:deterministic])

    ShapeDb.Connection.checkout_write!(ctx.stack_id, :test, fn %{conn: conn} ->
      {:ok, []} =
        ShapeDb.Connection.fetch_all(conn, "UPDATE shapes SET shape = ?1 WHERE handle = ?2", [
          {:blob, stored_before},
          {:blob, "handle-2"}
        ])
    end)

    assert [<<131, 80, _::binary>>, ^stored_before] = stored_blobs(ctx)

    assert {:ok, ^shape1} = ShapeDb.shape_for_handle(ctx.stack_id, "handle-1")
    assert {:ok, ^shape2} = ShapeDb.shape_for_handle(ctx.stack_id, "handle-2")

    assert ShapeDb.reduce_shapes(ctx.stack_id, [], fn entry, acc -> [entry | acc] end)
           |> Enum.sort() == [{"handle-1", shape1}, {"handle-2", shape2}]

    assert [{"handle-1", _, encoded1}, {"handle-2", _, encoded2}] =
             read_encoded_shapes(ctx.stack_id)

    assert {:erlang.binary_to_term(encoded1), :erlang.binary_to_term(encoded2)} ==
             {shape1, shape2}
  end

  test "reduce_shapes/3", ctx do
    {handles, _shapes} =
      Enum.map(1..100, fn n ->
        shape = Shape.new!("items", inspector: @stub_inspector, where: "id = #{n}")
        handle = "handle-#{n}"
        {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
        {handle, shape}
      end)
      |> Enum.unzip()

    assert ShapeDb.reduce_shapes(
             ctx.stack_id,
             MapSet.new(),
             fn {handle, %Shape{} = _shape}, acc -> MapSet.put(acc, handle) end
           ) == MapSet.new(handles)
  end

  for exclusive_mode <- [false, true] do
    describe "reduce_encoded_shapes/3 (exclusive_mode: #{exclusive_mode})" do
      @describetag shape_db_opts: [exclusive_mode: exclusive_mode]

      test "reads every stored shape once, in handle order, as stored, over several pages", ctx do
        # handles SQLite and Erlang must order alike: a prefix before its extensions, zero bytes,
        # bytes past ASCII
        odd = ["a", "a" <> <<0>>, "a" <> <<0, 1>>, "ab", <<0xFF>>, "é", "Z", <<0>>]
        handles = Enum.map(1..250, &"handle-#{&1}") ++ odd

        shapes =
          for {handle, n} <- Enum.with_index(handles), into: %{} do
            table = if rem(n, 2) == 0, do: "items", else: "other_table"
            shape = Shape.new!(table, inspector: @stub_inspector, where: "id = #{n}")
            {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
            {handle, shape}
          end

        WriteBuffer.flush_sync(ctx.stack_id)

        read = read_encoded_shapes(ctx.stack_id)

        assert Enum.map(read, &elem(&1, 0)) == Enum.sort(handles)

        for {handle, oid, encoded} <- read do
          shape = Map.fetch!(shapes, handle)
          assert {^oid, ^shape} = {shape.root_table_id, :erlang.binary_to_term(encoded)}
        end
      end

      test "gives a shape the store files under no table a nil oid", ctx do
        shape = Shape.new!("items", inspector: @stub_inspector, where: "id = 1")
        {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-1")
        WriteBuffer.flush_sync(ctx.stack_id)

        ShapeDb.Connection.checkout_write!(ctx.stack_id, :test, fn %{conn: conn} ->
          ShapeDb.Connection.execute(conn, "DELETE FROM relations")
        end)

        assert [{"handle-1", nil, encoded}] = read_encoded_shapes(ctx.stack_id)
        assert :erlang.binary_to_term(encoded) == shape
      end

      test "a raise in the reducer leaves the connection free for the next read", ctx do
        for n <- 1..250 do
          shape = Shape.new!("items", inspector: @stub_inspector, where: "id = #{n}")
          {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-#{n}")
        end

        WriteBuffer.flush_sync(ctx.stack_id)

        assert_raise RuntimeError, "boom on the second page", fn ->
          ShapeDb.reduce_encoded_shapes(ctx.stack_id, 0, fn
            _entry, 150 -> raise "boom on the second page"
            _entry, n -> n + 1
          end)
        end

        assert length(read_encoded_shapes(ctx.stack_id)) == 250
      end
    end
  end

  @tag shape_db_opts: [exclusive_mode: false]
  test "reduce_encoded_shapes/3 reads the store as it was when the read began", ctx do
    for n <- 1..150 do
      shape = Shape.new!("items", inspector: @stub_inspector, where: "id = #{n}")
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-#{n}")
    end

    WriteBuffer.flush_sync(ctx.stack_id)

    # a shape committed while the read is on its first page, on a handle its second page reaches
    late = Shape.new!("items", inspector: @stub_inspector, where: "id = 0")

    read =
      ShapeDb.reduce_encoded_shapes(ctx.stack_id, [], fn
        {handle, _oid, _encoded}, [] ->
          {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, late, "handle-late")
          WriteBuffer.flush_sync(ctx.stack_id)
          [handle]

        {handle, _oid, _encoded}, acc ->
          [handle | acc]
      end)

    assert length(read) == 150
    refute "handle-late" in read
    assert {:ok, ^late} = ShapeDb.shape_for_handle(ctx.stack_id, "handle-late")
  end

  defp stored_blobs(ctx) do
    {:ok, rows} =
      ShapeDb.Connection.checkout!(ctx.stack_id, :test, fn %{conn: conn} ->
        ShapeDb.Connection.fetch_all(conn, "SELECT shape FROM shapes ORDER BY handle", [])
      end)

    Enum.map(rows, fn [blob] -> blob end)
  end

  defp read_encoded_shapes(stack_id) do
    stack_id
    |> ShapeDb.reduce_encoded_shapes([], fn entry, acc -> [entry | acc] end)
    |> Enum.reverse()
  end

  test "reduce_shape_meta/3", ctx do
    expected =
      Enum.map(1..100, fn n ->
        shape = Shape.new!("items", inspector: @stub_inspector, where: "id = #{n}")
        handle = "handle-#{n}"
        {:ok, hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)

        # Mark some shapes as snapshot_complete
        if rem(n, 2) == 0 do
          :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, handle)
          {handle, hash, true}
        else
          {handle, hash, false}
        end
      end)

    # reduce_shape_meta reads snapshot state from SQLite, so flush first
    ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)

    assert ShapeDb.reduce_shape_meta(
             ctx.stack_id,
             MapSet.new(),
             fn {handle, hash, snapshot_complete}, acc ->
               MapSet.put(acc, {handle, hash, snapshot_complete})
             end
           ) == MapSet.new(expected)
  end

  test "reduce_shape_meta surfaces a reducer error instead of an after_fun crash", ctx do
    # stream. reduce_shape_meta reads from SQLite, so flush the buffer first.
    shape = Shape.new!("items", inspector: @stub_inspector, where: "id = 1")
    {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-1")
    ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)

    # during the abnormal unwind holding the 3-tuple continuation accumulator. A 2-tuple-only
    # after_fun clause would raise FunctionClauseError there and swallow this error, so the caller
    # must see the reducer's own exception for the read failure to be classifiable.
    assert_raise RuntimeError, "boom mid-reduce", fn ->
      ShapeDb.reduce_shape_meta(ctx.stack_id, MapSet.new(), fn _meta, _acc ->
        raise "boom mid-reduce"
      end)
    end
  end

  test "count_shapes/1", ctx do
    assert {:ok, 0} = ShapeDb.count_shapes(ctx.stack_id)

    Enum.each(1..100, fn n ->
      shape = Shape.new!("items", inspector: @stub_inspector, where: "id = #{n}")
      handle = "handle-#{n}"
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
      assert {:ok, n} == ShapeDb.count_shapes(ctx.stack_id)
    end)

    Enum.each(100..1//-1, fn n ->
      handle = "handle-#{n}"
      :ok = ShapeDb.remove_shape(ctx.stack_id, handle)
      assert {:ok, n - 1} == ShapeDb.count_shapes(ctx.stack_id)
    end)

    assert {:ok, 0} = ShapeDb.count_shapes(ctx.stack_id)
  end

  test "mark_snapshot_complete/2", ctx do
    assert :error = ShapeDb.mark_snapshot_complete(ctx.stack_id, "no-such-handle")

    shape1 = Shape.new!("items", inspector: @stub_inspector)
    handle1 = "handle-1"
    {:ok, _hash1} = ShapeDb.add_shape(ctx.stack_id, shape1, handle1)

    # should allow for marking a snapshot complete before the snapshot
    # has been marked started
    assert :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, handle1)

    shape2 = Shape.new!("items", inspector: @stub_inspector, where: "id = 2")
    handle2 = "handle-2"
    {:ok, _hash2} = ShapeDb.add_shape(ctx.stack_id, shape2, handle2)

    assert :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, handle2)
  end

  defp make_valid_shape(ctx, shape, handle) do
    make_shape_with_snapshot_status(ctx, shape, handle, snapshot_complete: true)
  end

  defp make_shape_with_snapshot_status(%{stack_id: stack_id}, shape, handle, opts \\ []) do
    snapshot_complete? = Keyword.get(opts, :snapshot_complete, false)

    {:ok, _hash1} = ShapeDb.add_shape(stack_id, shape, handle)

    if snapshot_complete?,
      do: :ok = ShapeDb.mark_snapshot_complete(stack_id, handle)

    {handle, shape}
  end

  defp get_snapshot_states(stack_id) do
    ShapeDb.reduce_shape_meta(stack_id, %{}, fn {handle, _hash, complete}, acc ->
      Map.put(acc, handle, complete)
    end)
  end

  defp flush_before_first_tombstone_check do
    Repatch.patch(WriteBuffer, :is_tombstoned?, fn stack_id, handle ->
      if Process.put(:tombstone_checks, (Process.get(:tombstone_checks) || 0) + 1) == nil,
        do: :ok = WriteBuffer.flush_sync(stack_id)

      Repatch.real(WriteBuffer.is_tombstoned?(stack_id, handle))
    end)
  end

  defp remove_and_flush_before_second_tombstone_check(removed_handle) do
    Repatch.patch(WriteBuffer, :is_tombstoned?, fn stack_id, handle ->
      if Process.put(:tombstone_checks, (Process.get(:tombstone_checks) || 0) + 1) == 1 do
        :ok = ShapeDb.remove_shape(stack_id, removed_handle)
        :ok = WriteBuffer.flush_sync(stack_id)
      end

      Repatch.real(WriteBuffer.is_tombstoned?(stack_id, handle))
    end)
  end

  test "validate_existing_shapes/1", ctx do
    valid_shapes =
      Enum.map(1..10, fn n ->
        shape = Shape.new!("items", inspector: @stub_inspector, where: "id = #{n}")
        handle = "handle-#{n}"
        make_valid_shape(ctx, shape, handle)
      end)

    not_completed =
      Enum.map(21..30, fn n ->
        shape = Shape.new!("items", inspector: @stub_inspector, where: "id = #{n}")
        handle = "handle-#{n}"
        make_shape_with_snapshot_status(ctx, shape, handle)
      end)

    {remove_handles, _shapes} = Enum.unzip(not_completed)

    {:ok, invalid_handles, 10} = ShapeDb.validate_existing_shapes(ctx.stack_id)

    assert MapSet.new(invalid_handles) == MapSet.new(remove_handles)

    {handles, _shapes} = Enum.unzip(valid_shapes)

    assert ShapeDb.reduce_shapes(
             ctx.stack_id,
             MapSet.new(),
             fn {handle, %Shape{} = _shape}, acc -> MapSet.put(acc, handle) end
           ) == MapSet.new(handles)
  end

  test "reset/1", ctx do
    assert {:ok, 0} = ShapeDb.count_shapes(ctx.stack_id)

    Enum.map(1..10, fn n ->
      shape = Shape.new!("items", inspector: @stub_inspector, where: "id = #{n}")
      handle = "handle-#{n}"
      make_valid_shape(ctx, shape, handle)
    end)

    assert {:ok, 10} = ShapeDb.count_shapes(ctx.stack_id)

    assert :ok = ShapeDb.reset(ctx.stack_id)

    assert ShapeDb.reduce_shapes(ctx.stack_id, 0, fn {_handle, %Shape{} = _shape}, acc ->
             acc + 1
           end) == 0

    assert {:ok, 0} = ShapeDb.count_shapes(ctx.stack_id)
  end

  describe "write buffer" do
    test "shapes visible while buffered and after flush", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      handle = "handle-1"

      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)

      assert {:ok, [{^handle, ^shape}]} = ShapeDb.list_shapes(ctx.stack_id)
      assert {:ok, ^shape} = ShapeDb.shape_for_handle(ctx.stack_id, handle)
      assert {:ok, ^handle} = ShapeDb.handle_for_shape(ctx.stack_id, shape)

      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)

      assert {:ok, [{^handle, ^shape}]} = ShapeDb.list_shapes(ctx.stack_id)
      assert {:ok, ^shape} = ShapeDb.shape_for_handle(ctx.stack_id, handle)
      assert {:ok, ^handle} = ShapeDb.handle_for_shape(ctx.stack_id, shape)
    end

    test "snapshot functions work on shapes only in SQLite", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      handle = "handle-1"

      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)

      # Verify initial state: not complete
      assert %{^handle => false} = get_snapshot_states(ctx.stack_id)

      assert :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, handle)
      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)

      # Verify snapshot complete
      assert %{^handle => true} = get_snapshot_states(ctx.stack_id)
    end

    test "snapshot functions fail for shapes pending removal", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      handle = "handle-1"

      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)

      assert :ok = ShapeDb.remove_shape(ctx.stack_id, handle)

      assert :error = ShapeDb.mark_snapshot_complete(ctx.stack_id, handle)
    end

    test "remove cancels buffered add", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      handle = "handle-1"

      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
      assert {:ok, [{^handle, ^shape}]} = ShapeDb.list_shapes(ctx.stack_id)

      assert :ok = ShapeDb.remove_shape(ctx.stack_id, handle)
      assert {:ok, []} = ShapeDb.list_shapes(ctx.stack_id)

      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
      assert {:ok, []} = ShapeDb.list_shapes(ctx.stack_id)
    end

    test "handle_exists? respects pending removes", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      handle = "handle-1"

      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
      assert ShapeDb.handle_exists?(ctx.stack_id, handle)

      assert :ok = ShapeDb.remove_shape(ctx.stack_id, handle)
      refute ShapeDb.handle_exists?(ctx.stack_id, handle)
    end

    # The tombstone is checked after the SQLite read, so a flush landing between the two commits
    # the delete and clears the tombstone that would have caught the row the read already returned.
    test "handle_for_shape does not return a handle whose removal commits during its read", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      handle = "handle-1"

      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      :ok = ShapeDb.remove_shape(ctx.stack_id, handle)
      flush_before_first_tombstone_check()

      assert :error = ShapeDb.handle_for_shape(ctx.stack_id, shape)
    end

    test "shape_for_handle does not return a shape whose removal commits during its read", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      handle = "handle-1"

      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      remove_and_flush_before_second_tombstone_check(handle)

      assert :error = ShapeDb.shape_for_handle(ctx.stack_id, handle)
    end

    test "pending_count_diff/1", ctx do
      assert 0 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)

      shape = Shape.new!("items", inspector: @stub_inspector)
      handle = "handle-0"

      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)

      assert 1 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)

      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
      assert 0 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)

      assert :ok = ShapeDb.remove_shape(ctx.stack_id, handle)

      assert -1 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)

      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
      assert 0 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)

      {_handles1, _shapes} =
        Enum.map(1..10, fn n ->
          shape = Shape.new!("items", inspector: @stub_inspector, where: "id = #{n}")
          handle = "handle-#{n}"

          make_shape_with_snapshot_status(ctx, shape, handle, snapshot_complete: false)
        end)
        |> Enum.unzip()

      {handles2, _shapes} =
        Enum.map(11..20, fn n ->
          shape = Shape.new!("items", inspector: @stub_inspector, where: "id = #{n}")
          handle = "handle-#{n}"

          make_shape_with_snapshot_status(ctx, shape, handle, snapshot_complete: false)
        end)
        |> Enum.unzip()

      assert 20 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)

      Enum.each(handles2, &ShapeDb.remove_shape(ctx.stack_id, &1))

      assert 10 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)

      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)

      assert 0 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)
    end

    test "duplicate removal operations do not block the write queue", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      handle = "handle-0"

      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)

      assert 1 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)

      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
      assert 0 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)
      assert :ok = ShapeDb.remove_shape(ctx.stack_id, handle)

      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
      assert 0 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)

      ShapeDb.WriteBuffer.remove_shape(ctx.stack_id, handle)
      assert -1 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)

      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
      assert 0 = ShapeDb.WriteBuffer.pending_count_diff(ctx.stack_id)
    end

    # A process stopped between marking a removal and queuing it leaves the mark alone.
    test "queue_marked_removal queues, once, the removal a mark was left without", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      handle = "handle-marked"
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      true =
        :ets.insert_new(
          WriteBuffer.shapes_table_name(ctx.stack_id),
          {{:tombstone, handle}, WriteBuffer.op_key()}
        )

      assert WriteBuffer.queue_marked_removal(ctx.stack_id, handle)
      assert WriteBuffer.queue_marked_removal(ctx.stack_id, handle)
      assert 1 = WriteBuffer.pending_operations_count(ctx.stack_id)
      assert -1 = WriteBuffer.pending_count_diff(ctx.stack_id)

      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      refute WriteBuffer.is_tombstoned?(ctx.stack_id, handle)
      refute ShapeDb.handle_exists?(ctx.stack_id, handle)
      assert 0 = WriteBuffer.pending_count_diff(ctx.stack_id)
      refute WriteBuffer.queue_marked_removal(ctx.stack_id, handle)
    end

    # `queue_marked_removal/2` queued the removal between the mark and its queuing here, and a flush
    # has taken it up since.
    test "remove_shape leaves a removal queued under its mark as it is", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      handle = "handle-raced"
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      :ok = WriteBuffer.pause_flush(ctx.stack_id)

      key = WriteBuffer.op_key()
      queued = {key, {:remove, handle}, true, 2}
      ops_table = WriteBuffer.operations_table_name(ctx.stack_id)
      true = :ets.insert_new(ops_table, queued)
      :ets.update_counter(WriteBuffer.shapes_table_name(ctx.stack_id), :count_remove, 1)
      Repatch.patch(WriteBuffer, :op_key, fn -> key end)

      assert :ok = WriteBuffer.remove_shape(ctx.stack_id, handle)
      assert [^queued] = :ets.lookup(ops_table, key)
      assert -1 = WriteBuffer.pending_count_diff(ctx.stack_id)
    end

    test "failing to mark a snapshot completed does not block the write queue", ctx do
      handle = "handle-0"
      ShapeDb.WriteBuffer.queue_snapshot_complete(ctx.stack_id, handle)
      assert 1 = ShapeDb.WriteBuffer.pending_operations_count(ctx.stack_id)
      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
      assert 0 = ShapeDb.WriteBuffer.pending_operations_count(ctx.stack_id)
    end

    test "dead-letters a poisoned operation so the queue drains instead of wedging forever",
         ctx do
      # persistently-raising batch (a constraint violation, a full disk, a corrupt db). It
      # is seeded one failed flush short of the attempt cap.
      ops_table = WriteBuffer.operations_table_name(ctx.stack_id)

      poison =
        {WriteBuffer.op_key(), {:poison, "handle-x"}, false, WriteBuffer.max_flush_attempts() - 1}

      true = :ets.insert(ops_table, poison)
      assert 1 = WriteBuffer.pending_operations_count(ctx.stack_id)

      {_result, log} = with_log(fn -> WriteBuffer.flush_sync(ctx.stack_id) end)

      assert 0 = WriteBuffer.pending_operations_count(ctx.stack_id)
      assert log =~ "dead-lettered"
    end

    test "dead-lettering a failed :remove retires its tombstone and pending-count bookkeeping",
         ctx do
      # in a batch that keeps raising (a full disk, a corrupt db), one failed flush short of the cap.
      ops_table = WriteBuffer.operations_table_name(ctx.stack_id)
      handle = "handle-stuck"

      :ok = WriteBuffer.remove_shape(ctx.stack_id, handle)
      assert WriteBuffer.is_tombstoned?(ctx.stack_id, handle)
      assert -1 = WriteBuffer.pending_count_diff(ctx.stack_id)

      [[remove_ts]] = :ets.match(ops_table, {:"$1", {:remove, handle}, :_, :_})
      true = :ets.update_element(ops_table, remove_ts, {4, WriteBuffer.max_flush_attempts() - 1})

      true =
        :ets.insert(
          ops_table,
          {WriteBuffer.op_key(), {:poison, "handle-poison"}, false,
           WriteBuffer.max_flush_attempts() - 1}
        )

      {_result, log} = with_log(fn -> WriteBuffer.flush_sync(ctx.stack_id) end)

      assert log =~ "dead-lettered"
      assert 0 = WriteBuffer.pending_operations_count(ctx.stack_id)
      refute WriteBuffer.is_tombstoned?(ctx.stack_id, handle)
      assert 0 = WriteBuffer.pending_count_diff(ctx.stack_id)
    end

    # The drain's caller stops waiting soon after its deadline: a drain the buffer only reaches
    # later, behind a long flush, would hold the buffer up for a caller that has gone on.
    test "a drain starts no batch after its deadline", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-1")
      buffer = GenServer.whereis(WriteBuffer.name(ctx.stack_id))

      assert {:error, :drain_deadline} =
               GenServer.call(buffer, {:drain, System.monotonic_time(:millisecond) - 1})

      assert 1 = WriteBuffer.pending_operations_count(ctx.stack_id)

      assert :ok = WriteBuffer.drain(ctx.stack_id)
      assert 0 = WriteBuffer.pending_operations_count(ctx.stack_id)

      # Nothing left to give up: an expired drain of an empty queue succeeds, and starts no batch.
      assert :ok = GenServer.call(buffer, {:drain, System.monotonic_time(:millisecond) - 1})
    end

    test "a drain whose deadline passes during a batch starts no other", ctx do
      # More than one batch: a drain commits at most 100 operations at a time.
      for i <- 1..150 do
        shape = Shape.new!("items", inspector: @stub_inspector, where: "id = #{i}")
        {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-#{i}")
      end

      # The first batch starts before the deadline, a receive timeout out, and takes the write
      # connection only once it has passed.
      test_pid = self()
      window = ExUnit.configuration()[:assert_receive_timeout]
      deadline = System.monotonic_time(:millisecond) + window

      Repatch.patch(
        ShapeDb.Connection,
        :checkout_write!,
        [mode: :shared],
        fn stack_id, label, fun, timeout ->
          if is_nil(Process.put(:held, true)) do
            send(test_pid, {:batch_started, self()})
            Process.sleep(max(deadline - System.monotonic_time(:millisecond), 0) + 1)
          end

          Repatch.real(ShapeDb.Connection.checkout_write!(stack_id, label, fun, timeout))
        end
      )

      buffer = GenServer.whereis(WriteBuffer.name(ctx.stack_id))
      Repatch.allow(self(), buffer)
      drain = Task.async(fn -> GenServer.call(buffer, {:drain, deadline}, :infinity) end)

      assert_receive {:batch_started, ^buffer}
      assert {:error, :drain_deadline} = Task.await(drain, 2 * window)
      assert 50 = WriteBuffer.pending_operations_count(ctx.stack_id)
    end
  end

  describe "exclusive mode" do
    @describetag shape_db_opts: [exclusive_mode: true]

    @tag shape_db_opts: [exclusive_mode: false]
    test "when disabled read and write connections are different", ctx do
      assert {:ok, read_conn} =
               ShapeDb.Connection.checkout!(ctx.stack_id, :read_call, fn %{conn: conn} ->
                 {:ok, conn}
               end)

      assert {:ok, write_conn} =
               ShapeDb.Connection.checkout_write!(ctx.stack_id, :write_call, fn %{conn: conn} ->
                 {:ok, conn}
               end)

      refute read_conn == write_conn

      assert {:ok, [path]} =
               ShapeDb.Connection.checkout!(ctx.stack_id, :read_call, fn %{conn: conn} ->
                 ShapeDb.Connection.fetch_one(conn, "SELECT file FROM pragma_database_list", [])
               end)

      assert path =~ ~r/meta\/shape-db/
    end

    test "returns the same connection for both read and write calls", ctx do
      assert {:ok, read_conn} =
               ShapeDb.Connection.checkout!(ctx.stack_id, :read_call, fn %{conn: conn} ->
                 {:ok, conn}
               end)

      assert {:ok, write_conn} =
               ShapeDb.Connection.checkout_write!(ctx.stack_id, :write_call, fn %{conn: conn} ->
                 {:ok, conn}
               end)

      assert read_conn == write_conn
    end

    @tag shape_db_opts: [exclusive_mode: false]
    test "when disabled sets journal_mode=WAL", ctx do
      assert {:ok, ["wal"]} =
               ShapeDb.Connection.checkout!(ctx.stack_id, :read_call, fn %{conn: conn} ->
                 ShapeDb.Connection.fetch_one(conn, "PRAGMA journal_mode", [])
               end)
    end

    test "sets journal_mode=DELETE", ctx do
      assert {:ok, ["delete"]} =
               ShapeDb.Connection.checkout!(ctx.stack_id, :read_call, fn %{conn: conn} ->
                 ShapeDb.Connection.fetch_one(conn, "PRAGMA journal_mode", [])
               end)
    end

    test "includes read-mode queries", ctx do
      assert {:ok, 0} = ShapeDb.count_shapes(ctx.stack_id)
    end

    @tag shape_db_opts: [exclusive_mode: true, write_checkout_timeout: 50]
    test "write buffer outlives a reader holding the connection past the checkout timeout",
         ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")
      buffer_ref = ctx.stack_id |> WriteBuffer.name() |> GenServer.whereis() |> Process.monitor()
      test_pid = self()

      reader =
        Task.async(fn ->
          ShapeDb.Connection.checkout!(ctx.stack_id, :slow_read, fn _conn ->
            send(test_pid, :connection_held)

            receive do
              :release -> :ok
            end
          end)
        end)

      assert_receive :connection_held

      assert {:error, :checkout_timeout} = WriteBuffer.flush_sync(ctx.stack_id)

      refute_received {:DOWN, ^buffer_ref, :process, _pid, _reason}

      assert [{_ts, {:add, "handle-0", _shape, _comparable, _hash, _relations}, false, 0}] =
               :ets.tab2list(WriteBuffer.operations_table_name(ctx.stack_id))

      send(reader.pid, :release)
      :ok = Task.await(reader)

      assert :ok = WriteBuffer.flush_sync(ctx.stack_id)
      assert 0 = WriteBuffer.pending_operations_count(ctx.stack_id)
      assert {:ok, 1} = ShapeDb.count_shapes(ctx.stack_id)
    end

    # An instance waiting for the lock on the same storage runs its boot `quick_check` first, and
    # in rollback-journal mode that read holds a shared lock on the file for as long as it runs.
    # Every commit here waits out the busy timeout and fails, flush after flush. That is the same
    # contention as a busy connection, so it must not spend the write's attempts and drop it, nor
    # close the only connection: reopening it reruns the integrity check on this instance.
    test "write buffer keeps a write that another connection's read lock delays", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")

      # Exclusive mode has one connection for reads and writes, and a read checkout takes no lock.
      write_conn = fn ->
        ShapeDb.Connection.checkout!(ctx.stack_id, :read_call, fn %{conn: conn} -> {:ok, conn} end)
      end

      {:ok, conn_before} = write_conn.()

      ops_table = WriteBuffer.operations_table_name(ctx.stack_id)
      [[ts]] = :ets.match(ops_table, {:"$1", {:add, "handle-0", :_, :_, :_, :_}, :_, :_})
      true = :ets.update_element(ops_table, ts, {4, WriteBuffer.max_flush_attempts() - 1})

      {:ok, reader} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(reader, "BEGIN")
      {:ok, [_count]} = ShapeDb.Connection.fetch_one(reader, "SELECT count(*) FROM shapes", [])

      {result, log} = with_log(fn -> WriteBuffer.flush_sync(ctx.stack_id, 15_000) end)

      assert {:error, _reason} = result
      refute log =~ "dead-lettered"

      assert [{^ts, {:add, "handle-0", _shape, _comparable, _hash, _relations}, false, attempts}] =
               :ets.tab2list(ops_table)

      assert attempts == WriteBuffer.max_flush_attempts() - 1
      assert {:ok, ^conn_before} = write_conn.()

      :ok = ShapeDb.Connection.execute(reader, "ROLLBACK")
      :ok = ShapeDb.Connection.close(reader)

      assert :ok = WriteBuffer.flush_sync(ctx.stack_id)
      assert {:ok, 1} = ShapeDb.count_shapes(ctx.stack_id)
    end

    # The batch read only a missing shape out of a snapshot-complete update. An update that failed,
    # such as one that met another connection's lock, was dropped and the commit took the mark as
    # written, so the snapshot stayed incomplete and the next takeover reclaimed the shape.
    test "write buffer keeps a snapshot-complete mark whose update fails", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      {:ok, other} = ShapeDb.Sqlite3.open(path, [])

      :ok =
        ShapeDb.Connection.execute(other, """
        CREATE TRIGGER refuse_mark BEFORE UPDATE OF snapshot_complete ON shapes
        BEGIN SELECT RAISE(ABORT, 'refused'); END
        """)

      :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "handle-0")
      {result, _log} = with_log(fn -> WriteBuffer.flush_sync(ctx.stack_id) end)

      assert {:error, _reason} = result

      assert [{_ts, {:snapshot_complete, "handle-0"}, false, _attempts}] =
               :ets.tab2list(WriteBuffer.operations_table_name(ctx.stack_id))

      refute ShapeDb.snapshot_complete?(ctx.stack_id, "handle-0")

      :ok = ShapeDb.Connection.execute(other, "DROP TRIGGER refuse_mark")
      :ok = ShapeDb.Connection.close(other)

      assert :ok = WriteBuffer.flush_sync(ctx.stack_id)
      assert ShapeDb.snapshot_complete?(ctx.stack_id, "handle-0")
    end

    # The queue lives in the buffer's process. Stopped while another process held the file, the
    # buffer's final flush failed once and the queued writes went with the process.
    test "write buffer's final flush waits for another connection's read lock", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")

      {:ok, reader} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(reader, "BEGIN")
      {:ok, [0]} = ShapeDb.Connection.fetch_one(reader, "SELECT count(*) FROM shapes", [])

      # The lock goes once a flush has failed on it.
      test_pid = self()

      releaser =
        spawn_link(fn ->
          receive do
            :lock_lost ->
              :ok = ShapeDb.Connection.execute(reader, "ROLLBACK")
              send(test_pid, :released)
          end
        end)

      Repatch.patch(
        ShapeDb.Connection,
        :checkout_write!,
        [mode: :shared],
        fn stack_id, label, fun, timeout ->
          try do
            Repatch.real(ShapeDb.Connection.checkout_write!(stack_id, label, fun, timeout))
          rescue
            e ->
              send(releaser, :lock_lost)
              reraise e, __STACKTRACE__
          end
        end
      )

      Repatch.allow(self(), GenServer.whereis(WriteBuffer.name(ctx.stack_id)))

      :ok = stop_supervised(ctx.shape_db)

      assert_receive :released
      assert {:ok, [1]} = ShapeDb.Connection.fetch_one(reader, "SELECT count(*) FROM shapes", [])
      :ok = ShapeDb.Connection.close(reader)
    end

    # A drain before the replication lock goes meets the lock of an instance waiting for it, which
    # holds the file for its boot integrity check. One flush would give up at the first contention.
    test "a drain retries through another connection's read lock", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")

      {:ok, reader} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(reader, "BEGIN")
      {:ok, [0]} = ShapeDb.Connection.fetch_one(reader, "SELECT count(*) FROM shapes", [])

      # The lock goes once a flush has failed on it.
      test_pid = self()

      releaser =
        spawn_link(fn ->
          receive do
            :lock_lost ->
              :ok = ShapeDb.Connection.execute(reader, "ROLLBACK")
              send(test_pid, :released)
          end
        end)

      Repatch.patch(
        ShapeDb.Connection,
        :checkout_write!,
        [mode: :shared],
        fn stack_id, label, fun, timeout ->
          try do
            Repatch.real(ShapeDb.Connection.checkout_write!(stack_id, label, fun, timeout))
          rescue
            e ->
              send(releaser, :lock_lost)
              reraise e, __STACKTRACE__
          end
        end
      )

      Repatch.allow(self(), GenServer.whereis(WriteBuffer.name(ctx.stack_id)))

      assert :ok = WriteBuffer.drain(ctx.stack_id)
      assert_receive :released
      assert 0 = WriteBuffer.pending_operations_count(ctx.stack_id)
      assert {:ok, [1]} = ShapeDb.Connection.fetch_one(reader, "SELECT count(*) FROM shapes", [])
      :ok = ShapeDb.Connection.close(reader)
    end

    # A flush already running at the shutdown signal can hold the buffer for a checkout and a busy
    # timeout before the final flush starts, and its last attempt as long again after its window.
    # A fixed shutdown timeout cut it off once the checkout timeout was raised.
    test "write buffer's shutdown timeout covers its final flush" do
      for checkout_timeout <- [50, 10_000, 30_000] do
        assert %{shutdown: shutdown} =
                 WriteBuffer.child_spec(
                   stack_id: "stack",
                   write_checkout_timeout: checkout_timeout
                 )

        assert shutdown >= 2 * (checkout_timeout + 2_000) + 20_000
      end
    end

    # Validation runs when this instance takes the replication lock again, and reads the catalog as
    # persisted. A flush that met another process's read lock left a snapshot-complete mark queued,
    # and validation, which went on, read the shape as incomplete and removed it.
    test "validation keeps a shape whose snapshot-complete mark a read lock leaves queued", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "handle-0")

      {:ok, reader} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(reader, "BEGIN")
      {:ok, [1]} = ShapeDb.Connection.fetch_one(reader, "SELECT count(*) FROM shapes", [])

      # The lock goes once the flush has failed on it, before validation reads the catalog.
      Repatch.patch(WriteBuffer, :flush_sync, fn stack_id, timeout ->
        result = Repatch.real(WriteBuffer.flush_sync(stack_id, timeout))

        if result == {:error, :lock_contention} and is_nil(Process.get(:released)) do
          Process.put(:released, true)
          :ok = ShapeDb.Connection.execute(reader, "ROLLBACK")
        end

        result
      end)

      assert {:ok, [], 1} = ShapeDb.validate_existing_shapes(ctx.stack_id)
      assert Process.get(:released)
      :ok = ShapeDb.Connection.close(reader)

      assert :ok = WriteBuffer.flush_sync(ctx.stack_id)
      assert ShapeDb.snapshot_complete?(ctx.stack_id, "handle-0")
    end

    # Validation removes an invalid shape whose removal the flush leaves queued, and reports it
    # for cleanup. A count that took the buffer's queued removals out of the catalog's rows took
    # that shape out twice until the buffer's next flush.
    test "validation counts once a shape whose removal a read lock leaves queued", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      other_shape = Shape.new!("items", inspector: @stub_inspector, where: "id = 1")
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, other_shape, "handle-1")
      :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "handle-0")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      :ok = ShapeDb.remove_shape(ctx.stack_id, "handle-1")

      {:ok, reader} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(reader, "BEGIN")
      {:ok, [2]} = ShapeDb.Connection.fetch_one(reader, "SELECT count(*) FROM shapes", [])

      # The lock goes once the flush has failed on it, before validation reads the catalog.
      Repatch.patch(WriteBuffer, :flush_sync, fn stack_id, timeout ->
        result = Repatch.real(WriteBuffer.flush_sync(stack_id, timeout))

        if result == {:error, :lock_contention} and is_nil(Process.get(:released)) do
          Process.put(:released, true)
          :ok = ShapeDb.Connection.execute(reader, "ROLLBACK")
        end

        result
      end)

      assert {:ok, ["handle-1"], 1} = ShapeDb.validate_existing_shapes(ctx.stack_id)
      assert Process.get(:released)
      assert {:ok, [1]} = ShapeDb.Connection.fetch_one(reader, "SELECT count(*) FROM shapes", [])
      :ok = ShapeDb.Connection.close(reader)

      assert :ok = WriteBuffer.flush_sync(ctx.stack_id)
      assert {:ok, 1} = ShapeDb.count_shapes(ctx.stack_id)
      refute ShapeDb.handle_exists?(ctx.stack_id, "handle-1")
    end

    # The buffer takes a committed operation off its counters only once it has let the connection
    # go, so a count read from them can still debit a row the catalog no longer has: 0 with a
    # shape left, and on the first refresh that clears every shape's storage.
    for exclusive_mode <- [true, false],
        {left, kind, count} <- [
          {"a committed shape", :committed, 1},
          {"a queued shape", :queued, 1},
          {"an empty catalog", :none, 0}
        ] do
      @tag shape_db_opts: [exclusive_mode: exclusive_mode]
      test "validation counts #{left} while the buffer still debits a removal it committed " <>
             "(exclusive_mode: #{exclusive_mode})",
           %{stack_id: stack_id} do
        removed = Shape.new!("items", inspector: @stub_inspector, where: "id = 1")
        shape = Shape.new!("items", inspector: @stub_inspector)
        {:ok, _hash} = ShapeDb.add_shape(stack_id, removed, "handle-1")
        :ok = ShapeDb.mark_snapshot_complete(stack_id, "handle-1")

        if unquote(kind) == :committed do
          {:ok, _hash} = ShapeDb.add_shape(stack_id, shape, "handle-0")
          :ok = ShapeDb.mark_snapshot_complete(stack_id, "handle-0")
        end

        :ok = WriteBuffer.flush_sync(stack_id)

        if unquote(kind) == :queued do
          {:ok, _hash} = ShapeDb.add_shape(stack_id, shape, "handle-0")
        end

        # Committed as by a flush that has not taken it off the buffer's counters yet.
        :ok = ShapeDb.remove_shape(stack_id, "handle-1")

        :ok =
          ShapeDb.Connection.checkout_write!(
            stack_id,
            :test,
            &ShapeDb.Query.remove_shape(&1, "handle-1")
          )

        Repatch.patch(WriteBuffer, :flush_sync, fn _stack_id, _timeout -> :ok end)

        assert {:ok, [], unquote(count)} = ShapeDb.validate_existing_shapes(stack_id)
      end
    end

    # A queued removal can still fail for good, and the buffer then drops it and leaves the row.
    # Counted as done, the removal of the only shape read 0, which on the first refresh clears the
    # storage of the shape left in the catalog.
    test "validation counts a shape until its queued removal commits", %{stack_id: stack_id} do
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(stack_id, shape, "handle-0")
      :ok = ShapeDb.mark_snapshot_complete(stack_id, "handle-0")
      :ok = WriteBuffer.flush_sync(stack_id)
      :ok = ShapeDb.remove_shape(stack_id, "handle-0")
      Repatch.patch(WriteBuffer, :flush_sync, fn _stack_id, _timeout -> :ok end)

      assert {:ok, [], 1} = ShapeDb.validate_existing_shapes(stack_id)
    end

    # The buffer lists a queued addition under its handle alone, and dropping an older addition and
    # its removal under the same handle takes that listing with them. Counted from the listings,
    # the addition still queued read 0, which on the first refresh clears its storage.
    test "validation counts an addition queued after a dropped one under the same handle",
         %{stack_id: stack_id} do
      ops_table = WriteBuffer.operations_table_name(stack_id)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(stack_id, shape, "handle-0")
      :ok = ShapeDb.remove_shape(stack_id, "handle-0")

      for [ts] <- :ets.match(ops_table, {:"$1", :_, :_, :_}) do
        true = :ets.update_element(ops_table, ts, {4, WriteBuffer.max_flush_attempts() - 1})
      end

      {:ok, _hash} = ShapeDb.add_shape(stack_id, shape, "handle-0")
      true = :ets.insert(ops_table, {WriteBuffer.op_key(), {:poison, "handle-x"}, false, 0})

      {_result, log} = with_log(fn -> WriteBuffer.flush_sync(stack_id) end)
      assert log =~ "dead-lettered"
      assert 2 = WriteBuffer.pending_operations_count(stack_id)

      Repatch.patch(WriteBuffer, :flush_sync, fn _stack_id, _timeout -> :ok end)

      assert {:ok, [], 1} = ShapeDb.validate_existing_shapes(stack_id)
    end

    # A flush that fails for any other reason leaves the mark queued as well.
    test "validation keeps a shape whose snapshot-complete mark a failed flush leaves queued",
         ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      {:ok, other} = ShapeDb.Sqlite3.open(path, [])

      :ok =
        ShapeDb.Connection.execute(other, """
        CREATE TRIGGER refuse_mark BEFORE UPDATE OF snapshot_complete ON shapes
        BEGIN SELECT RAISE(ABORT, 'refused'); END
        """)

      :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "handle-0")

      {result, _log} = with_log(fn -> ShapeDb.validate_existing_shapes(ctx.stack_id) end)
      assert {:ok, [], 1} = result

      :ok = ShapeDb.Connection.execute(other, "DROP TRIGGER refuse_mark")
      :ok = ShapeDb.Connection.close(other)

      assert :ok = WriteBuffer.flush_sync(ctx.stack_id)
      assert ShapeDb.snapshot_complete?(ctx.stack_id, "handle-0")
    end

    # Exclusive mode has one connection, which readers share. A flush that waited for it past a
    # call's default 5 s ended validation with an exit, and one that gave up on the checkout left
    # validation reading a catalog behind the buffer.
    @tag shape_db_opts: [exclusive_mode: true, write_checkout_timeout: 6_000]
    test "validation waits for a flush that a held connection delays", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "handle-0")
      test_pid = self()

      reader =
        Task.async(fn ->
          ShapeDb.Connection.checkout!(ctx.stack_id, :slow_read, fn _conn ->
            send(test_pid, :connection_held)

            receive do
              :release -> :ok
            end
          end)
        end)

      assert_receive :connection_held

      # The connection goes once a flush has given up on it.
      Repatch.patch(WriteBuffer, :flush_sync, fn stack_id, timeout ->
        result = Repatch.real(WriteBuffer.flush_sync(stack_id, timeout))

        if result == {:error, :checkout_timeout} and is_nil(Process.get(:released)) do
          Process.put(:released, true)
          send(reader.pid, :release)
        end

        result
      end)

      assert {:ok, [], 1} = ShapeDb.validate_existing_shapes(ctx.stack_id)
      assert Process.get(:released)
      :ok = Task.await(reader)

      assert :ok = WriteBuffer.flush_sync(ctx.stack_id)
      assert ShapeDb.snapshot_complete?(ctx.stack_id, "handle-0")
    end

    # With nothing queued, the flush takes no lock, and validation's own commit meets the reader.
    # It raised, and the refresh that replication resumes after failed with it.
    test "validation waits for its commit through another connection's read lock", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")
      :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "handle-0")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      {:ok, reader} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(reader, "BEGIN")
      {:ok, [1]} = ShapeDb.Connection.fetch_one(reader, "SELECT count(*) FROM shapes", [])

      # The lock goes once a commit has failed on it.
      Repatch.patch(ShapeDb.Connection, :checkout_write!, fn stack_id, label, fun, timeout ->
        try do
          Repatch.real(ShapeDb.Connection.checkout_write!(stack_id, label, fun, timeout))
        rescue
          e ->
            if is_nil(Process.get(:released)) do
              Process.put(:released, true)
              :ok = ShapeDb.Connection.execute(reader, "ROLLBACK")
            end

            reraise e, __STACKTRACE__
        end
      end)

      assert {:ok, [], 1} = ShapeDb.validate_existing_shapes(ctx.stack_id)
      assert Process.get(:released)
      :ok = ShapeDb.Connection.close(reader)
    end

    # The connection manager waits for validation, shutdown included, so a lock that never ends
    # must not hold it forever.
    @tag timeout: 20_000
    test "validation gives up on a read lock that outlasts its timeout", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")
      :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "handle-0")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      {:ok, reader} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(reader, "BEGIN")
      {:ok, [1]} = ShapeDb.Connection.fetch_one(reader, "SELECT count(*) FROM shapes", [])

      error =
        assert_raise MatchError, fn -> ShapeDb.validate_existing_shapes(ctx.stack_id, 100) end

      assert ShapeDb.Connection.lock_contention?(error)

      :ok = ShapeDb.Connection.execute(reader, "ROLLBACK")
      :ok = ShapeDb.Connection.close(reader)
    end

    # The bound starts with validation. Its flush, which a held connection delays for as long as
    # the write buffer waits for a checkout, ran first with no call timeout, and each attempt then
    # waited a minute for the connection, whatever was left of the bound.
    @tag shape_db_opts: [exclusive_mode: true, write_checkout_timeout: 30_000]
    @tag timeout: 20_000
    test "validation's bound covers its flush and its wait for the connection", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "handle-0")
      test_pid = self()

      reader =
        Task.async(fn ->
          ShapeDb.Connection.checkout!(ctx.stack_id, :slow_read, fn _conn ->
            send(test_pid, :connection_held)

            receive do
              :release -> :ok
            end
          end)
        end)

      assert_receive :connection_held
      started = System.monotonic_time(:millisecond)

      # A last attempt waits for the connection as long as a write does by default, 10 s.
      assert {:timeout, {NimblePool, :checkout, _}} =
               catch_exit(ShapeDb.validate_existing_shapes(ctx.stack_id, 100))

      assert (System.monotonic_time(:millisecond) - started) in 9_000..18_000

      send(reader.pid, :release)
      :ok = Task.await(reader)
    end

    # A flush that waits 3 s for the connection leaves a 15 s bound about 12 s for the commit's
    # own wait. With the deadline taken after the flush, the commit had the whole 15 s again.
    @tag shape_db_opts: [exclusive_mode: true, write_checkout_timeout: 3_000]
    test "validation's flush counts against its bound", ctx do
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, "handle-0")
      test_pid = self()

      reader =
        Task.async(fn ->
          ShapeDb.Connection.checkout!(ctx.stack_id, :slow_read, fn _conn ->
            send(test_pid, :connection_held)

            receive do
              :release -> :ok
            end
          end)
        end)

      assert_receive :connection_held

      Repatch.patch(ShapeDb.Connection, :checkout_write!, fn stack_id, label, fun, timeout ->
        send(test_pid, {:validation_checkout, timeout})
        send(reader.pid, :release)
        Repatch.real(ShapeDb.Connection.checkout_write!(stack_id, label, fun, timeout))
      end)

      assert {:ok, [], 1} = ShapeDb.validate_existing_shapes(ctx.stack_id, 15_000)
      assert_received {:validation_checkout, timeout}
      assert timeout in 10_000..12_100
      :ok = Task.await(reader)
    end

    # Shape creation looks the handle up on the writer. In rollback-journal mode a write
    # transaction cannot commit while another process holds a read lock, even one that changed
    # nothing, so wrapping the lookup in one failed every shape creation for as long as a waiting
    # instance's boot integrity check ran.
    test "the consistent handle lookup does not wait for another connection's read lock", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-0")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      {:ok, reader} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(reader, "BEGIN")
      {:ok, [_count]} = ShapeDb.Connection.fetch_one(reader, "SELECT count(*) FROM shapes", [])

      try do
        assert {:ok, "handle-0"} = ShapeDb.handle_for_shape_critical(ctx.stack_id, shape)
      after
        :ok = ShapeDb.Connection.execute(reader, "ROLLBACK")
        :ok = ShapeDb.Connection.close(reader)
      end
    end

    @tag shape_db_opts: [exclusive_mode: true, storage_dir: ":memory:"]
    test "allows for an in-memory database", ctx do
      # file is empty for in-memory
      assert {:ok, [[""]]} =
               ShapeDb.Connection.checkout!(ctx.stack_id, :read_call, fn %{conn: conn} ->
                 ShapeDb.Connection.fetch_all(
                   conn,
                   "SELECT file FROM pragma_database_list WHERE name = 'main'",
                   []
                 )
               end)
    end

    # A read that meets another connection's lock answers neither way and says nothing about the
    # connection. Closed, the store's only connection would be reopened with the boot's integrity
    # check, which the lock can fail too.
    test "a handle read that meets another connection's lock raises with the connection kept",
         ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-locked")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)
      assert WriteBuffer.has_handle?(ctx.stack_id, "handle-locked") == :unknown

      connection = ShapeDb.Connection.checkout!(ctx.stack_id, :connection, & &1)
      {:ok, holder} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(holder, "BEGIN EXCLUSIVE")

      error =
        assert_raise ShapeDb.Error, fn ->
          ShapeDb.handle_exists?(ctx.stack_id, "handle-locked")
        end

      assert ShapeDb.Connection.lock_contention?(error.error)

      :ok = ShapeDb.Connection.execute(holder, "ROLLBACK")
      :ok = ShapeDb.Connection.close(holder)

      assert ShapeDb.Connection.checkout!(ctx.stack_id, :connection, & &1) == connection
      assert ShapeDb.handle_exists?(ctx.stack_id, "handle-locked")
    end

    # Reopened, the store's only connection would rerun the boot's integrity check, and a check
    # the same fault failed would delete the store. Only a read that found the file damaged has
    # the connection reopened. A step that fails after the row leaves the statement mid-step as a
    # first one does, and a kept connection must read with it again.
    for {fault, code, kept?} <- [
          {"an I/O error", 266, true},
          {"a damaged page", 11, false},
          {"a file that is not a database", 26, false}
        ],
        {at, failing} <- [{"", 1}, {" after its row", 2}] do
      test "a handle read that fails on #{fault}#{at} raises, the connection #{if kept?, do: "kept", else: "reopened"}",
           ctx do
        shape = Shape.new!("items", inspector: @stub_inspector)
        {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-faulted")
        :ok = WriteBuffer.flush_sync(ctx.stack_id)
        assert WriteBuffer.has_handle?(ctx.stack_id, "handle-faulted") == :unknown

        connection = ShapeDb.Connection.checkout!(ctx.stack_id, :connection, & &1)

        # The step fails once, in this process, where the checkout runs its function: the read's
        # first, or the one after the row it found.
        Repatch.patch(ShapeDb.Sqlite3, :step, fn conn, stmt ->
          step = Process.get(:steps, 0) + 1
          Process.put(:steps, step)

          if step == unquote(failing),
            do: {:error, unquote(code)},
            else: Repatch.real(ShapeDb.Sqlite3.step(conn, stmt))
        end)

        error =
          assert_raise ShapeDb.Error, fn ->
            ShapeDb.handle_exists?(ctx.stack_id, "handle-faulted")
          end

        assert error.error == unquote(code)

        if unquote(kept?),
          do: assert(ShapeDb.Connection.checkout!(ctx.stack_id, :connection, & &1) == connection),
          else:
            refute(ShapeDb.Connection.checkout!(ctx.stack_id, :connection, & &1) == connection)

        assert ShapeDb.handle_exists?(ctx.stack_id, "handle-faulted")
      end
    end
  end

  describe "recovery" do
    test "resets state when db file is corrupted", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      assert {:ok, 0} = ShapeDb.count_shapes(ctx.stack_id)

      stop_supervised!(ctx.shape_db)

      File.write!(path, "invalid!")

      assert {:ok, _pid} =
               start_supervised(
                 {Electric.ShapeCache.ShapeStatus.ShapeDb.Supervisor,
                  [
                    stack_id: ctx.stack_id,
                    shape_db_opts: [
                      storage_dir: ctx.tmp_dir,
                      manual_flush_only: true,
                      read_pool_size: 1
                    ]
                  ]},
                 id: "shape_db"
               )
    end

    # `execute_all/2` runs the migration one statement at a time with no enclosing transaction, and
    # `PRAGMA user_version` is the last of them, so a process that dies partway leaves a db with
    # some of the schema and a version of 0. It opens, and it passes quick_check -- half a schema is
    # structurally valid -- so neither recovery branch fires. The next boot reads version 0, replays
    # the migration from the top, and `CREATE TABLE shapes` fails on the table that is already
    # there. Reachable on the first boot after a version bump, which is exactly when a process is
    # most likely to be killed.
    test "recovers from a migration that died partway through", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      stop_supervised!(ctx.shape_db)
      File.rm_rf!(Path.dirname(path))
      File.mkdir_p!(Path.dirname(path))

      {:ok, conn} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(conn, "CREATE TABLE shapes (handle BLOB NOT NULL) STRICT")
      :ok = ShapeDb.Connection.close(conn)

      assert {:ok, 0} = interrupted_db_version(path)

      assert {:ok, _pid} =
               start_supervised(
                 {Electric.ShapeCache.ShapeStatus.ShapeDb.Supervisor,
                  [
                    stack_id: ctx.stack_id,
                    shape_db_opts: [
                      storage_dir: ctx.tmp_dir,
                      manual_flush_only: true,
                      read_pool_size: 1
                    ]
                  ]},
                 id: "shape_db"
               )

      assert {:ok, 0} = ShapeDb.count_shapes(ctx.stack_id)
    end

    # The other half: recovery above rebuilds a partial db, this keeps one from being written. A
    # failure partway through must leave nothing new behind, which is only true if the schema goes
    # in as one transaction.
    test "a migration that fails partway leaves no half-built schema", ctx do
      # first makes the run fail only once several statements have already succeeded.
      path = Path.join(ctx.tmp_dir, "partial.sqlite")
      {:ok, conn} = ShapeDb.Sqlite3.open(path, [])

      :ok =
        ShapeDb.Connection.execute(
          conn,
          "CREATE TABLE relations (handle BLOB NOT NULL, oid INTEGER NOT NULL) STRICT"
        )

      assert {:error, _} = ShapeDb.Connection.migrate(conn, [])

      assert {:ok, ["relations"]} =
               ShapeDb.Connection.fetch_one(
                 conn,
                 "SELECT group_concat(name) FROM sqlite_master WHERE type = 'table'",
                 []
               ),
             "the failed migration left tables behind, so a killed one leaves a db no later boot can migrate"

      assert {:ok, [0]} = ShapeDb.Connection.fetch_one(conn, "PRAGMA user_version", [])
      :ok = ShapeDb.Connection.close(conn)
    end

    defp interrupted_db_version(path) do
      {:ok, conn} = ShapeDb.Sqlite3.open(path, [])
      result = ShapeDb.Connection.fetch_one(conn, "PRAGMA user_version", [])
      :ok = ShapeDb.Connection.close(conn)

      with {:ok, [version]} <- result, do: {:ok, version}
    end

    # The tests either side of this one overwrite the whole file, so `Sqlite3.open` fails on the
    # header and recovery runs from the outer error branch. `PRAGMA quick_check` exists for the case
    # those never reach: a file SQLite opens happily whose pages are damaged. Nothing covered it.
    test "resets state when a page inside an openable db file is damaged", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)

      Enum.each(1..200, fn i ->
        {:ok, _} =
          ShapeDb.add_shape(
            ctx.stack_id,
            Shape.new!("items", inspector: @stub_inspector, where: "id = #{i}"),
            "handle-#{i}"
          )
      end)

      ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
      stop_supervised!(ctx.shape_db)

      # Page 1 carries the header and the schema, so it stays intact and the file still opens. A
      # later page holds table content: damaged there, only a b-tree walk notices.
      contents = File.read!(path)
      page = 4096
      assert byte_size(contents) > page * 3, "the db is too small to damage a non-schema page"

      File.write!(
        path,
        binary_part(contents, 0, page * 2) <>
          :binary.copy(<<0xA5>>, page) <>
          binary_part(contents, page * 3, byte_size(contents) - page * 3)
      )

      {started, log} =
        with_log(fn ->
          start_supervised(
            {Electric.ShapeCache.ShapeStatus.ShapeDb.Supervisor,
             [
               stack_id: ctx.stack_id,
               shape_db_opts: [
                 storage_dir: ctx.tmp_dir,
                 manual_flush_only: true,
                 read_pool_size: 1
               ]
             ]},
            id: "shape_db"
          )
        end)

      assert {:ok, _pid} = started

      assert log =~ "Database file corrupt",
             "the db was accepted rather than recovered, so the damage is still in there"

      assert {:ok, 0} = ShapeDb.count_shapes(ctx.stack_id),
             "recovery must leave a usable empty db, not a half-readable damaged one"
    end

    # The db path embeds the shape status version, the schema version and the OTP release, so an
    # upgrade that bumps any of them boots onto an empty catalog and leaves the full one behind
    # under its old name. Nothing ever read or removed it, so every bump added another copy of the
    # whole catalog to the volume for good.
    #
    # Both modes, because they take different paths to the same directory: exclusive mode migrates
    # inside the pool worker rather than in the migrator, and the stacks this fork is deployed on
    # run exclusive mode -- so a sweep proven only on the pooled path is a sweep proven nowhere that
    # matters.
    for exclusive_mode <- [false, true] do
      @tag shape_db_opts: [exclusive_mode: exclusive_mode]
      test "discards the catalog left behind by a previous build (exclusive_mode: #{exclusive_mode})",
           ctx do
        # written it under. Renaming rather than fabricating keeps the logged shape count honest.
        {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)

        Enum.each(1..20, fn i ->
          {:ok, _} =
            ShapeDb.add_shape(
              ctx.stack_id,
              Shape.new!("items", inspector: @stub_inspector, where: "id = #{i}"),
              "handle-#{i}"
            )
        end)

        ShapeDb.WriteBuffer.flush_sync(ctx.stack_id)
        stop_supervised!(ctx.shape_db)

        dir = Path.dirname(path)
        superseded = Path.join(dir, "v1_1@otp-1.sqlite")
        sidecar = superseded <> "-wal"
        unrelated = Path.join(dir, "operator-notes.txt")
        File.rename!(path, superseded)
        File.write!(sidecar, "")
        File.write!(unrelated, "leave me alone")

        {started, log} =
          with_log(fn ->
            start_supervised(
              {Electric.ShapeCache.ShapeStatus.ShapeDb.Supervisor,
               [
                 stack_id: ctx.stack_id,
                 shape_db_opts: [
                   storage_dir: ctx.tmp_dir,
                   exclusive_mode: unquote(exclusive_mode),
                   manual_flush_only: true,
                   read_pool_size: 1
                 ]
               ]},
              id: "shape_db"
            )
          end)

        assert {:ok, _pid} = started

        refute File.exists?(superseded),
               "the superseded catalog survived, so every version bump keeps a full copy of it forever"

        refute File.exists?(sidecar),
               "the superseded catalog's sidecar outlived the catalog: #{inspect(File.ls!(dir))}"

        assert File.exists?(path), "this build's own catalog was taken with the superseded one"

        assert File.read!(unrelated) == "leave me alone",
               "the sweep is not scoped to the files the shape db names"

        assert log =~ "20 shapes",
               "the count of shapes about to be re-snapshotted was not reported, which is the " <>
                 "only warning before the load lands on Postgres"

        assert {:ok, 0} = ShapeDb.count_shapes(ctx.stack_id)
      end
    end

    @tag shape_db_opts: [exclusive_mode: true]
    test "resets state when db file is corrupted in exclusive mode", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      assert {:ok, 0} = ShapeDb.count_shapes(ctx.stack_id)

      stop_supervised!(ctx.shape_db)

      File.write!(path, "invalid!")

      assert {:ok, _pid} =
               start_supervised(
                 {Electric.ShapeCache.ShapeStatus.ShapeDb.Supervisor,
                  [
                    stack_id: ctx.stack_id,
                    shape_db_opts: [
                      storage_dir: ctx.tmp_dir,
                      manual_flush_only: true,
                      read_pool_size: 1
                    ]
                  ]},
                 id: "shape_db"
               )
    end

    # A second instance sharing the storage takes the database lock for each commit. An open that
    # meets that lock has found a busy catalog, not a damaged one, and must leave it in place.
    @tag shape_db_opts: [exclusive_mode: true]
    test "keeps a catalog that another connection holds locked", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-kept")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      stop_supervised!(ctx.shape_db)

      {:ok, holder} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(holder, "BEGIN EXCLUSIVE")

      shape_db_spec =
        {Electric.ShapeCache.ShapeStatus.ShapeDb.Supervisor,
         [
           stack_id: ctx.stack_id,
           shape_db_opts: [
             storage_dir: ctx.tmp_dir,
             exclusive_mode: true,
             manual_flush_only: true,
             read_pool_size: 1
           ]
         ]}

      # The open fails and leaves the file alone; a later start, the stack's restart, opens it.
      assert {{:error, _}, log} =
               with_log(fn -> start_supervised(shape_db_spec, id: "shape_db") end)

      refute log =~ "Database file corrupt"
      assert log =~ "locked by another connection"

      :ok = ShapeDb.Connection.execute(holder, "ROLLBACK")
      :ok = ShapeDb.Connection.close(holder)

      assert {:ok, _pid} = start_supervised(shape_db_spec, id: "shape_db")

      assert {:ok, [{"handle-kept", ^shape}]} = ShapeDb.list_shapes(ctx.stack_id)
    end

    # Taken before the check is prepared, the lock fails the prepare with an error code. Taken
    # between the prepare and the first step, it reaches the step, which the driver reports as
    # `:"$busy"` rather than an error tuple; that raised, the raise became a string, and the
    # recovery deleted the catalog.
    @tag shape_db_opts: [exclusive_mode: true]
    test "keeps a catalog locked between preparing the integrity check and running it", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      shape = Shape.new!("items", inspector: @stub_inspector)
      {:ok, _hash} = ShapeDb.add_shape(ctx.stack_id, shape, "handle-kept")
      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      stop_supervised!(ctx.shape_db)

      {:ok, holder} = ShapeDb.Sqlite3.open(path, [])

      Repatch.patch(ShapeDb.Sqlite3, :prepare, fn conn, sql ->
        result = Repatch.real(ShapeDb.Sqlite3.prepare(conn, sql))

        if sql == "PRAGMA quick_check" and is_nil(Process.get(:check_locked)) do
          Process.put(:check_locked, true)
          :ok = ShapeDb.Connection.execute(holder, "BEGIN EXCLUSIVE")
        end

        result
      end)

      pool_state = [storage_dir: ctx.tmp_dir, exclusive_mode: true]

      {result, log} =
        with_log(fn -> ShapeDb.Connection.open(pool_state, integrity_check: true) end)

      assert Process.get(:check_locked)
      assert {:error, _reason} = result
      refute log =~ "Database file corrupt"
      assert log =~ "locked by another connection"

      :ok = ShapeDb.Connection.execute(holder, "ROLLBACK")
      :ok = ShapeDb.Connection.close(holder)

      assert {:ok, _pid} =
               start_supervised(
                 {Electric.ShapeCache.ShapeStatus.ShapeDb.Supervisor,
                  [
                    stack_id: ctx.stack_id,
                    shape_db_opts: [
                      storage_dir: ctx.tmp_dir,
                      exclusive_mode: true,
                      manual_flush_only: true,
                      read_pool_size: 1
                    ]
                  ]},
                 id: "shape_db"
               )

      assert {:ok, [{"handle-kept", ^shape}]} = ShapeDb.list_shapes(ctx.stack_id)
    end

    # After quick_check, the check reads the schema version to find a migration that died partway.
    # A lock that failed that read passed the check, and such a catalog went on to the migration,
    # which fails on a table that is already there instead of rebuilding the file.
    @tag shape_db_opts: [exclusive_mode: true]
    test "keeps a partly migrated catalog whose version read meets a lock", ctx do
      {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
      stop_supervised!(ctx.shape_db)
      File.rm_rf!(Path.dirname(path))
      File.mkdir_p!(Path.dirname(path))

      {:ok, conn} = ShapeDb.Sqlite3.open(path, [])
      :ok = ShapeDb.Connection.execute(conn, "CREATE TABLE shapes (handle BLOB NOT NULL) STRICT")
      :ok = ShapeDb.Connection.close(conn)

      {:ok, holder} = ShapeDb.Sqlite3.open(path, [])

      # The lock is held from the version read's prepare until its step has failed.
      Repatch.patch(ShapeDb.Sqlite3, :prepare, fn conn, sql ->
        result = Repatch.real(ShapeDb.Sqlite3.prepare(conn, sql))

        if sql == "PRAGMA user_version" and is_nil(Process.get(:version_locked)) do
          Process.put(:version_locked, :held)
          :ok = ShapeDb.Connection.execute(holder, "BEGIN EXCLUSIVE")
        end

        result
      end)

      Repatch.patch(ShapeDb.Sqlite3, :step, fn conn, stmt ->
        result = Repatch.real(ShapeDb.Sqlite3.step(conn, stmt))

        if Process.get(:version_locked) == :held do
          Process.put(:version_locked, :released)
          :ok = ShapeDb.Connection.execute(holder, "ROLLBACK")
        end

        result
      end)

      pool_state = [storage_dir: ctx.tmp_dir, exclusive_mode: true]

      {result, log} =
        with_log(fn -> ShapeDb.Connection.open(pool_state, integrity_check: true) end)

      assert Process.get(:version_locked) == :released
      assert {:error, _reason} = result
      assert log =~ "locked by another connection"

      :ok = ShapeDb.Connection.close(holder)

      assert {:ok, _pid} =
               start_supervised(
                 {Electric.ShapeCache.ShapeStatus.ShapeDb.Supervisor,
                  [
                    stack_id: ctx.stack_id,
                    shape_db_opts: [
                      storage_dir: ctx.tmp_dir,
                      exclusive_mode: true,
                      manual_flush_only: true,
                      read_pool_size: 1
                    ]
                  ]},
                 id: "shape_db"
               )

      assert {:ok, 0} = ShapeDb.count_shapes(ctx.stack_id)
    end
  end

  describe "error handling" do
    test "errors raised within transaction do not cause errors attempting to rollback", ctx do
      assert_raise RuntimeError, "source error", fn ->
        ShapeDb.Connection.checkout_write!(ctx.stack_id, :raising, fn %{conn: conn} ->
          # commit the txn so that attempting to rollback after the exception
          # will return an error
          :ok = ShapeDb.Connection.execute(conn, "COMMIT")
          raise RuntimeError, "source error"
        end)
      end
    end

    test "crashing WriteBuffer restarts entire supervision tree", ctx do
      assert supervisor_pid = GenServer.whereis(ShapeDb.Supervisor.name(ctx.stack_id))
      super_ref = Process.monitor(supervisor_pid)
      assert write_buffer_pid = GenServer.whereis(ShapeDb.WriteBuffer.name(ctx.stack_id))

      buffer_ref = Process.monitor(write_buffer_pid)
      Process.exit(write_buffer_pid, :some_reason)
      assert_receive {:DOWN, ^buffer_ref, :process, ^write_buffer_pid, :some_reason}
      assert_receive {:DOWN, ^super_ref, :process, ^supervisor_pid, _}
    end
  end

  describe "pool scaling" do
    @tag shape_db_opts: [connection_idle_timeout: 5, read_pool_size: 10]
    test "scales to 0 if nothing is active", ctx do
      parent = self()

      n = ctx.shape_db_opts[:read_pool_size] || raise "missing :read_pool_size configuration"

      pids =
        for i <- 1..n do
          start_supervised!(
            {Task,
             fn ->
               ShapeDb.Connection.checkout!(ctx.stack_id, :test, fn _conn ->
                 send(parent, {:ready, i})
                 receive(do: (_msg -> :ok))
               end)

               send(parent, {:done, i})
             end},
            id: {:checkout, i}
          )
        end

      for i <- 1..n, do: assert_receive({:ready, ^i})

      assert :ok = assert_stats_match(ctx, connections: n)

      for pid <- pids, do: send(pid, :done)
      for i <- 1..n, do: assert_receive({:done, ^i})

      assert :ok = assert_stats_match(ctx, connections: 0)
    end

    @tag shape_db_opts: [exclusive_mode: true, connection_idle_timeout: 10]
    test "does not scale the pool in exclusive mode", ctx do
      ShapeDb.Connection.checkout!(ctx.stack_id, :test, fn _conn ->
        assert :ok = assert_stats_match(ctx, connections: 1)
      end)

      # refute the match rather than assert connections: 1 because the
      # connections: 1 case is vulnerable to timing issues if the connection
      # count lags
      assert :error = assert_stats_match(ctx, [connections: 0], 50)
      assert :ok = assert_stats_match(ctx, connections: 1)
    end

    defp assert_stats_match(ctx, match, repeats \\ 250)

    defp assert_stats_match(_ctx, _match, 0) do
      :error
    end

    defp assert_stats_match(ctx, match, repeats) do
      {:ok, stats} = ShapeDb.statistics(ctx.stack_id)

      if Enum.all?(match, fn {k, v} -> stats[k] == v end) do
        :ok
      else
        Process.sleep(20)
        assert_stats_match(ctx, match, repeats - 1)
      end
    end
  end
end
