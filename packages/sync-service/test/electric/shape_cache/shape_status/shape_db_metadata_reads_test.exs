defmodule Electric.ShapeCache.ShapeStatus.ShapeDbMetadataReadsTest do
  # A boot asks the store two things about every shape that the shape itself does not answer: its
  # digest and whether its snapshot completed. Both used to go through the rows, so through every
  # stored shape: the whole table to list their metadata, and all of them held in
  # memory at once to find the snapshots that never completed.
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO
  import Support.ComponentSetup

  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @moduletag :tmp_dir
  @moduletag shape_db_opts: [exclusive_mode: true]

  @inspector StubInspector.new(
               tables: ["items"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "value", type: "text"}
               ]
             )

  setup [:with_stack_id_from_test, :with_shape_db]

  describe "query plans" do
    test "the metadata walk reads indexes alone", ctx do
      assert [_ | _] = plan = query_plan(ctx.stack_id, "list_shape_meta")
      assert Enum.all?(plan, &(&1 =~ "USING COVERING INDEX")), inspect(plan)
    end

    test "the incomplete snapshots are read from an index alone", ctx do
      assert [_ | _] = plan = query_plan(ctx.stack_id, "select_incomplete")
      assert Enum.all?(plan, &(&1 =~ "USING COVERING INDEX")), inspect(plan)
    end
  end

  describe "complete and incomplete snapshots" do
    setup ctx do
      shapes =
        for n <- 1..20 do
          shape = Shape.new!("items", inspector: @inspector, where: "id = #{n}")
          handle = "handle-#{n}"
          {:ok, digest} = ShapeDb.add_shape(ctx.stack_id, shape, handle)
          complete? = rem(n, 3) != 0
          if complete?, do: :ok = ShapeDb.mark_snapshot_complete(ctx.stack_id, handle)
          {handle, digest, complete?}
        end

      :ok = WriteBuffer.flush_sync(ctx.stack_id)

      %{shapes: shapes}
    end

    test "reduce_shape_meta/3 reports the digest and the flag of each shape", ctx do
      assert ShapeDb.reduce_shape_meta(ctx.stack_id, [], &[&1 | &2]) |> Enum.sort() ==
               Enum.sort(ctx.shapes)
    end

    # full-xid positions enabled: no stored shape is unsupported, only the flag decides
    test "validate_existing_shapes/1 removes the incomplete shapes", ctx do
      Electric.StackConfig.put(ctx.stack_id, :feature_flags, ["full_xid_source_positions"])
      incomplete = for {handle, _digest, false} <- ctx.shapes, do: handle
      complete = for {_handle, _digest, true} = shape <- ctx.shapes, do: shape

      assert {:ok, removed, 14} = ShapeDb.validate_existing_shapes(ctx.stack_id)
      assert Enum.sort(removed) == Enum.sort(incomplete)

      assert ShapeDb.reduce_shape_meta(ctx.stack_id, [], &[&1 | &2]) |> Enum.sort() ==
               Enum.sort(complete)
    end
  end

  # The plan lines `ShapeDb.explain/1` prints for one named statement.
  defp query_plan(stack_id, statement) do
    output = capture_io(fn -> :ok = ShapeDb.explain(stack_id) end)

    case Regex.run(~r/#{statement}.*?: \[[^\]]*\] ([^\n]*)/s, strip_ansi(output)) do
      [_, plan] -> String.split(plan, ", ", trim: true)
      nil -> []
    end
  end

  defp strip_ansi(text), do: String.replace(text, ~r/\e\[[0-9;]*m/, "")
end
