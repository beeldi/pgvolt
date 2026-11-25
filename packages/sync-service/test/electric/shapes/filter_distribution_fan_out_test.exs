defmodule Electric.Shapes.FilterDistributionFanOutTest do
  use ExUnit.Case, async: false

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Filter.WhereCondition
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["employees"],
               columns: [
                 %{name: "id", type: "text", pk_position: 0},
                 %{name: "firmId", type: "text"},
                 %{name: "divisionId", type: "text"},
                 %{name: "categoryId", type: "text"},
                 %{name: "floorId", type: "text"},
                 %{name: "deletedAt", type: "timestamptz"}
               ]
             )

  @token ~s|'cache-buster:00003' = 'cache-buster:00003'|

  defp shape(where), do: Shape.new!("employees", where: where, inspector: @inspector)

  defp inlist(col, prefix, offset, k) do
    ~s|"#{col}" IN | <> "(" <> Enum.map_join(1..k, ", ", &"'#{prefix}#{offset + &1}'") <> ")"
  end

  defp mem(f) do
    ws = :erlang.system_info(:wordsize)

    for t <- [f.where_cond_table, f.eq_index_table, f.shapes_table, f.other_shapes_table] do
      i = :ets.info(t)
      {i[:size], Float.round(i[:memory] * ws / 1024, 1)}
    end
  end

  defp total_kb(f) do
    ws = :erlang.system_info(:wordsize)

    [f.where_cond_table, f.eq_index_table, f.shapes_table, f.other_shapes_table]
    |> Enum.map(&(:ets.info(&1)[:memory] * ws / 1024))
    |> Enum.sum()
    |> Float.round(1)
  end

  defp row(label, cells), do: IO.puts(String.pad_trailing(label, 34) <> Enum.join(cells, "  "))

  @tag :performance
  test "case 1: two IN lists in one clause -> multiplicative leaf explosion" do
    IO.puts("\n### case 1: n x m cross product from a single shape ###")
    row("clause", ["where_rows", "eq_rows", "where_KB", "add_ms"])

    for {n, m} <- [{5, 5}, {10, 10}, {20, 20}, {40, 40}, {60, 60}] do
      where =
        ~s|"firmId" = 'F0' AND | <>
          inlist("divisionId", "b", 0, n) <>
          " AND " <> inlist("categoryId", "c", 0, m) <> ~s| AND "deletedAt" IS NULL|

      s = shape(where)
      f = Filter.new()
      {us, f} = :timer.tc(fn -> Filter.add_shape(f, "x", s) end)
      [{wr, wkb}, {er, _}, _, _] = mem(f)
      row("IN(#{n}) AND IN(#{m})", [wr, er, wkb, Float.round(us / 1000, 1)])
    end
  end

  @tag :performance
  test "case 2: three OR groups -> exponential" do
    IO.puts("\n### case 2: k OR-groups of 10 each ###")
    row("clause", ["where_rows", "eq_rows", "where_KB", "add_ms"])

    cols = ["divisionId", "categoryId", "floorId"]

    for k <- 1..3 do
      where =
        ~s|"firmId" = 'F0' AND | <>
          (cols
           |> Enum.take(k)
           |> Enum.map_join(" AND ", fn c -> inlist(c, String.first(c), 0, 10) end)) <>
          ~s| AND "deletedAt" IS NULL|

      f = Filter.new()
      {us, f} = :timer.tc(fn -> Filter.add_shape(f, "x", shape(where)) end)
      [{wr, wkb}, {er, _}, _, _] = mem(f)
      row("#{k} x IN(10)", [wr, er, wkb, Float.round(us / 1000, 1)])
    end
  end

  @tag :performance
  test "case 3: scaling law in IN-list size for ONE shape" do
    IO.puts("\n### case 3: single shape, IN(n), residual ###")
    row("n", ["where_rows", "eq_rows", "total_KB", "KB/elem", "add_ms", "ms/elem"])

    for n <- [5, 10, 20, 40, 80, 160, 320] do
      where =
        ~s|"firmId" = 'F0' AND | <>
          inlist("divisionId", "b", 0, n) <> ~s| AND "deletedAt" IS NULL AND | <> @token

      f = Filter.new()
      {us, f} = :timer.tc(fn -> Filter.add_shape(f, "x", shape(where)) end)
      [{wr, _}, {er, _}, _, _] = mem(f)
      kb = total_kb(f)

      row("#{n}", [
        wr,
        er,
        kb,
        Float.round(kb / n, 2),
        Float.round(us / 1000, 2),
        Float.round(us / 1000 / n, 4)
      ])
    end
  end

  @tag :performance
  test "case 4: many shapes sharing divisions (overlap)" do
    IO.puts("\n### case 4: 200 shapes over a shared 400-division firm ###")
    row("divisions/shape", ["where_rows", "eq_rows", "total_KB", "build_ms"])

    for n <- [10, 45, 100, 200] do
      f0 = Filter.new()

      {us, f} =
        :timer.tc(fn ->
          Enum.reduce(1..200, f0, fn i, f ->
            # overlapping windows over 400 divisions, wrapping
            offset = rem(i * 7, 400)

            where =
              ~s|"firmId" = 'F0' AND | <>
                inlist("divisionId", "b", offset, n) <>
                ~s| AND "deletedAt" IS NULL AND | <> @token

            Filter.add_shape(f, "h#{i}", shape(where))
          end)
        end)

      [{wr, _}, {er, _}, _, _] = mem(f)
      row("IN(#{n})", [wr, er, total_kb(f), Float.round(us / 1000, 1)])
    end
  end

  @tag :performance
  test "case 5: residual term size, measured" do
    IO.puts("\n### case 5: what is actually stored at each leaf ###")

    where =
      ~s|"firmId" = 'F0' AND | <>
        inlist("divisionId", "b", 0, 45) <> ~s| AND "deletedAt" IS NULL AND | <> @token

    f = Filter.add_shape(Filter.new(), "x", shape(where))

    rows = :ets.tab2list(f.where_cond_table)
    ws = :erlang.system_info(:wordsize)

    leaf_rows = :ets.tab2list(f.other_shapes_table)
    leaves = leaf_rows |> Enum.uniq_by(fn {condition_id, _, _, _} -> condition_id end)

    IO.puts("  where_cond rows: #{length(rows)}  (leaves with other_shapes: #{length(leaves)})")

    {_condition_id, shape_id, branch_key, residual} = hd(leaf_rows)
    key = {shape_id, branch_key}

    IO.puts("  leaf key:      #{inspect(key) |> String.slice(0, 120)}")
    IO.puts("  residual size: #{:erts_debug.size(residual) * ws} bytes (flat)")
    IO.puts("  key size:      #{:erts_debug.size(key) * ws} bytes")

    branch_bytes =
      leaf_rows
      |> Enum.map(fn {_, shape_id, branch_key, _} ->
        :erts_debug.size({shape_id, branch_key}) * ws
      end)
      |> Enum.sum()

    residual_bytes =
      leaf_rows
      |> Enum.map(fn {_, _, _, residual} -> :erts_debug.size(residual) * ws end)
      |> Enum.sum()

    IO.puts("  SUM branch keys over all leaves: #{branch_bytes} bytes")
    IO.puts("  SUM residuals   over all leaves: #{residual_bytes} bytes")
    IO.puts("  where_cond table total: #{total_kb(f)} KB (all 4 tables)")
  end

  test "NULL / three-valued logic through distribution" do
    # deletedAt IS NULL residual, record has non-null deletedAt -> must NOT match
    where =
      ~s|"firmId" = 'F0' AND | <>
        inlist("divisionId", "b", 0, 5) <> ~s| AND "deletedAt" IS NULL|

    f = Filter.add_shape(Filter.new(), "s", shape(where))

    live = %NewRecord{
      relation: {"public", "employees"},
      record: %{"id" => "e", "firmId" => "F0", "divisionId" => "b3", "deletedAt" => nil}
    }

    deleted = %NewRecord{
      relation: {"public", "employees"},
      record: %{
        "id" => "e",
        "firmId" => "F0",
        "divisionId" => "b3",
        "deletedAt" => "2026-01-01 00:00:00Z"
      }
    }

    null_division = %NewRecord{
      relation: {"public", "employees"},
      record: %{"id" => "e", "firmId" => "F0", "divisionId" => nil, "deletedAt" => nil}
    }

    assert Filter.affected_shapes(f, live) |> Enum.sort() == ["s"]
    assert Filter.affected_shapes(f, deleted) |> Enum.sort() == []
    assert Filter.affected_shapes(f, null_division) |> Enum.sort() == []
  end

  test "OR of two different fields, overlapping match" do
    # divisionId in list AND categoryId in list -- record matches both -> single id
    where =
      ~s|"firmId" = 'F0' AND (| <>
        ~s|"divisionId" = 'b1' OR "categoryId" = 'c1') AND "deletedAt" IS NULL|

    f = Filter.add_shape(Filter.new(), "s", shape(where))

    both = %NewRecord{
      relation: {"public", "employees"},
      record: %{
        "id" => "e",
        "firmId" => "F0",
        "divisionId" => "b1",
        "categoryId" => "c1",
        "deletedAt" => nil
      }
    }

    assert Filter.affected_shapes(f, both) |> Enum.sort() == ["s"]
  end

  test "removal is exact for the pathological cross product" do
    where =
      ~s|"firmId" = 'F0' AND | <>
        inlist("divisionId", "b", 0, 20) <>
        " AND " <> inlist("categoryId", "c", 0, 20) <> ~s| AND "deletedAt" IS NULL|

    empty = Filter.new()
    base = mem(empty)
    f = Filter.add_shape(empty, "x", shape(where))
    f = Filter.remove_shape(f, "x")
    assert mem(f) == base
  end

  describe "the residual copied into each distributed branch" do
    defp record(division, floor, deleted_at \\ nil) do
      %NewRecord{
        relation: {"public", "employees"},
        record: %{
          "id" => "e",
          "firmId" => "F0",
          "divisionId" => division,
          "floorId" => floor,
          "deletedAt" => deleted_at
        }
      }
    end

    defp affected(filter, change), do: filter |> Filter.affected_shapes(change) |> Enum.sort()

    test "keeps a long IN list beside a long residual unindexed, and routes it" do
      residual = Enum.map_join(1..200, " AND ", &~s|"floorId" <> 'f#{&1}'|)
      shape = shape(inlist("divisionId", "b", 0, 1000) <> " AND " <> residual)

      refute WhereCondition.indexed_where?(shape.where)

      filter = Filter.add_shape(Filter.new(), "s", shape)

      assert affected(filter, record("b7", "f0")) == ["s"]
      assert affected(filter, record("b7", "f9")) == []
      assert affected(filter, record("b1001", "f0")) == []
    end

    test "indexes the full disjunct count beside a short residual" do
      residual = ~s|"deletedAt" IS NULL AND ("floorId" <> 'f1' OR "floorId" IS NULL)|
      shape = shape(inlist("divisionId", "b", 0, 1024) <> " AND " <> residual)

      assert WhereCondition.indexed_where?(shape.where)

      filter = Filter.add_shape(Filter.new(), "s", shape)

      assert affected(filter, record("b1024", "f0")) == ["s"]
      assert affected(filter, record("b1024", "f1")) == []
      assert affected(filter, record("b1024", "f0", "2026-01-01 00:00:00Z")) == []
    end
  end
end
