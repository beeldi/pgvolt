defmodule Electric.Shapes.FilterPerimeterOverlapTest do
  use ExUnit.Case, async: false

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["employees"],
               columns: [
                 %{name: "id", type: "text", pk_position: 0},
                 %{name: "firmId", type: "text"},
                 %{name: "divisionId", type: "text"},
                 %{name: "deletedAt", type: "timestamptz"}
               ]
             )

  @token ~s|'cache-buster:00003' = 'cache-buster:00003'|

  defp shape(where), do: Shape.new!("employees", where: where, inspector: @inspector)

  defp restricted(divisions) do
    ~s|"firmId" = 'F0' AND "divisionId" IN (| <>
      Enum.map_join(divisions, ", ", &"'b#{&1}'") <>
      ~s|) AND "deletedAt" IS NULL AND | <> @token
  end

  defp change(division) do
    %NewRecord{
      relation: {"public", "employees"},
      record: %{"id" => "e1", "firmId" => "F0", "divisionId" => division, "deletedAt" => nil}
    }
  end

  @shape_count 120

  defp matching_shapes(firm_size) do
    1..@shape_count
    |> Enum.reduce(Filter.new(), fn i, f ->
      start = rem(i * 13, firm_size)
      divisions = Enum.map(0..44, &rem(start + &1, firm_size))
      Filter.add_shape(f, "h#{i}", shape(restricted(divisions)))
    end)
    |> Filter.affected_shapes(change("b1"))
    |> MapSet.size()
  end

  test "how far division perimeters overlap decides how many shapes a change routes to" do
    # Every shape covers 45 consecutive divisions of a firm of firm_size, so at 45 they all
    # cover the same 45 and one change routes to all of them. As the firm grows the
    # perimeters spread apart and the same change reaches progressively fewer.
    assert Enum.map([45, 100, 400, 1000], &matching_shapes/1) == [120, 52, 11, 4]
  end

  test "duplicate values inside the IN list" do
    f = Filter.add_shape(Filter.new(), "d", shape(restricted([1, 1, 2, 2, 1])))
    hit = Filter.affected_shapes(f, change("b1")) |> Enum.sort()
    miss = Filter.affected_shapes(f, change("b9")) |> Enum.sort()
    assert hit == ["d"]
    assert miss == []

    # Act
    f = Filter.remove_shape(f, "d")

    # Assert
    assert :ets.info(f.where_cond_table)[:size] == 0
    assert :ets.info(f.other_shapes_table)[:size] == 0
    assert :ets.info(f.eq_index_table)[:size] == 0
  end
end
