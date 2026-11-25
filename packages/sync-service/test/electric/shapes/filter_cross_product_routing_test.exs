defmodule Electric.Shapes.FilterCrossProductRoutingTest do
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
                 %{name: "categoryId", type: "text"},
                 %{name: "deletedAt", type: "timestamptz"}
               ]
             )

  defp shape(where), do: Shape.new!("employees", where: where, inspector: @inspector)

  defp inlist(field, k) do
    ~s|"#{field}" IN | <> "(" <> Enum.map_join(1..k, ", ", &"'v#{&1}'") <> ")"
  end

  defp stats(f) do
    ws = :erlang.system_info(:wordsize)
    i = :ets.info(f.where_cond_table)
    {i[:size], Float.round(i[:memory] * ws / 1024, 1)}
  end

  test "routing stays correct despite fan-out" do
    # Arrange
    where = inlist("divisionId", 10) <> " AND " <> inlist("categoryId", 10)
    f = Filter.add_shape(Filter.new(), "s1", shape(where))

    rec = fn b, c ->
      %NewRecord{
        relation: {"public", "employees"},
        record: %{"id" => "e1", "divisionId" => b, "categoryId" => c, "deletedAt" => nil}
      }
    end

    # Act / Assert
    assert Filter.affected_shapes(f, rec.("v3", "v7")) |> MapSet.to_list() == ["s1"]
    assert Filter.affected_shapes(f, rec.("v3", "zzz")) |> MapSet.to_list() == []
    assert Filter.affected_shapes(f, rec.("zzz", "v7")) |> MapSet.to_list() == []
    assert Filter.remove_shape(f, "s1") |> stats() == {0, elem(stats(Filter.new()), 1)}
  end
end
