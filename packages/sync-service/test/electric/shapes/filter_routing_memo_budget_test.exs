defmodule Electric.Shapes.FilterRoutingMemoBudgetTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["notes"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "value", type: "text"}
               ]
             )

  # Wide text keys retain reference-counted binaries that `:ets.info(:memory)` does not count, so a
  # budget measured on ETS table memory never bites and the memo grows unbounded. With the budget
  # accounting for the retained binaries, feeding many distinct wide values past 32 MiB forces the
  # memo to reset, keeping its entry count well below the number of distinct keys seen.
  test "the routing memo evicts once its retained binary bytes exceed the budget" do
    filter =
      Filter.new(filter_routing_memo: true)
      |> Filter.add_shape(
        "s1",
        Shape.new!("notes", where: "value = 'match'", inspector: @inspector)
      )

    record_count = 150
    value_bytes = 512 * 1024

    for i <- 1..record_count do
      value = <<i::unsigned-32>> <> :binary.copy("x", value_bytes)
      change = %NewRecord{relation: {"public", "notes"}, record: %{"value" => value}}
      {shapes, _exact?} = Filter.affected_shapes_exact(filter, change)
      assert MapSet.size(shapes) == 0
    end

    memo_size = :ets.info(filter.route_memo_table, :size)

    assert memo_size > 0
    assert memo_size < record_count
  end
end
