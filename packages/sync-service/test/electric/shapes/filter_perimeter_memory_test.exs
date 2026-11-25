defmodule Electric.Shapes.FilterPerimeterMemoryTest do
  @moduledoc """
  Quantifies what a multi-division filtered shape costs the router, because at high shape counts
  their materialisation can exhaust boot memory.

  A multi-division filter is `"firmId" = 'F0' AND "divisionId" IN (<n divisions>)`. The parser
  expands `IN` into a left-nested chain of `or` over `=`, and the router then decomposes
  that chain into one equality-index entry per division. Both are deliberate and both buy
  routing speed; neither cost is visible from the clause text, which is why it is asserted
  here rather than left to be rediscovered as a memory regression.
  """
  use ExUnit.Case, async: true

  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["employees"],
               columns: [
                 %{name: "id", type: "text", pk_position: 0},
                 %{name: "firmId", type: "text"},
                 %{name: "divisionId", type: "text"}
               ]
             )

  @wordsize :erlang.system_info(:wordsize)

  defp perimeter_where(division_count) do
    divisions =
      Enum.map_join(1..division_count, ", ", &"'dv#{String.pad_leading("#{&1}", 21, "0")}'")

    ~s|"firmId" = 'F0' AND "divisionId" IN (#{divisions})|
  end

  defp perimeter_shape(division_count) do
    Shape.new!("employees", inspector: @inspector, where: perimeter_where(division_count))
  end

  defp router_tables(shape) do
    filter = Filter.new()
    Filter.add_shape(filter, "handle", shape)

    [
      shapes: filter.shapes_table,
      tables: filter.tables_table,
      where_cond: filter.where_cond_table,
      other_shapes: filter.other_shapes_table,
      eq_index: filter.eq_index_table,
      incl_index: filter.incl_index_table,
      relation_index: filter.relation_index_table
    ]
    |> Map.new(fn {name, table} -> {name, :ets.info(table, :memory) * @wordsize} end)
    |> then(fn tables ->
      # a condition's state is its row plus the residual clauses filed under it
      %{tables | where_cond: tables.where_cond + tables.other_shapes}
    end)
  end

  defp marginal(large, small, span, key), do: div(large[key] - small[key], span)

  test "a firm perimeter costs a bounded amount per division, and the cost is where it looks" do
    # Arrange
    small_count = 1
    # A table grows in steps, sized by its locking options. Over a short span one step weighs
    # on every division: 200 divisions measure 264 or 449 bytes of where-condition state by
    # the options alone. From 400 on the options no longer show.
    large_count = 800
    span = large_count - small_count

    # Act
    small = router_tables(perimeter_shape(small_count))
    large = router_tables(perimeter_shape(large_count))

    shape_per_division =
      div(
        :erts_debug.flat_size(perimeter_shape(large_count)) * @wordsize -
          :erts_debug.flat_size(perimeter_shape(small_count)) * @wordsize,
        span
      )

    per_table =
      [:shapes, :where_cond, :eq_index]
      |> Map.new(&{&1, marginal(large, small, span, &1)})

    # Assert: the clause text costs ~26 bytes per division. These bounds sit just above the
    # measured cost, so a representation regression fails here instead of at boot with many
    # shapes. Lowering them is the point of the follow-up work; raising one needs a reason.
    assert shape_per_division < 900,
           "a division costs #{shape_per_division} bytes in a copied shape"

    assert per_table.shapes < 20,
           "a division costs #{per_table.shapes} bytes of shapes_table"

    assert per_table.where_cond < 400,
           "a division costs #{per_table.where_cond} bytes of where-condition state"

    assert per_table.eq_index < 300,
           "a division costs #{per_table.eq_index} bytes of equality index"
  end
end
