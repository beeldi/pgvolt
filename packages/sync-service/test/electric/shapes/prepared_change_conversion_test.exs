defmodule Electric.Shapes.PreparedChangeConversionTest do
  @moduledoc """
  The consumer has two paths from a routed fragment to storage: the multi-fragment write path,
  which reads a `PreparedChange` explicitly, and the single-fragment short-circuit through
  `TransactionBuilder` and `Shape.convert_change/3`. On the second path a `PreparedChange` once
  matched no clause; the consumer caught the raise and routed the shape to `stop_and_clean`, so
  the collector-side benchmark reported the destroyed work as a 4.4x speedup.

  The contract that keeps both paths equivalent: `convert_change/3` passes a `PreparedChange`
  through untouched, for every shape kind. Routing already decided membership when the change
  was prepared, so re-deciding here is at best redundant and at worst (the `where: nil,
  selects_all_columns` clause matches any struct; a where-clause shape matches none) fatal.
  """
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes.PreparedChange
  alias Electric.Shapes.Shape

  import Support.EmployeesFixtures, only: [inspector: 1, insert: 1, shape!: 2]

  @inspector inspector(~w[id firmId note])

  defp prepared_for(shape) do
    change = insert(%{"id" => "eq-1", "firmId" => "F0", "note" => "n"})
    [converted] = Shape.convert_change(shape, change)

    log_items =
      converted
      |> Electric.LogItems.from_change(100, Shape.pk(shape, change.relation), shape.replica)
      |> Enum.map(fn {offset, %{key: key, headers: %{operation: op}} = item} ->
        {offset, key, op, Jason.encode!(item)}
      end)

    %PreparedChange{
      change: converted,
      log_items: log_items,
      total_size: Enum.reduce(log_items, 0, fn {_, _, _, j}, acc -> acc + byte_size(j) end)
    }
  end

  test "a where-clause shape passes a PreparedChange through untouched" do
    # Arrange
    shape = shape!(@inspector, where: ~s|"firmId" = 'F0'|)
    prepared = prepared_for(shape)

    # Act
    result = Shape.convert_change(shape, prepared, stack_id: "probe", shape_handle: "h")

    # Assert
    assert result == [prepared]
  end

  test "a where-less select-all shape passes a PreparedChange through untouched" do
    # Arrange
    # This shape takes the `%{where: nil, flags: %{selects_all_columns: true}}` clause, which
    # matches any struct at all — the PreparedChange clause must sit in front of it, or the
    # prepared struct sails on to `LogItems.from_change` and crashes there instead.
    shape = shape!(@inspector, [])
    prepared = prepared_for(shape)

    # Act
    result = Shape.convert_change(shape, prepared, stack_id: "probe", shape_handle: "h")

    # Assert
    assert result == [prepared]
  end
end
