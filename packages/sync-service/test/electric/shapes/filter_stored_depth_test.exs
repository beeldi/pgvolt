defmodule Electric.Shapes.FilterStoredDepthTest do
  # A shape created before the parser's depth limit is restored with its clause. When the filter
  # drops the clause's tree, removing the shape parses its text again, and that parse must not
  # refuse what the shape was created with.
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.Parser
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["employees"],
               columns: [%{name: "id", type: "text", pk_position: 0}]
             )

  test "a stored clause past the depth limit is removed from the filter" do
    refs = %{["active"] => :bool}
    query = String.duplicate("NOT ", 99) <> "active"

    assert {:error, _} = Parser.parse_and_validate_expression(query, refs: refs)

    {:ok, where} = Parser.parse_and_validate_expression(query, refs: refs, max_depth: :infinity)

    shape = %{Shape.new!("employees", inspector: @inspector) | where: where}
    filter = Filter.add_shape(Filter.new(), :shape, shape)

    assert %{where: %{eval: :parse_from_query}} = Filter.get_shape(filter, :shape)

    Filter.remove_shape(filter, :shape)
    assert Filter.active_shapes(filter) == []
  end
end
