defmodule Electric.Shapes.FragmentAssemblerReclaimTest do
  @moduledoc """
  An unblockable mark excludes a shape from framed-block membership. It must live exactly as long
  as the shape it names: leaving it behind on removal grows the set with historical churn and
  permanently defeats the `MapSet.size(unblockable) == 0` fast path the shared-block hot path
  relies on. `remove_shape/2` reclaims it.
  """
  use ExUnit.Case, async: true

  alias Electric.Shapes.FragmentAssembler

  test "remove_shape reclaims the removed shape's unblockable mark" do
    assembler = FragmentAssembler.new() |> FragmentAssembler.mark_unblockable(["s1", "s2"])

    assembler = FragmentAssembler.remove_shape(assembler, "s1")

    refute MapSet.member?(assembler.unblockable, "s1")
    assert MapSet.member?(assembler.unblockable, "s2")
  end
end
