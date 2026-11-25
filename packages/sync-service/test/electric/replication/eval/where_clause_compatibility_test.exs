defmodule Electric.Replication.Eval.WhereClauseCompatibilityTest do
  @moduledoc """
  Pins the operators and functions a where clause may use. The parser refuses anything
  outside this set at shape creation — the shape is rejected and no rows are served — so
  the set is the fork's published where-clause compatibility surface.

  When this test fails the registry changed. Update both the pinned sets here and the
  supported/unsupported tables in `fork/docs/sync/guides/shapes.md`, so a client keeps
  seeing the true surface in the docs before it hits a 400.
  """
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.Env.KnownFunctions

  @functions ~w(
    array_append array_cat array_ndims array_prepend bool boolout bytea coalesce concat
    date dateout float4 float4out float8 float8out greatest ilike int2 int2out int4 int4out
    int8 int8out interval intervalout json jsonb justify_days justify_hours justify_interval
    least like lower numeric numericout text time timeout timestamp timestampout timestamptz
    timezone upper uuid uuidout
  )

  @operators ~w(!~~ !~~* # & && * + - / < <= <> <@ = > >= @ @> ^ | |/ || ~ ~~ ~~*)

  test "supported functions are exactly the published surface" do
    assert supported(KnownFunctions.known_functions()) == Enum.sort(@functions)
  end

  test "supported operators are exactly the published surface" do
    assert supported(KnownFunctions.known_operators()) == Enum.sort(@operators)
  end

  defp supported(registry) do
    registry
    |> Map.keys()
    |> Enum.map(fn {name, _arity} -> name |> to_string() |> String.trim(~s(")) end)
    |> Enum.uniq()
    |> Enum.sort()
  end
end
