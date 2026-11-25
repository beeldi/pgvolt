defmodule Electric.Shapes.FilterOrArityTest do
  @moduledoc """
  `flatten_or/4` only matches `%Func{name: "or", args: [left, right]}`. Any other arity
  falls to the `flatten_or(eval, _budget, acc, count)` catch-all, which returns the SAME
  node back as a single disjunct; `optimise_where/1` then re-enters on it -> infinite loop.

  Upstream's `%Func{name: "or", args: [left, right]}` head simply fell through to
  `:not_optimised` for those shapes, so this is a hazard the patch introduces.
  """
  use ExUnit.Case, async: false

  alias Electric.Replication.Eval.Expr
  alias Electric.Replication.Eval.Parser.Const
  alias Electric.Replication.Eval.Parser.Func
  alias Electric.Replication.Eval.Parser.Ref
  alias Electric.Replication.PostgresInterop.Casting
  alias Electric.Shapes.Filter.WhereCondition

  defp eq(field, value) do
    %Func{
      name: ~s("="),
      args: [
        %Ref{path: [field], type: :text, location: 0},
        %Const{value: value, type: :text, location: 0}
      ],
      type: :bool,
      implementation: &Kernel.==/2,
      strict?: true,
      location: 0
    }
  end

  defp or_func(args) do
    %Func{
      name: "or",
      args: args,
      type: :bool,
      implementation: &Casting.pg_or/2,
      strict?: false,
      location: 0
    }
  end

  defp expr(eval), do: %Expr{eval: eval, used_refs: %{["divisionId"] => :text}, returns: :bool}

  test "binary or is fine (control)" do
    assert WhereCondition.indexed_where?(
             expr(or_func([eq("divisionId", "b1"), eq("divisionId", "b2")]))
           )
  end

  test "1-arity or terminates" do
    e = expr(or_func([eq("divisionId", "b1")]))

    task = Task.async(fn -> WhereCondition.indexed_where?(e) end)

    result =
      case Task.yield(task, 3_000) || Task.shutdown(task, :brutal_kill) do
        {:ok, v} -> {:returned, v}
        {:exit, reason} -> {:crashed, reason}
        nil -> :hung
      end

    assert {:returned, _} = result
  end

  test "3-arity or terminates" do
    e =
      expr(or_func([eq("divisionId", "b1"), eq("divisionId", "b2"), eq("divisionId", "b3")]))

    task = Task.async(fn -> WhereCondition.indexed_where?(e) end)

    result =
      case Task.yield(task, 3_000) || Task.shutdown(task, :brutal_kill) do
        {:ok, v} -> {:returned, v}
        {:exit, reason} -> {:crashed, inspect(reason) |> String.slice(0, 120)}
        nil -> :hung
      end

    assert {:returned, _} = result
  end
end
