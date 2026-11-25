defmodule Electric.Shapes.NegationMembershipLeakTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.{Decomposer, Parser, Runner}
  alias Electric.Shapes.{DnfPlan, Shape}

  # The membership-leak lives in Shape.compute_active_conditions, reached only on the subquery /
  # shared-conversion path (project_row_metadata with a compiled %DnfPlan{}). We build that plan
  # from the real Decomposer -- the same decomposition compile/1 uses -- so the negated position and
  # its raising inner predicate are genuine; only the shape_dependencies==[] guard (which decides
  # WHICH shapes get a plan, not the plan's semantics) is bypassed.
  #
  # WHERE NOT (1 / (x - 42) > 0):
  #   x=41 -> 1/-1=-1 > 0 false -> NOT false -> included   (control: negation includes)
  #   x=43 -> 1/1 = 1 > 0 true  -> NOT true  -> excluded   (control: negation excludes)
  #   x=42 -> 1/0 raises -> {:error,_} -> the row under test; fail-closed must EXCLUDE it.
  @where Parser.parse_and_validate_expression!("NOT (1 / (x - 42) > 0)", refs: %{["x"] => :int4})

  # A predicate over a NULL column value evaluates to SQL NULL, not a raise. Postgres 3VL makes
  # `NOT (NULL > 5)` unknown -> the row is excluded; the {:ok, nil} arm folded it to active?=false
  # then `not false`=true and ADMITTED it, the same shape of leak as the raising arm above.
  @where_null Parser.parse_and_validate_expression!("NOT (y > 5)", refs: %{["y"] => :int4})

  # Faithful DnfPlan from the real decomposition (mirrors DnfPlan.do_compile/enrich_positions for a
  # non-subquery position: subquery?/tag metadata are nil, negated? and ast come straight from the
  # Decomposer).
  defp plan, do: plan_for(@where.eval)

  defp plan_for(eval) do
    {:ok, d} = Decomposer.decompose(eval)

    positions =
      Map.new(d.subexpressions, fn {pos, subexpr} ->
        {pos,
         %{
           ast: subexpr.ast,
           sql: "",
           subquery?: false,
           negated?: subexpr.negated?,
           dependency_index: nil,
           subquery_ref: nil,
           tag_columns: nil
         }}
      end)

    %DnfPlan{
      disjuncts: d.disjuncts,
      disjuncts_positions: d.disjuncts_positions,
      position_count: d.position_count,
      positions: positions,
      dependency_positions: %{},
      dependency_disjuncts: %{},
      dependency_polarities: %{}
    }
  end

  defp included?(x) do
    {:ok, refs} = Runner.record_to_ref_values(@where.used_refs, %{"x" => Integer.to_string(x)})

    {:ok, inc, _tags, _active} =
      Shape.get_row_metadata(plan(), %{"x" => Integer.to_string(x)}, refs, @where, "s", "h")

    inc
  end

  defp null_included?(y) do
    record = %{"y" => y}
    {:ok, refs} = Runner.record_to_ref_values(@where_null.used_refs, record)

    {:ok, inc, _tags, _active} =
      Shape.get_row_metadata(plan_for(@where_null.eval), record, refs, @where_null, "s", "h")

    inc
  end

  test "the negated position is genuinely negated (guards the oracle)" do
    assert plan().position_count == 1
    assert plan().positions[0].negated? == true
  end

  test "an evaluable row the negation matches is included (control)" do
    assert included?(41) == true
  end

  test "an evaluable row the negation excludes is dropped (control)" do
    assert included?(43) == false
  end

  test "a row whose negated predicate RAISES is NOT admitted (fail-closed)" do
    # Upstream folds {:error,_} to base_result=false, then `not false`=true and ADMITS this row into
    # the negated shape -- the leak. Fail-closed membership returns false.
    assert included?(42) == false
  end

  test "the NULL-predicate position is genuinely negated (guards the oracle)" do
    assert plan_for(@where_null.eval).position_count == 1
    assert plan_for(@where_null.eval).positions[0].negated? == true
  end

  test "a row whose negated predicate is NULL is NOT admitted (fail-closed 3VL)" do
    # `NOT (NULL > 5)` is unknown, not true. The {:ok, nil} arm admitted it; fail-closed excludes it.
    assert null_included?(nil) == false
  end

  test "an evaluable row the NULL-guard negation excludes is dropped (control)" do
    assert null_included?("10") == false
  end

  test "an evaluable row the NULL-guard negation matches is included (control)" do
    assert null_included?("3") == true
  end
end
