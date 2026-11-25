defmodule Electric.Shapes.FilterStripDecisionTest do
  # Adding a shape decides whether its evaluated clause can be dropped and rebuilt from the query
  # text on removal. A restore asks that once per stored shape, and answering it with a parse of
  # the query was a large part of the restore with many shapes. The plain clauses are
  # settled by reading the text.
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Replication.Eval.Env
  alias Electric.Replication.Eval.Parser
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

  @plain ~s|"firmId" = 'F0' AND "divisionId" IN ('b1', 'b2') AND "deletedAt" IS NULL AND id < 'k' AND 'cache-buster:000083' = 'cache-buster:000083'|

  test "adding a shape with a plain clause does not parse its query" do
    {filter, parses} = add_counting_parses(@plain)

    assert parses == 0
    assert %{where: %{eval: :parse_from_query}} = Filter.get_shape(filter, :shape)

    Filter.remove_shape(filter, :shape)
    assert Filter.active_shapes(filter) == []
  end

  test "a clause the text does not settle is still settled by a parse" do
    {filter, parses} = add_counting_parses(~s|"firmId" = 'F0' AND id LIKE 'a%'|)

    assert parses == 1
    assert %{where: %{eval: :parse_from_query}} = Filter.get_shape(filter, :shape)

    Filter.remove_shape(filter, :shape)
    assert Filter.active_shapes(filter) == []
  end

  test "a clause that folded a column away keeps its evaluated tree" do
    {filter, parses} = add_counting_parses(~s|"firmId" = NULL AND "divisionId" = 'b1'|)

    assert parses == 1
    assert %{where: %{eval: eval}} = Filter.get_shape(filter, :shape)
    assert eval != :parse_from_query

    Filter.remove_shape(filter, :shape)
    assert Filter.active_shapes(filter) == []
  end

  # As a shape stored before where clauses over such a column were refused: removing it parses
  # its query again, and that parse now refuses it.
  test "a clause over a column Electric cannot cast keeps its evaluated tree" do
    {:ok, where} =
      Parser.parse_and_validate_expression(~s|"status" IS NULL|,
        refs: %{["status"] => {:enum, "status"}},
        env: %{Env.new() | allow_enums: true}
      )

    shape = %{Shape.new!("employees", inspector: @inspector) | where: where}
    filter = Filter.add_shape(Filter.new(), :shape, shape)

    assert %{where: %{eval: eval}} = Filter.get_shape(filter, :shape)
    assert eval != :parse_from_query

    Filter.remove_shape(filter, :shape)
    assert Filter.active_shapes(filter) == []
  end

  # As a shape stored by the release before, whose parser dropped any collation named `C`: this
  # one refuses `C` outside `pg_catalog`, so removing the shape cannot parse its query again.
  test "a clause stored with a collation this release refuses keeps its evaluated tree" do
    {:ok, where} =
      Parser.parse_and_validate_expression(~s|"firmId" = 'F0'|, refs: %{["firmId"] => :text})

    where = %{where | query: ~s|"firmId" COLLATE other."C" = 'F0'|}
    assert {:error, _} = Parser.parse_and_validate_expression(where.query, refs: where.used_refs)

    assert_removed_with_its_tree(where)
  end

  # Built in a subset's environment, which skips the array comparison check, as the release
  # before did for every clause.
  test "a clause stored with a comparison of jsonb arrays keeps its evaluated tree" do
    {:ok, where} =
      Parser.parse_and_validate_expression(~s|metas @> metas|,
        refs: %{["metas"] => {:array, :jsonb}},
        env: %{Env.new() | allow_enums: true}
      )

    assert {:error, _} = Parser.parse_and_validate_expression(where.query, refs: where.used_refs)

    assert_removed_with_its_tree(where)
  end

  # The release before folded this `greatest` by term order, to 29 days, and the shape was added
  # under that key. A reparse folds it to a month and would remove another key.
  test "a clause stored with a greatest of intervals is removed from the key it was added under" do
    refs = %{["period"] => :interval}

    {:ok, stored} =
      Parser.parse_and_validate_expression(~s|period = interval '29 days'|, refs: refs)

    stored = %{stored | query: ~s|period = greatest(interval '29 days', interval '1 month')|}

    {:ok, sibling} =
      Parser.parse_and_validate_expression(~s|period = interval '29 days'|, refs: refs)

    base = Shape.new!("employees", inspector: @inspector)
    filter = Filter.add_shape(Filter.new(), :stored, %{base | where: stored})
    filter = Filter.add_shape(filter, :sibling, %{base | where: sibling})

    Filter.remove_shape(filter, :stored)

    insert = %NewRecord{
      relation: {"public", "employees"},
      record: %{"id" => "1", "period" => "29 days"}
    }

    assert Filter.affected_shapes(filter, insert) == MapSet.new([:sibling])
  end

  # Folding drops the comparison from the tree, and `array_ndims` keeps the column among the
  # clause's references, so neither the tree nor the references show what a reparse refuses.
  test "a clause whose jsonb array comparison folded away keeps its evaluated tree" do
    {:ok, where} =
      Parser.parse_and_validate_expression(
        ~s|COALESCE(TRUE, metas @> metas) AND array_ndims(metas) = 1|,
        refs: %{["metas"] => {:array, :jsonb}},
        env: %{Env.new() | allow_enums: true}
      )

    assert {:error, _} = Parser.parse_and_validate_expression(where.query, refs: where.used_refs)

    assert_removed_with_its_tree(where)
  end

  # As a shape stored by the release before, which read `ago` any number of times: the interval
  # is assembled from pieces, in a branch the tree folded away.
  test "a clause stored with an interval this release refuses, built from pieces, keeps its evaluated tree" do
    {:ok, where} =
      Parser.parse_and_validate_expression(~s|"firmId" = 'F0'|, refs: %{["firmId"] => :text})

    where = %{
      where
      | query:
          ~s|"firmId" = 'F0' AND (TRUE OR ('1 day a'::text \|\| 'go a'::text \|\| 'go'::text)::interval > interval '1 day')|
    }

    assert {:error, _} = Parser.parse_and_validate_expression(where.query, refs: where.used_refs)

    assert_removed_with_its_tree(where)
  end

  # Parsed at add because it names an interval, it gives its tree back, so it is rebuilt as any
  # other clause.
  test "a clause over an interval that this release reads the same is still rebuilt from its text" do
    {:ok, where} =
      Parser.parse_and_validate_expression(
        ~s|"firmId" = 'F0' AND period IN (interval '1 day', interval '2 days')|,
        refs: %{["firmId"] => :text, ["period"] => :interval}
      )

    shape = %{Shape.new!("employees", inspector: @inspector) | where: where}
    filter = Filter.add_shape(Filter.new(), :shape, shape)

    assert %{where: %{eval: :parse_from_query}} = Filter.get_shape(filter, :shape)

    Filter.remove_shape(filter, :shape)
    assert Filter.active_shapes(filter) == []
  end

  test "a null test of a jsonb array column is still rebuilt from its text" do
    {:ok, where} =
      Parser.parse_and_validate_expression(~s|metas IS NULL|,
        refs: %{["metas"] => {:array, :jsonb}}
      )

    shape = %{Shape.new!("employees", inspector: @inspector) | where: where}
    filter = Filter.add_shape(Filter.new(), :shape, shape)

    assert %{where: %{eval: :parse_from_query}} = Filter.get_shape(filter, :shape)

    Filter.remove_shape(filter, :shape)
    assert Filter.active_shapes(filter) == []
  end

  # The release before folded this `greatest` by term order, so the `COALESCE` took the numeric
  # 1.0 and the shape was added under `[1.0]`. A reparse takes the integer 1: the two trees
  # compare equal with `==`, but the index keys `[1]` and `[1.0]` apart.
  test "a clause whose reparse folds to an integer where it was added under a float is removed from that key" do
    refs = %{["vals"] => {:array, :numeric}}

    {:ok, stored} = Parser.parse_and_validate_expression(~s|vals = ARRAY[1.0]|, refs: refs)

    stored = %{
      stored
      | query:
          ~s|vals = ARRAY[COALESCE((GREATEST(interval '29 days', interval '1 month') = interval '1 month' OR NULL)::int4, 1.0)]|
    }

    {:ok, sibling} = Parser.parse_and_validate_expression(~s|vals = ARRAY[1.0]|, refs: refs)

    base = Shape.new!("employees", inspector: @inspector)
    filter = Filter.add_shape(Filter.new(), :stored, %{base | where: stored})
    filter = Filter.add_shape(filter, :sibling, %{base | where: sibling})

    Filter.remove_shape(filter, :stored)

    insert = %NewRecord{
      relation: {"public", "employees"},
      record: %{"id" => "1", "vals" => "{1.0}"}
    }

    assert Filter.affected_shapes(filter, insert) == MapSet.new([:sibling])
  end

  defp assert_removed_with_its_tree(where) do
    shape = %{Shape.new!("employees", inspector: @inspector) | where: where}
    filter = Filter.add_shape(Filter.new(), :shape, shape)

    assert %{where: %{eval: eval}} = Filter.get_shape(filter, :shape)
    assert eval != :parse_from_query

    Filter.remove_shape(filter, :shape)
    assert Filter.active_shapes(filter) == []
  end

  defp add_counting_parses(where) do
    shape = Shape.new!("employees", where: where, inspector: @inspector)
    filter = Filter.new()
    parent = self()

    Repatch.patch(Parser, :parse_query, fn query ->
      send(parent, :parse_of_the_query)
      Repatch.real(Parser.parse_query(query))
    end)

    Filter.add_shape(filter, :shape, shape)

    {filter, count_parses(0)}
  end

  defp count_parses(count) do
    receive do
      :parse_of_the_query -> count_parses(count + 1)
    after
      0 -> count
    end
  end
end
