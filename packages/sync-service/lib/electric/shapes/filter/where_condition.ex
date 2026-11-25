defmodule Electric.Shapes.Filter.WhereCondition do
  @moduledoc """
  Responsible for knowing which shapes are affected by a change to a specific table.

  When `add_shape/4` is called, shapes are added to a tree stored in ETS. Each node on the tree represents
  an optimised (indexed) condition in the shape's where clause, with shapes that share an optimised condition
  being on the same branch.

  Each WhereCondition is identified by a unique condition id and stores:
  - `index_keys`: MapSet of {field_key, operation} tuples for indexed conditions, kept in
    the filter's `where_cond_table` as `{condition_id, index_keys}`
  - `other_shapes`: the where clauses of non-optimised shapes, kept in the filter's
    `other_shapes_table` (a `:duplicate_bag` keyed on the condition id) as one row per shape:
    `{condition_id, shape_id, branch_key, where_clause}`. One row per shape means an add writes
    its own clause and nothing else (a single aggregate row would be copied out of ETS and back
    on every add and remove), and the condition id as the key means routing still reads a node's
    clauses in one lookup.

  The logic for specific indexes (equality, inclusion) is handled by dedicated modules that also use ETS.
  """

  alias Electric.Replication.Eval.Expr
  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Parser.Const
  alias Electric.Replication.Eval.Parser.Func
  alias Electric.Replication.Eval.Parser.Ref
  alias Electric.Replication.Eval.Parser.RowExpr
  alias Electric.Replication.PostgresInterop.Casting
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Filter.Index
  alias Electric.Shapes.WhereClause
  alias Electric.Telemetry.OpenTelemetry

  require Logger

  def init(%Filter{where_cond_table: table}, condition_id) do
    :ets.insert(table, {condition_id, MapSet.new()})
  end

  @doc """
  Returns `true` when the WHERE clause can use the filter's indexed routing
  path instead of relying entirely on `other_shapes`.
  """
  @spec indexed_where?(Expr.t() | nil) :: boolean()
  def indexed_where?(where_clause), do: indexed_optimisation?(optimise(where_clause))

  @doc false
  # What `add_shape/4` makes of the clause before it stores the shape, for a caller that also needs
  # `indexed_where?/1`: it runs the analysis once, asks `indexed_optimisation?/1` and hands the
  # analysis on to `add_optimised_shape/5`.
  def optimise(where_clause), do: optimise_where(where_clause)

  @doc false
  def indexed_optimisation?(optimisation), do: optimisation != :not_optimised

  # Upper bound on the disjuncts a single `(a OR b) AND c` distribution may fan out to.
  # Beyond this the clause stays unindexed rather than allocating one ETS branch per disjunct.
  @max_distributed_disjuncts 1024

  # Upper bound on the array length a single `@>`/`<@` containment clause may index. The inclusion
  # index stores a shape's sorted array as a root-to-leaf path, one ETS node per prefix, so an
  # n-element array costs n(n+1)/2 stored path elements and an O(n^2) insert on the routing spine.
  # Beyond this the clause stays unindexed and routes through `other_shapes`, whose per-change cost
  # is linear in the array rather than quadratic at insert.
  @max_inclusion_elements 256

  def add_shape(%Filter{} = filter, condition_id, shape_id, where_clause) do
    add_shape(filter, condition_id, shape_id, where_clause, 1)
  end

  @doc false
  # `add_shape/4` with `optimise(where_clause)` already run
  def add_optimised_shape(%Filter{} = filter, condition_id, shape_id, where_clause, optimisation),
    do: add_optimised_shape(filter, condition_id, shape_id, where_clause, optimisation, 1)

  @doc false
  def add_shape(%Filter{} = filter, condition_id, shape_id, where_clause, branch_key) do
    optimisation = optimise_where(where_clause)
    add_optimised_shape(filter, condition_id, shape_id, where_clause, optimisation, branch_key)
  end

  defp add_optimised_shape(filter, condition_id, shape_id, where_clause, optimisation, branch_key) do
    case optimisation do
      :not_optimised ->
        add_shape_to_other_shapes(filter, condition_id, shape_id, branch_key, where_clause)

      {:or, disjuncts} ->
        disjuncts
        |> Enum.with_index()
        |> Enum.each(fn {disjunct, index} ->
          add_shape(filter, condition_id, shape_id, disjunct, {branch_key, index})
        end)

      %{operation: _} = optimisation ->
        add_shape_to_index(filter, condition_id, shape_id, optimisation, branch_key)
    end
  end

  defp add_shape_to_other_shapes(
         %Filter{other_shapes_table: table},
         condition_id,
         shape_id,
         branch_key,
         where_clause
       ) do
    :ets.insert(table, {condition_id, shape_id, branch_key, where_clause})
  end

  defp add_shape_to_index(
         %Filter{where_cond_table: table} = filter,
         condition_id,
         shape_id,
         optimisation,
         branch_key
       ) do
    [{_, index_keys}] = :ets.lookup(table, condition_id)
    key = {optimisation.field, optimisation.operation}
    :ets.insert(table, {condition_id, MapSet.put(index_keys, key)})

    Index.add_shape(filter, condition_id, shape_id, optimisation, branch_key)
  end

  @doc false
  defp optimise_where(nil), do: :not_optimised

  defp optimise_where(%Expr{eval: :parse_from_query}),
    do:
      raise("a shape the filter stored without its parsed clause was routed without restoring it")

  defp optimise_where(%Expr{eval: eval}), do: optimise_where(eval)

  defp optimise_where(%Func{name: name} = func) when name in ["or", "and"],
    do: func |> analyse() |> elem(0)

  defp optimise_where(%Func{
         name: ~s("="),
         args: [%Ref{path: [field], type: type}, %Const{value: value}]
       }) do
    %{operation: "=", field: field, type: type, value: value, and_where: nil}
  end

  defp optimise_where(%Func{
         name: ~s("="),
         args: [%Const{value: value}, %Ref{path: [field], type: type}]
       }) do
    %{operation: "=", field: field, type: type, value: value, and_where: nil}
  end

  defp optimise_where(%Func{
         name: ~s("@>"),
         args: [%Ref{path: [field], type: type}, %Const{value: value}]
       })
       when is_list(value) do
    inclusion_optimisation(field, type, value)
  end

  defp optimise_where(%Func{
         name: ~s("<@"),
         args: [%Const{value: value}, %Ref{path: [field], type: type}]
       })
       when is_list(value) do
    inclusion_optimisation(field, type, value)
  end

  # `a && ARRAY[v]` holds exactly when `v` is in `a`, which is what `a @> ARRAY[v]` says, so a
  # one-element overlap indexes as a one-element containment.
  defp optimise_where(%Func{
         name: ~s("&&"),
         args: [%Ref{path: [field], type: type}, %Const{value: [value]}]
       }) do
    inclusion_optimisation(field, type, [value])
  end

  # Overlap is satisfied by any single shared element, so `a && ARRAY[v1, …, vn]` is
  # `a @> [v1] OR … OR a @> [vn]` and each disjunct reaches the inclusion index. Without this the
  # whole clause is unindexed, and every shape under the same node is re-evaluated on every change
  # to the table - the array-access form of the IN-list case `distribute_and_over_or/3` handles.
  #
  # Each disjunct is the parsed node with its constant narrowed to one element rather than a node
  # built here, so it keeps the real implementation and types and still evaluates correctly if it
  # is ever reached outside the index.
  defp optimise_where(
         %Func{name: ~s("&&"), args: [%Ref{}, %Const{value: values} = const]} = overlap
       )
       when is_list(values) and values != [] and
              length(values) <= @max_distributed_disjuncts do
    {:or,
     values
     |> Enum.uniq()
     |> Enum.map(fn value ->
       where_expr(%{overlap | args: [hd(overlap.args), %{const | value: [value]}]})
     end)}
  end

  # const = ANY(array_ref) → reuse @> index with [const] as single-element array
  defp optimise_where(%Func{
         name: "any",
         args: [
           %Func{
             name: ~s("="),
             map_over_array_in_pos: 1,
             args: [%Const{value: value}, %Ref{path: [field], type: {:array, _} = type}]
           }
         ]
       })
       when not is_nil(value) do
    %{operation: "@>", field: field, type: type, value: [value], and_where: nil}
  end

  defp optimise_where(%Func{name: "sublink_membership_check"} = subquery) do
    subquery_optimisation(subquery, :positive)
  end

  defp optimise_where(%Func{
         name: "not",
         args: [%Func{name: "sublink_membership_check"} = subquery]
       }) do
    subquery_optimisation(subquery, :negated)
  end

  defp optimise_where(_), do: :not_optimised

  # A node's analysis, and whether the node may ride along in a distributed branch's residual, come
  # from one walk of the tree. Distribution is always sound; what it must not do is multiply out.
  # Only a node this optimiser would itself turn into `{:or, disjuncts}` can multiply, so a residual
  # may hold no such node outside an `or` the optimiser rejects: an `or` like
  # `a IS NULL OR b IS NOT NULL` is inert and rides along in each branch's residual, where it is
  # never re-entered because the branch's own indexable conjunct claims the clause and stores the
  # rest as `and_where`. Refusing on the mere presence of an `or` dropped whole clauses into
  # `other_shapes`, which is a per-change linear scan: measured at 20k shapes, an inert `or` in the
  # residual took routing from 5 µs flat to 555-23000 µs rising with the shapes on the node. A
  # multi-element `a && ARRAY[...]` is such a node too, though it is named `"&&"`.
  #
  # Asking the second question of a residual afresh analysed it again, which doubled the work at
  # every level a clause nests `AND` over `OR`: 18 levels, about 540 bytes of where clause, took
  # 160 ms. So the answer is built with the analysis, from the children's, and each node is analysed
  # once. It is only asked of a residual, so the right side of an `AND` whose left claims the clause
  # is not walked for it: `{:unwalked, trees}` stands for the subtrees still to analyse, and
  # `distributable?/1` analyses them when asked. In a multi-division filter,
  # `"firmId" = ? AND "divisionId" IN (...)`, the IN list would be analysed for nothing.
  defp analyse(%Expr{eval: eval}) when eval != :parse_from_query, do: analyse(eval)

  defp analyse(%Func{name: "or"} = or_func) do
    optimisation = optimise_or(or_func)
    {optimisation, optimisation == :not_optimised}
  end

  defp analyse(%Func{name: "and", args: [left, right]}) do
    case analyse(left) do
      {%{operation: _} = optimisation, left_distributable} ->
        {%{optimisation | and_where: merge_and_where(optimisation.and_where, right)},
         both(left_distributable, {:unwalked, [right]})}

      {_, left_distributable} = left_analysis ->
        {_, right_distributable} = right_analysis = analyse(right)

        {optimise_and(left, left_analysis, right, right_analysis),
         both(left_distributable, right_distributable)}
    end
  end

  defp analyse(%Func{name: "and", args: args}), do: {:not_optimised, {:unwalked, args}}

  defp analyse(%Func{args: args} = func) do
    case optimise_where(func) do
      {:or, _} = optimisation -> {optimisation, false}
      optimisation -> {optimisation, {:unwalked, args}}
    end
  end

  defp analyse(tree), do: {optimise_where(tree), true}

  # A node whose analysis is `{:or, _}` answers `false`, and an `AND` is `{:or, _}` only through a
  # child that is, so an `AND` answers for both its children and no more.
  defp both(false, _), do: false
  defp both(_, false), do: false
  defp both(true, distributable), do: distributable
  defp both(distributable, true), do: distributable
  defp both(left, right), do: {:both, left, right}

  defp distributable?(distributable) when is_boolean(distributable), do: distributable

  defp distributable?({:both, left, right}),
    do: distributable?(left) and distributable?(right)

  defp distributable?({:unwalked, trees}),
    do: Enum.all?(trees, fn tree -> tree |> analyse() |> elem(1) |> distributable?() end)

  defp optimise_or(or_func) do
    case flatten_or(or_func, @max_distributed_disjuncts) do
      :over ->
        :not_optimised

      disjuncts ->
        if Enum.all?(disjuncts, &(optimise_where(&1) != :not_optimised)) do
          {:or, Enum.map(disjuncts, &where_expr/1)}
        else
          :not_optimised
        end
    end
  end

  defp optimise_and(left, {left_optimisation, left_distributable}, right, right_analysis) do
    {right_optimisation, right_distributable} = right_analysis

    case {left_optimisation, right_optimisation} do
      {_, %{operation: _} = optimisation} ->
        %{optimisation | and_where: merge_and_where(left, optimisation.and_where)}

      # `(a OR b) AND c` distributes to `(a AND c) OR (b AND c)` so each disjunct can still reach an
      # index. Without this an `IN` list -- which parses to nested `or` -- drops the whole clause into
      # `other_shapes`, whose ETS row is then copied on every change to the parent condition.
      # Distribution is refused when it would multiply out: the disjunct count is bounded and the
      # residual carries no `or` this optimiser would itself distribute, otherwise
      # `a IN (n) AND b IN (m)` fans out to n*m branches.
      {{:or, disjuncts}, _} ->
        distribute_and_over_or(disjuncts, right, right_distributable, :residual_right)

      {_, {:or, disjuncts}} ->
        distribute_and_over_or(disjuncts, left, left_distributable, :residual_left)

      _ ->
        :not_optimised
    end
  end

  defp inclusion_optimisation(field, type, value) do
    if length(List.flatten(value)) <= @max_inclusion_elements do
      %{operation: "@>", field: field, type: type, value: value, and_where: nil}
    else
      :not_optimised
    end
  end

  # Each distributed branch stores its own copy of the residual on an ETS row, so the copies cost the
  # disjunct count times the residual's size. A residual of up to 16 nodes, a soft-delete test and a
  # row filter, say, stays indexed at the full 1,024 disjuncts, but a long one beside a long IN list
  # would store hundreds of thousands of nodes for a single shape. Past this budget the clause stays
  # unindexed, which is where upstream leaves every such clause.
  @max_distributed_nodes 16_384

  defp distribute_and_over_or(disjuncts, residual, distributable, side) do
    if distributable?(distributable) and
         nodes_left(residual, div(@max_distributed_nodes, length(disjuncts))) >= 0 do
      {:or,
       Enum.map(disjuncts, fn disjunct ->
         case side do
           :residual_right -> merge_and_where(disjunct, residual)
           :residual_left -> merge_and_where(residual, disjunct)
         end
       end)}
    else
      :not_optimised
    end
  end

  # The budget less the nodes of the tree, an array constant counting one node per element. Counting
  # stops once the budget is spent.
  defp nodes_left(_tree, budget) when budget < 0, do: budget
  defp nodes_left(%Expr{eval: eval}, budget), do: nodes_left(eval, budget)
  defp nodes_left(%Func{args: args}, budget), do: nodes_left(args, budget - 1)
  defp nodes_left(%{elements: elements}, budget), do: nodes_left(elements, budget - 1)

  defp nodes_left(%Const{value: value}, budget) when is_list(value),
    do: budget - 1 - length(List.flatten(value))

  defp nodes_left([], budget), do: budget
  defp nodes_left([head | tail], budget), do: nodes_left(tail, nodes_left(head, budget))
  defp nodes_left(_leaf, budget), do: budget - 1

  # Collapses a nested `or` tree into its disjuncts in one pass, giving up once the budget is
  # exhausted so a huge `IN` list stays unindexed rather than allocating one branch per element.
  defp flatten_or(eval, budget) do
    case flatten_or(eval, budget, [], 0) do
      :over -> :over
      {disjuncts, _count} -> disjuncts
    end
  end

  defp flatten_or(_eval, budget, _acc, count) when count >= budget, do: :over

  defp flatten_or(%Func{name: "or", args: [left, right]}, budget, acc, count) do
    case flatten_or(right, budget, acc, count) do
      :over -> :over
      {acc, count} -> flatten_or(left, budget, acc, count)
    end
  end

  # A non-binary `or` would otherwise fall to the catch-all below, which hands the SAME node back as
  # a lone disjunct -- and `optimise_where/1` re-enters on it, looping forever. The parser folds
  # `a OR b OR c` to nested binary ops but passes a one-argument `or` through untouched, so refuse
  # anything that is not binary and leave it unindexed, which is what upstream did for every arity.
  defp flatten_or(%Func{name: "or"}, _budget, _acc, _count), do: :over

  defp flatten_or(eval, _budget, acc, count), do: {[eval | acc], count + 1}

  defp where_expr(eval) do
    %Expr{eval: eval, used_refs: Parser.find_refs(eval), returns: :bool}
  end

  # A conjunct the parser already folded to a constant TRUE -- a cache-busting rotation token
  # is one -- carries no routing information, so it is dropped rather than copied into
  # every distributed branch's residual.
  defp merge_and_where(left, right), do: merge_conjuncts(drop_true(left), drop_true(right))

  defp drop_true(nil), do: nil

  defp drop_true(%Expr{eval: eval} = expr) do
    case drop_true(eval) do
      nil -> nil
      ^eval -> expr
      reduced -> where_expr(reduced)
    end
  end

  defp drop_true(%Const{value: true}), do: nil

  defp drop_true(%Func{name: "and", args: [left, right]} = func) do
    case {drop_true(left), drop_true(right)} do
      {nil, nil} -> nil
      {reduced, nil} -> reduced
      {nil, reduced} -> reduced
      {^left, ^right} -> func
      {left, right} -> %{func | args: [left, right]}
    end
  end

  defp drop_true(other), do: other

  defp merge_conjuncts(nil, nil), do: nil

  defp merge_conjuncts(left, nil), do: normalise_where(left)

  defp merge_conjuncts(nil, right), do: normalise_where(right)

  defp merge_conjuncts(left, right) do
    left_eval = extract_eval(left)
    right_eval = extract_eval(right)

    where_expr(%Func{
      args: [left_eval, right_eval],
      type: :bool,
      implementation: &Casting.pg_and/2,
      name: "and",
      strict?: false,
      location: min(Map.get(left_eval, :location, 0), Map.get(right_eval, :location, 0))
    })
  end

  defp normalise_where(%Expr{} = expr), do: expr
  defp normalise_where(eval), do: where_expr(eval)

  defp extract_eval(%Expr{eval: eval}), do: eval
  defp extract_eval(eval), do: eval

  defp subquery_optimisation(
         %Func{name: "sublink_membership_check", args: [testexpr, %Ref{path: subquery_ref}]} =
           _subquery,
         polarity
       ) do
    with {:ok, field_key} <- subquery_field_key(testexpr),
         {:ok, dep_index} <- dep_index_from_ref(subquery_ref) do
      %{
        operation: "subquery",
        field: field_key,
        testexpr: testexpr,
        subquery_ref: subquery_ref,
        dep_index: dep_index,
        polarity: polarity,
        and_where: nil
      }
    else
      _ -> :not_optimised
    end
  end

  @doc """
  Remove a shape from a WhereCondition.

  Returns `:deleted` if the condition is now empty and was deleted,
  or `:ok` if the condition still has shapes.
  """
  @spec remove_shape(Filter.t(), reference(), String.t(), Expr.t() | nil) :: :deleted | :ok
  def remove_shape(%Filter{} = filter, condition_id, shape_id, where_clause) do
    remove_shape(filter, condition_id, shape_id, where_clause, 1)
  end

  @doc false
  def remove_shape(%Filter{} = filter, condition_id, shape_id, where_clause, branch_key) do
    case optimise_where(where_clause) do
      :not_optimised ->
        remove_shape_from_other_shapes(filter, condition_id, shape_id, branch_key)

      {:or, disjuncts} ->
        disjuncts
        |> Enum.with_index()
        |> Enum.each(fn {disjunct, index} ->
          _ = remove_shape(filter, condition_id, shape_id, disjunct, {branch_key, index})
        end)

        condition_status(filter, condition_id)

      %{operation: _} = optimisation ->
        remove_shape_from_index(filter, condition_id, shape_id, optimisation, branch_key)
    end
  end

  defp remove_shape_from_other_shapes(
         %Filter{other_shapes_table: table} = filter,
         condition_id,
         shape_id,
         branch_key
       ) do
    # the ids are compared as constants: a shape id may be any term, a match variable included
    :ets.select_delete(table, [
      {{condition_id, :"$1", :"$2", :_},
       [
         {:andalso, {:"=:=", :"$1", {:const, shape_id}}, {:"=:=", :"$2", {:const, branch_key}}}
       ], [true]}
    ])

    delete_condition_if_empty(filter, condition_id)
  end

  defp remove_shape_from_index(
         %Filter{where_cond_table: table} = filter,
         condition_id,
         shape_id,
         optimisation,
         branch_key
       ) do
    case Index.remove_shape(filter, condition_id, shape_id, optimisation, branch_key) do
      :deleted ->
        case :ets.lookup(table, condition_id) do
          [] ->
            :deleted

          [{_, index_keys}] ->
            key = {optimisation.field, optimisation.operation}
            :ets.insert(table, {condition_id, MapSet.delete(index_keys, key)})
            delete_condition_if_empty(filter, condition_id)
        end

      :ok ->
        :ok
    end
  end

  defp condition_status(%Filter{where_cond_table: table}, condition_id) do
    case :ets.lookup(table, condition_id) do
      [] -> :deleted
      [_] -> :ok
    end
  end

  defp delete_condition_if_empty(%Filter{where_cond_table: table} = filter, condition_id) do
    case :ets.lookup(table, condition_id) do
      [] ->
        :deleted

      [{_, index_keys}] ->
        if MapSet.size(index_keys) == 0 and not has_other_shapes?(filter, condition_id) do
          :ets.delete(table, condition_id)
          :deleted
        else
          :ok
        end
    end
  end

  defp has_other_shapes?(%Filter{other_shapes_table: table}, condition_id) do
    :ets.member(table, condition_id)
  end

  def affected_shapes(%Filter{} = filter, condition_id, record) do
    MapSet.union(
      indexed_shapes_affected(filter, condition_id, record),
      other_shapes_affected(filter, condition_id, record)
    )
  rescue
    error ->
      Logger.error("""
      Unexpected error in Filter.WhereCondition.affected_shapes:
      #{Exception.format(:error, error, __STACKTRACE__)}
      """)

      # We can't tell which shapes are affected, the safest thing to do is return all shapes
      Filter.mark_routing_inexact()
      all_shape_ids(filter, condition_id)
  end

  defp indexed_shapes_affected(%Filter{where_cond_table: table} = filter, condition_id, record) do
    OpenTelemetry.with_child_span(
      "filter.filter_using_indexes",
      [],
      fn ->
        [{_, index_keys}] = :ets.lookup(table, condition_id)
        OpenTelemetry.add_span_attributes(index_count: MapSet.size(index_keys))

        index_keys
        |> Enum.map(fn {field, operation} ->
          Index.affected_shapes(filter, condition_id, field, operation, record)
        end)
        |> Enum.reduce(MapSet.new(), &MapSet.union(&1, &2))
      end
    )
  end

  defp other_shapes_affected(
         %Filter{subquery_index: index, other_shapes_table: table},
         condition_id,
         record
       ) do
    other_shapes = :ets.lookup(table, condition_id)

    OpenTelemetry.with_child_span(
      "filter.filter_other_shapes",
      [shape_count: length(other_shapes)],
      fn ->
        for {_condition_id, shape_id, _branch_key, where} <- other_shapes,
            other_shape_matches?(index, shape_id, where, record),
            into: MapSet.new() do
          shape_id
        end
      end
    )
  end

  defp other_shape_matches?(index, shape_id, where, record) do
    case WhereClause.includes_record_result(
           where,
           record,
           WhereClause.subquery_member_from_index(index, shape_id)
         ) do
      {:ok, included?} ->
        included?

      :error ->
        Filter.mark_routing_inexact()
        true
    end
  end

  defp subquery_field_key(%Ref{path: [field]}), do: {:ok, field}

  defp subquery_field_key(%RowExpr{elements: elements}) do
    if Enum.all?(elements, &match?(%Ref{path: [_]}, &1)) do
      {:ok, Enum.map(elements, fn %Ref{path: [field]} -> field end)}
    else
      :error
    end
  end

  defp subquery_field_key(_), do: :error

  defp dep_index_from_ref([_prefix, dep_index]) when is_binary(dep_index) do
    case Integer.parse(dep_index) do
      {idx, ""} -> {:ok, idx}
      _ -> :error
    end
  end

  defp dep_index_from_ref(_), do: :error

  def all_shape_ids(
        %Filter{where_cond_table: table, other_shapes_table: other_shapes_table} = filter,
        condition_id
      ) do
    case :ets.lookup(table, condition_id) do
      [] ->
        MapSet.new()

      [{_, index_keys}] ->
        index_shapes =
          Enum.reduce(index_keys, MapSet.new(), fn {field, operation}, acc ->
            MapSet.union(acc, Index.all_shape_ids(filter, condition_id, field, operation))
          end)

        other_shape_ids =
          other_shapes_table
          |> :ets.select([{{condition_id, :"$1", :_, :_}, [], [:"$1"]}])
          |> MapSet.new()

        MapSet.union(index_shapes, other_shape_ids)
    end
  end
end
