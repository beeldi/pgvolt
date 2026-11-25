defmodule Electric.Shapes.Filter do
  @moduledoc """
  Responsible for knowing which shapes are affected by a change.

  `affected_shapes(filter, change)` will return a set of IDs for the shapes that are affected by the change
  considering all the shapes that have been added to the filter using `add_shape/3`.


  The `Filter` module keeps track of what tables are referenced by the shapes and changes and delegates
  the table specific logic to the `Filter.WhereCondition` module.

  Data is stored in ETS tables (outside the process heap) to avoid GC pressure with large numbers of shapes.
  """

  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.DeletedRecord
  alias Electric.Replication.Changes.NewRecord
  alias Electric.Replication.Changes.Relation
  alias Electric.Replication.Changes.TruncatedRelation
  alias Electric.Replication.Changes.UpdatedRecord
  alias Electric.Replication.Eval.Env
  alias Electric.Replication.Eval.Expr
  alias Electric.Replication.Eval.Parser
  alias Electric.Shapes.DnfPlan
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Filter.Indexes.SubqueryIndex
  alias Electric.Shapes.Filter.RefScan
  alias Electric.Shapes.Filter.WhereCondition
  alias Electric.Shapes.Shape
  alias Electric.Telemetry.OpenTelemetry

  require Logger

  defstruct [
    :shapes_table,
    :tables_table,
    :where_cond_table,
    :other_shapes_table,
    :eq_index_table,
    :incl_index_table,
    :incl_index_paths_table,
    :subquery_index,
    :condition_ids,
    :relation_index_table,
    :route_memo_table,
    :route_fields_table,
    :route_memo_bytes,
    route_memo?: false
  ]

  # the environment `with_where_ast/1` reparses in
  @env Env.new()

  @type t :: %Filter{}
  @type shape_id :: any()

  # Condition ids identify nodes in the where-condition tree (table conditions,
  # equality/inclusion/subquery sub-conditions). They were `make_ref/0`s, but a
  # boxed reference costs ~3 words on every ETS row that embeds one, so they are
  # integers drawn from one atomic counter: the same uniqueness for ~0 bytes
  # (small ints are immediates stored inline in the key tuple), whichever process
  # draws them.
  @type condition_id :: pos_integer()

  # The process that owns the filter is its only writer, except during the collector's restore,
  # which spreads the build over a few processes (`ShapeLogCollector.Restore`). Each of them
  # writes the conditions of its own tables, so they never write the same row, but they do write
  # the same tables at once: hence public tables, with a lock per row group rather than one per
  # table.
  @table_opts [:public, write_concurrency: :auto]

  @spec new(keyword()) :: Filter.t()
  def new(opts \\ []) do
    route_memo? = Keyword.get(opts, :filter_routing_memo, false)

    %Filter{
      shapes_table: :ets.new(:filter_shapes, [:set | @table_opts]),
      tables_table: :ets.new(:filter_tables, [:set | @table_opts]),
      where_cond_table: :ets.new(:filter_where, [:set | @table_opts]),
      other_shapes_table: :ets.new(:filter_other_shapes, [:duplicate_bag | @table_opts]),
      eq_index_table: :ets.new(:filter_eq, [:set | @table_opts]),
      incl_index_table: :ets.new(:filter_incl, [:set | @table_opts]),
      incl_index_paths_table: :ets.new(:filter_incl_paths, [:ordered_set | @table_opts]),
      subquery_index: SubqueryIndex.new(Keyword.take(opts, [:stack_id])),
      condition_ids: :atomics.new(1, signed: false),
      relation_index_table: :ets.new(:filter_relation_index, [:ordered_set | @table_opts]),
      route_memo_table: if(route_memo?, do: :ets.new(:filter_route_memo, [:set | @table_opts])),
      route_fields_table:
        if(route_memo?, do: :ets.new(:filter_route_fields, [:set | @table_opts])),
      route_memo_bytes: if(route_memo?, do: :counters.new(1, [:atomics])),
      route_memo?: route_memo?
    }
  end

  @doc """
  Allocate a fresh, filter-unique condition id.
  """
  @spec next_condition_id(Filter.t()) :: condition_id()
  def next_condition_id(%Filter{condition_ids: ids}) do
    :atomics.add_get(ids, 1, 1)
  end

  @spec has_shape?(t(), shape_id()) :: boolean()
  def has_shape?(%Filter{shapes_table: table}, shape_handle) do
    :ets.member(table, shape_handle)
  end

  @spec active_shapes(t()) :: [shape_id()]
  def active_shapes(%Filter{shapes_table: table}) do
    :ets.select(table, [{{:"$1", :_}, [], [:"$1"]}])
  end

  @doc """
  Returns `true` when ShapeLogCollector can route the shape through any of its
  indexes instead of relying exclusively on `other_shapes` scans.
  """
  @spec indexed_shape?(Shape.t()) :: boolean()
  def indexed_shape?(%Shape{} = shape) do
    WhereCondition.indexed_where?(shape.where)
  end

  @doc """
  The analysis of the shape's where clause that `add_shape/3` starts from. A caller that needs both
  `indexed_shape?/1` and `add_shape/3` runs it once and passes it to `indexed_analysis?/1` and
  `add_shape/4`, as the collector's restore does for every stored shape.
  """
  @spec analyse_shape(Shape.t()) :: term()
  def analyse_shape(%Shape{} = shape), do: WhereCondition.optimise(shape.where)

  @doc """
  `indexed_shape?/1` from the shape's `analyse_shape/1`.
  """
  @spec indexed_analysis?(term()) :: boolean()
  def indexed_analysis?(analysis), do: WhereCondition.indexed_optimisation?(analysis)

  @doc """
  Add a shape for the filter to track.

  The `shape_id` can be any term you like to identify the shape. Whatever you use will be returned
  by `affected_shapes/2` when the shape is affected by a change.
  """
  @spec add_shape(Filter.t(), shape_id(), Shape.t()) :: Filter.t()
  def add_shape(%Filter{} = filter, shape_id, shape),
    do: add_shape(filter, shape_id, shape, analyse_shape(shape))

  @doc """
  `add_shape/3` for a shape whose `analyse_shape/1` the caller has run already.
  """
  @spec add_shape(Filter.t(), shape_id(), Shape.t(), term()) :: Filter.t()
  def add_shape(%Filter{} = filter, shape_id, shape, analysis) do
    if has_shape?(filter, shape_id), do: raise("duplicate shape #{shape_id}")

    :ets.insert(filter.shapes_table, {shape_id, without_where_ast(shape)})
    :ets.insert(filter.relation_index_table, relation_index_rows(shape_id, shape))

    where_cond_id = get_or_create_table_condition(filter, shape.root_table)

    WhereCondition.add_optimised_shape(filter, where_cond_id, shape_id, shape.where, analysis)
    maybe_register_subquery_shape(filter, shape_id, shape)
    track_routing_fields(filter, shape, +1)

    filter
  end

  defp maybe_register_subquery_shape(
         %Filter{subquery_index: index},
         shape_id,
         %Shape{shape_dependencies: [_ | _]} = shape
       ) do
    {:ok, plan} = DnfPlan.compile(shape)
    SubqueryIndex.register_shape(index, shape_id, plan)
  end

  defp maybe_register_subquery_shape(_filter, _shape_id, _shape), do: :ok

  # `Shape.is_affected_by_relation_change?/2` answers `false` for every shape whose
  # root table shares neither the OID nor the qualified name of the relation, so a
  # relation event only ever needs to consider those two buckets. Indexing both
  # keeps the rename cases (same OID under a new name, same name under a new OID)
  # reachable without visiting shapes on unrelated tables. The name row carries the
  # shape's OID so that the overwhelmingly common case - a relation that still has
  # the OID and the name the shape was built with - can be served from the OID
  # bucket alone.
  #
  # The shape id belongs in the KEY. A bag keyed on the bucket alone holds every shape on a table
  # under one key, and a bag insert scans that key's object list to reject duplicates, so
  # registering shape n costs O(n) and a whole population O(n squared). Measured over 40k shapes:
  # 7.3ms to register one more and 146s to build the filter, against 0.11ms and 12s once the id is
  # in the key. ordered_set keeps each bucket contiguous, so the reads below still seek.
  defp relation_index_rows(shape_id, %Shape{root_table_id: oid, root_table: name}) do
    [{{:oid, oid, shape_id}}, {{:name, name, shape_id}, oid}]
  end

  # Safe only while removal is the one thing here that reads a shape's clause back: membership,
  # relation-change checks and preparing a change for encoding all ignore it.
  #
  # `used_refs` holds the refs the evaluated tree kept, and a clause that folds a comparison away
  # -- `"firmId" = NULL` and friends -- drops that column from `used_refs` while the query text
  # still names it. `with_where_ast/1` reparses with `refs: used_refs`, so it rebuilds the clause
  # only while every column the query names still survives in `used_refs`; otherwise the reparse
  # hits an unknown reference and raises. Keeping the tree for exactly those folding clauses makes
  # that match true by construction.
  defp without_where_ast(%Shape{where: %Expr{} = where, shape_dependencies: []} = shape) do
    if reconstructs?(where), do: %{shape | where: %{where | eval: :parse_from_query}}, else: shape
  end

  defp without_where_ast(shape), do: shape

  # `used_refs` was folded out of this same query, so it is always a subset of the columns the
  # query names, and the reparse succeeds iff that subset is the whole set. `parse_query/1` returns
  # the raw PgQuery AST without the pure-Elixir validate pass, so reading the query's syntactic
  # column refs off it settles the same decision for a fraction of a full parse+validate.
  #
  # A restore asks this once per stored shape, so the text is read first: `RefScan` settles the
  # plain clauses without a parse and leaves the rest to it.
  #
  # The reparse also refuses a column Electric cannot cast. A shape stored before such filters
  # were refused is still restored with one, so its clause keeps its tree as well.
  #
  # The same holds for a clause this release parses differently from the one that stored it: a
  # restored shape is routed by the tree it was stored with, and a reparse could refuse it or
  # name other index keys for its removal than the ones it was added under.
  defp reconstructs?(%Expr{query: query, used_refs: used_refs} = where) do
    refs_known? =
      case RefScan.scan(query, used_refs) do
        :known -> true
        :unknown -> parsed_refs_known?(query, used_refs)
      end

    refs_known? and Env.uncastable_refs(used_refs, @env) == [] and
      (reads_the_same?(query, used_refs) or reparses_to_itself?(where))
  end

  # The parser now refuses a `COLLATE` outside `pg_catalog`, an interval that takes `ago` twice
  # and a comparison of arrays of json, jsonb or bytea, and folds `greatest`/`least` over
  # intervals by their span. A constant that folds away leaves no trace in the tree, so the text
  # and the column types are read instead, and a clause that names one of these is parsed in full
  # here, as its removal would parse it, to see whether that gives its tree back. Matching words
  # is coarse: a clause that only names one is parsed when it did not need to be.
  @reparsed_words ["collate", "greatest", "least", "interval", "ago", "json", "bytea"]
  @reparsed_types [:interval, :json, :jsonb, :bytea]

  defp reads_the_same?(query, used_refs) do
    not String.contains?(String.downcase(query), @reparsed_words) and
      not Enum.any?(used_refs, fn {_ref, type} -> reparsed_type?(type) end)
  end

  defp reparsed_type?({:array, type}), do: reparsed_type?(type)
  defp reparsed_type?(type), do: type in @reparsed_types

  defp reparses_to_itself?(%Expr{query: query, used_refs: used_refs, eval: eval}) do
    case Parser.parse_and_validate_expression(query, refs: used_refs, env: @env) do
      {:ok, %Expr{eval: reparsed}} -> without_locations(reparsed) === without_locations(eval)
      {:error, _} -> false
    end
  end

  # A tree's locations point into the text it was parsed from, and a shape keeps its query
  # normalised, not as the text it was created with. The trees compare with `===`: the index keys
  # `1` and `1.0` apart.
  defp without_locations(node) when is_map(node) do
    :maps.map(
      fn
        :location, _ -> 0
        _key, value -> without_locations(value)
      end,
      node
    )
  end

  defp without_locations([head | tail]), do: [without_locations(head) | without_locations(tail)]

  defp without_locations(node) when is_tuple(node),
    do: node |> Tuple.to_list() |> without_locations() |> List.to_tuple()

  defp without_locations(node), do: node

  defp parsed_refs_known?(query, used_refs) do
    case Parser.parse_query(query) do
      {:ok, ast} -> Enum.all?(query_column_refs(ast), &Map.has_key?(used_refs, &1))
      _ -> false
    end
  end

  # Walk the raw PgQuery tree exhaustively rather than through a node-typed walker: a node
  # type without a dedicated walker impl would silently drop its subtree, hide a ref, and let
  # `reconstructs?/1` strip a clause `with_where_ast/1` then cannot rebuild. A generic descent
  # cannot miss a `ColumnRef` whatever encloses it.
  defp query_column_refs(node, acc \\ [])

  defp query_column_refs(%PgQuery.ColumnRef{fields: fields}, acc),
    do: [Enum.map(fields, &Parser.unwrap_node_string/1) | acc]

  defp query_column_refs(node, acc) when is_struct(node),
    do: node |> Map.from_struct() |> query_column_refs(acc)

  defp query_column_refs(node, acc) when is_map(node),
    do: Enum.reduce(node, acc, fn {_key, value}, acc -> query_column_refs(value, acc) end)

  defp query_column_refs(node, acc) when is_list(node),
    do: Enum.reduce(node, acc, &query_column_refs/2)

  defp query_column_refs(node, acc) when is_tuple(node),
    do: node |> Tuple.to_list() |> query_column_refs(acc)

  defp query_column_refs(_leaf, acc), do: acc

  defp with_where_ast(
         %Shape{where: %Expr{eval: :parse_from_query, query: query, used_refs: used_refs}} = shape
       ) do
    # A shape restored from the store may predate the parser's depth limit.
    {:ok, where} =
      Parser.parse_and_validate_expression(query,
        refs: used_refs,
        env: @env,
        max_depth: :infinity
      )

    %{shape | where: where}
  end

  defp with_where_ast(shape), do: shape

  defp get_or_create_table_condition(filter, table_name) do
    case :ets.lookup(filter.tables_table, table_name) do
      [] ->
        where_cond_id = next_condition_id(filter)
        WhereCondition.init(filter, where_cond_id)
        :ets.insert(filter.tables_table, {table_name, where_cond_id})
        where_cond_id

      [{_, where_cond_id}] ->
        where_cond_id
    end
  end

  @doc """
  Remove a shape from the filter.
  """
  @spec remove_shape(Filter.t(), shape_id()) :: Filter.t()
  def remove_shape(%Filter{} = filter, shape_id) do
    [{_, stored}] = :ets.lookup(filter.shapes_table, shape_id)
    shape = with_where_ast(stored)
    table_name = shape.root_table

    [{_, where_cond_id}] = :ets.lookup(filter.tables_table, table_name)

    case WhereCondition.remove_shape(filter, where_cond_id, shape_id, shape.where) do
      :deleted -> :ets.delete(filter.tables_table, table_name)
      :ok -> :ok
    end

    maybe_unregister_subquery_shape(filter, shape_id, shape)

    Enum.each(
      relation_index_rows(shape_id, shape),
      &:ets.delete(filter.relation_index_table, elem(&1, 0))
    )

    :ets.delete(filter.shapes_table, shape_id)
    track_routing_fields(filter, shape, -1)

    filter
  end

  defp maybe_unregister_subquery_shape(
         %Filter{subquery_index: index},
         shape_id,
         %Shape{shape_dependencies: [_ | _]}
       ) do
    SubqueryIndex.unregister_shape(index, shape_id)
  end

  defp maybe_unregister_subquery_shape(_filter, _shape_id, _shape), do: :ok

  # Routing fails open when it cannot evaluate a predicate: it returns the shapes it cannot
  # rule out and lets the consumer's own re-evaluation, which fails closed, drop what does
  # not belong. The shared-conversion path has no such second pass — it takes routing as
  # membership — so it has to know when a decision came from that safety net. Process-scoped
  # because routing one change runs start to finish in one process; the alternative is
  # threading a flag out of every index implementation on the hot path.
  @inexact_key :electric_filter_routing_inexact

  @doc """
  Like `affected_shapes/2`, and also reports whether the decision is exact.

  Callers that skip the consumer-side re-evaluation must not share a conversion across an
  inexact decision.
  """
  @spec affected_shapes_exact(Filter.t(), Changes.change()) :: {MapSet.t(shape_id()), boolean()}
  def affected_shapes_exact(%Filter{} = filter, change) do
    Process.delete(@inexact_key)
    shapes = affected_shapes(filter, change)
    {shapes, Process.get(@inexact_key) != true}
  end

  @doc false
  def mark_routing_inexact, do: Process.put(@inexact_key, true)

  @doc """
  Returns the shape IDs for all shapes that have been added to the filter
  that are affected by the given change.
  """
  @spec affected_shapes(Filter.t(), Changes.change() | Relation.t()) ::
          MapSet.t(shape_id())
  def affected_shapes(%Filter{} = filter, change) do
    OpenTelemetry.timed_fun("filter.affected_shapes.duration_us", fn ->
      try do
        shapes_affected_by_change(filter, change)
      catch
        kind, error ->
          Logger.error("""
          Unexpected error in Filter.affected_shapes:
          #{Exception.format(kind, error, __STACKTRACE__)}
          """)

          OpenTelemetry.record_exception(kind, error, __STACKTRACE__)

          # We can't tell which shapes are affected, the safest thing to do is return all shapes
          mark_routing_inexact()
          all_shape_ids(filter)
      end
    end)
  end

  defp shapes_affected_by_change(%Filter{} = filter, %Relation{} = relation) do
    filter
    |> relation_candidates(relation)
    |> Enum.reduce(MapSet.new(), fn shape_id, affected ->
      case :ets.lookup(filter.shapes_table, shape_id) do
        [{_, shape}] ->
          if Shape.is_affected_by_relation_change?(shape, relation),
            do: MapSet.put(affected, shape_id),
            else: affected

        [] ->
          affected
      end
    end)
  end

  defp shapes_affected_by_change(%Filter{} = filter, %NewRecord{
         relation: relation,
         record: record
       }) do
    shapes_affected_by_record(filter, relation, record)
  end

  defp shapes_affected_by_change(%Filter{} = filter, %DeletedRecord{
         relation: relation,
         old_record: record
       }) do
    shapes_affected_by_record(filter, relation, record)
  end

  defp shapes_affected_by_change(%Filter{} = filter, %UpdatedRecord{relation: relation} = change) do
    MapSet.union(
      shapes_affected_by_record(filter, relation, change.record),
      shapes_affected_by_record(filter, relation, change.old_record)
    )
  end

  defp shapes_affected_by_change(%Filter{} = filter, %TruncatedRelation{relation: table_name}) do
    shape_ids_for_table(filter, table_name)
  end

  # At storm fan-out the same routing decision is recomputed for every row: the decision is a
  # pure function of the columns the table's where clauses read (typically a few),
  # and a bulk write's rows take a handful of distinct values on them. Memoize per projected key —
  # on a synthetic bench of 22 shapes with 45-id IN lists, 16.5 µs per exact evaluation against
  # 0.26 µs per hit.
  #
  # The memo is sound only while routing is record-pure, so a table is only memoized when every
  # one of its shapes reads plain columns and none has shape dependencies — subquery membership
  # lives in mutable index state that a record key cannot see. Any add/remove on the table drops
  # its cached decisions.
  # Two bounds, because an entry holds a set of shape ids rather than a scalar: at wide fan-out
  # a few thousand entries are hundreds of megabytes, and at narrow fan-out a byte budget alone
  # would never bite. Whichever is reached first drops the table.
  @route_memo_cap 8192
  @route_memo_max_bytes 32 * 1024 * 1024

  defp shapes_affected_by_record(filter, table_name, record) do
    case routing_fields(filter, table_name) do
      :off ->
        compute_shapes_affected_by_record(filter, table_name, record)

      fields ->
        key = {table_name, Enum.map(fields, &Map.get(record, &1))}

        case :ets.lookup(filter.route_memo_table, key) do
          [{_, shapes}] ->
            shapes

          [] ->
            shapes = compute_shapes_affected_by_record(filter, table_name, record)

            # An inexact decision is a one-off fallback, not a routing fact: caching it
            # would make every later record with this key inherit the same fallback.
            unless Process.get(@inexact_key) == true do
              entry_bytes = memo_entry_refc_bytes(key)

              if :ets.info(filter.route_memo_table, :size) >= @route_memo_cap or
                   :ets.info(filter.route_memo_table, :memory) * :erlang.system_info(:wordsize) +
                     :counters.get(filter.route_memo_bytes, 1) + entry_bytes >=
                     @route_memo_max_bytes do
                :ets.delete_all_objects(filter.route_memo_table)
                :counters.put(filter.route_memo_bytes, 1, 0)
              end

              :ets.insert(filter.route_memo_table, {key, shapes})
              :counters.add(filter.route_memo_bytes, 1, entry_bytes)
            end

            shapes
        end
    end
  end

  defp compute_shapes_affected_by_record(filter, table_name, record) do
    case :ets.lookup(filter.tables_table, table_name) do
      [] ->
        MapSet.new()

      [{_, where_cond_id}] ->
        WhereCondition.affected_shapes(filter, where_cond_id, record)
    end
  end

  defp routing_fields(%Filter{route_memo?: false}, _table_name), do: :off

  defp routing_fields(%Filter{route_fields_table: table}, table_name) do
    case :ets.lookup(table, table_name) do
      [{_, fields, 0, _counts}] -> fields
      _ -> :off
    end
  end

  defp track_routing_fields(%Filter{route_memo?: false}, _shape, _delta), do: :ok

  defp track_routing_fields(%Filter{route_fields_table: table} = filter, shape, delta) do
    relation = shape.root_table

    {off, counts} =
      case :ets.lookup(table, relation) do
        [{_, _fields, off, counts}] -> {off, counts}
        [] -> {0, %{}}
      end

    {off, counts} =
      case shape_routing_fields(shape) do
        :off ->
          {off + delta, counts}

        {:ok, shape_fields} ->
          counts =
            Enum.reduce(shape_fields, counts, fn field, counts ->
              case Map.get(counts, field, 0) + delta do
                0 -> Map.delete(counts, field)
                n -> Map.put(counts, field, n)
              end
            end)

          {off, counts}
      end

    if off == 0 and counts == %{} and not :ets.member(filter.tables_table, relation) do
      :ets.delete(table, relation)
    else
      :ets.insert(table, {relation, Enum.sort(Map.keys(counts)), off, counts})
    end

    # Any shape change on the table changes what the cached decisions would be.
    freed_bytes =
      filter.route_memo_table
      |> :ets.select([{{{relation, :"$1"}, :_}, [], [:"$1"]}])
      |> Enum.reduce(0, fn values, acc -> acc + memo_entry_refc_bytes({relation, values}) end)

    :counters.sub(filter.route_memo_bytes, 1, freed_bytes)
    :ets.match_delete(filter.route_memo_table, {{relation, :_}, :_})
  end

  # `:ets.info(:memory)` counts the table's heap words but not the reference-counted binaries a
  # wide text key retains off-heap. Size an entry by those refc binaries so the byte budget bites
  # on real retention the table memory cannot see.
  defp memo_entry_refc_bytes({_table_name, values}) do
    Enum.reduce(values, 0, fn
      value, acc when is_binary(value) and byte_size(value) > 64 -> acc + byte_size(value)
      _value, acc -> acc
    end)
  end

  defp relation_candidates(%Filter{relation_index_table: table}, %Relation{id: oid} = relation) do
    matching_oid = :ets.select(table, [{{{:oid, oid, :"$1"}}, [], [:"$1"]}])

    matching_name_only =
      :ets.select(table, [
        {{{:name, {relation.schema, relation.table}, :"$1"}, :"$2"},
         [{:"/=", :"$2", {:const, oid}}], [:"$1"]}
      ])

    MapSet.new(matching_oid ++ matching_name_only)
  end

  # A shape routes purely on record columns only when its where clause references plain
  # top-level columns and it has no shape dependencies. A where-less shape reads no columns at
  # all: it matches every record of the table, which is still a pure (constant) decision.
  defp shape_routing_fields(%Shape{shape_dependencies: [_ | _]}), do: :off
  defp shape_routing_fields(%Shape{where: nil}), do: {:ok, []}

  defp shape_routing_fields(%Shape{where: %{used_refs: used_refs}}) do
    paths = Map.keys(used_refs)

    if Enum.all?(paths, &match?([_], &1)) do
      {:ok, Enum.map(paths, fn [column] -> column end)}
    else
      :off
    end
  end

  defp shape_routing_fields(%Shape{}), do: :off

  defp all_shape_ids(%Filter{} = filter), do: filter |> active_shapes() |> MapSet.new()

  defp shape_ids_for_table(%Filter{relation_index_table: table}, table_name) do
    table
    |> :ets.select([{{{:name, table_name, :"$1"}, :_}, [], [:"$1"]}])
    |> MapSet.new()
  end

  @doc """
  Get a shape by its ID. Used internally for where clause evaluation.
  """
  def get_shape(%Filter{shapes_table: table}, shape_id) do
    case :ets.lookup(table, shape_id) do
      [{_, shape}] -> shape
      [] -> nil
    end
  end

  @doc """
  Get the subquery index. Used by consumers to seed/update membership.
  """
  def subquery_index(%Filter{subquery_index: index}), do: index
end
