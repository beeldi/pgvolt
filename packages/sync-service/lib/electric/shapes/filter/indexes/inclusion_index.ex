defmodule Electric.Shapes.Filter.Indexes.InclusionIndex do
  @moduledoc """
  Efficiently finds shapes that are affected by a change when the shape's where clause has `array_field @> const_array` in it.

  The index is a tree stored in ETS. Each node in the tree represents a value in the array.

  When adding a shape to the tree, the shape's array is flattened, sorted and deduplicated, then first value is used to find the child node of the root node.
  The child to that node is then found using the next value and so on. When there are no values left, the shape is then added to the last node.
  Adding the shape to the last node is done by adding the shape the node's `WhereCondition` which represents the rest of the where
  clause of the shape (the `@>` comparison may be only part of the where clause) and can also contain many shapes.

  To find the shapes affected by a change, the values of the array in the change are flattened, sorted and deduplicated. The tree is then traversed using the values
  and any nodes that contain shapes on the way are added to the result set, because if the node has been reached the shape's array must be a subset of the change's array.

  A node is an array *element*, never a sub-array. Postgres's `@>`, `<@`, `&&` and `= ANY` compare
  element membership regardless of dimension, so `{{1,2},{3,4}} @> {1}` holds; the array parser
  returns a multidimensional value as nested lists, and leaving those nested would key the tree on
  `[1, 2]` where a shape is registered under `1`, routing no shapes to a record that matches. The
  same `List.flatten/1` the interpreter applies in `Env.KnownFunctions` is applied here so the
  index and the interpreter agree on membership.

  ## ETS Storage

  Tree nodes are stored in the incl_index_table with keys of the form:
  `{condition_id, field, path}` -> `%{child_count: non_neg_integer(), condition_id: condition_id | nil}`

  Where `path` is the list of array values traversed to reach this node (e.g., `[]` for root, `[1]`, `[1, 2]`, etc.).

  A node holds a count of its children rather than a list of their values. A child already IS the row
  `{condition_id, field, path ++ [value]}`, so the list duplicated what the table knows, and it was
  the largest thing in the row: maintaining it cost O(children) per insert - a full copy of the list
  in and out of ETS - and routing merged the change's array against it, O(children) per change, where
  the child count grows with the shape population. The count is enough for the two questions actually
  asked of it: is this node prunable, and is there any point descending.

  Additionally, the field type is cached at:
  `{:type, condition_id, field}` -> type
  This enables O(1) type lookup for parsing record values.

  A separate ordered set stores only paths with a where condition, keyed by
  `{condition_id, field, node_condition_id}`, each with the hash of every element. The bound key
  prefix lets a fallback scan just the paths for one field of one condition, while the trie keeps
  constant-time probes.

  What that scan costs is kept at:
  `{:scan_cost, condition_id, field}` -> the number of paths plus their elements
  so that routing can tell whether the trie walk or the scan is cheaper.
  """

  alias Electric.Replication.Eval.Env
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Filter.Indexes.IndexValue
  alias Electric.Shapes.Filter.WhereCondition

  @env Env.new()
  @extra_record_probes 4_096
  @path_batch_size 128
  @element_hash_range 4_294_967_296

  def add_shape(
        %Filter{incl_index_table: table} = filter,
        condition_id,
        shape_id,
        optimisation,
        branch_key
      ) do
    %{field: field, type: type, value: array_value, and_where: and_where} = optimisation
    :ets.insert(table, {{:type, condition_id, field}, type})

    values = canonical_elements(List.flatten(array_value), type)

    ctx = %{
      filter: filter,
      table: table,
      condition_id: condition_id,
      field: field,
      shape_id: shape_id,
      and_where: and_where,
      branch_key: branch_key
    }

    add_shape_to_node(ctx, [], values)
  end

  defp add_shape_to_node(ctx, path, [value | values]) do
    # There are still array values left, so don't add the shape to this node
    # Add it to a child or descendant node instead
    node_key = {ctx.condition_id, ctx.field, path}

    node = get_or_create_node(ctx.table, node_key)

    child_path = path ++ [value]
    child_key = {ctx.condition_id, ctx.field, child_path}

    case :ets.lookup(ctx.table, child_key) do
      [] ->
        :ets.insert(ctx.table, {child_key, %{child_count: 0, condition_id: nil}})
        :ets.insert(ctx.table, {node_key, %{node | child_count: node.child_count + 1}})

      _ ->
        :ok
    end

    add_shape_to_node(ctx, child_path, values)
  end

  defp add_shape_to_node(ctx, path, []) do
    # No more array values left, add the shape to this node
    node_key = {ctx.condition_id, ctx.field, path}

    node = get_or_create_node(ctx.table, node_key)

    node_condition_id =
      case node.condition_id do
        nil ->
          new_id = Filter.next_condition_id(ctx.filter)
          WhereCondition.init(ctx.filter, new_id)
          :ets.insert(ctx.table, {node_key, %{node | condition_id: new_id}})

          :ets.insert(
            ctx.filter.incl_index_paths_table,
            {{ctx.condition_id, ctx.field, new_id}, Enum.map(path, &element_hash/1), path}
          )

          add_scan_cost(ctx, path, 1)
          new_id

        existing_id ->
          existing_id
      end

    WhereCondition.add_shape(
      ctx.filter,
      node_condition_id,
      ctx.shape_id,
      ctx.and_where,
      ctx.branch_key
    )
  end

  defp get_or_create_node(table, node_key) do
    case :ets.lookup(table, node_key) do
      [] ->
        node = %{child_count: 0, condition_id: nil}
        :ets.insert(table, {node_key, node})
        node

      [{_, node}] ->
        node
    end
  end

  def remove_shape(
        %Filter{incl_index_table: table} = filter,
        condition_id,
        shape_id,
        optimisation,
        branch_key
      ) do
    %{field: field, type: type, value: array_value, and_where: and_where} = optimisation
    ordered = canonical_elements(List.flatten(array_value), type)

    ctx = %{
      filter: filter,
      table: table,
      condition_id: condition_id,
      field: field,
      shape_id: shape_id,
      and_where: and_where,
      branch_key: branch_key
    }

    remove_shape_from_node(ctx, [], ordered)

    root_key = {condition_id, field, []}

    case :ets.lookup(table, root_key) do
      [{_, root_node}] when root_node.child_count == 0 and root_node.condition_id == nil ->
        :ets.delete(table, root_key)
        :ets.delete(table, {:type, condition_id, field})
        :ets.delete(table, {:scan_cost, condition_id, field})
        :deleted

      _ ->
        :ok
    end
  end

  defp remove_shape_from_node(ctx, path, [value | values]) do
    # There are still array values left, so don't remove the shape from this node
    # Remove it from a child or descendant node instead
    child_path = path ++ [value]

    remove_shape_from_node(ctx, child_path, values)

    child_key = {ctx.condition_id, ctx.field, child_path}

    case :ets.lookup(ctx.table, child_key) do
      [{_, child_node}] ->
        if node_empty?(child_node) do
          :ets.delete(ctx.table, child_key)

          node_key = {ctx.condition_id, ctx.field, path}

          case :ets.lookup(ctx.table, node_key) do
            [{_, node}] ->
              :ets.insert(ctx.table, {node_key, %{node | child_count: node.child_count - 1}})

            [] ->
              :ok
          end
        end

      [] ->
        :ok
    end
  end

  defp remove_shape_from_node(ctx, path, []) do
    # No more array values left, remove the shape from this node
    node_key = {ctx.condition_id, ctx.field, path}

    case :ets.lookup(ctx.table, node_key) do
      [{_, %{condition_id: nil}}] ->
        :ok

      [{_, %{condition_id: node_condition_id} = node}] ->
        case WhereCondition.remove_shape(
               ctx.filter,
               node_condition_id,
               ctx.shape_id,
               ctx.and_where,
               ctx.branch_key
             ) do
          :deleted ->
            :ets.insert(ctx.table, {node_key, %{node | condition_id: nil}})

            :ets.delete(
              ctx.filter.incl_index_paths_table,
              {ctx.condition_id, ctx.field, node_condition_id}
            )

            add_scan_cost(ctx, path, -1)

          :ok ->
            :ok
        end

      [] ->
        :ok
    end
  end

  defp node_empty?(%{child_count: 0, condition_id: nil}), do: true
  defp node_empty?(_), do: false

  # A scanned path costs one row read plus one membership test per element.
  defp add_scan_cost(ctx, path, sign) do
    key = {:scan_cost, ctx.condition_id, ctx.field}
    :ets.update_counter(ctx.table, key, sign * (length(path) + 1), {key, 0})
  end

  def affected_shapes(%Filter{incl_index_table: table} = filter, condition_id, field, record) do
    case :ets.lookup(table, {:type, condition_id, field}) do
      [] ->
        MapSet.new()

      [{_, type}] ->
        case value_from_record(record, field, type) do
          {:ok, nil} ->
            MapSet.new()

          {:ok, values} when is_list(values) or is_struct(values, PgInterop.Array.Bounded) ->
            # Postgres never matches a NULL array element, even to another NULL, and its `@>`
            # reads no lower bound.
            sorted_values =
              values
              |> PgInterop.Array.elements()
              |> List.flatten()
              |> Enum.reject(&is_nil/1)
              |> canonical_elements(type)

            ctx = %{
              filter: filter,
              table: table,
              condition_id: condition_id,
              field: field,
              record: record
            }

            shapes_affected_by_record_array(ctx, sorted_values)

          # Postgres emits values this evaluator cannot read on types it accepts, so this is a
          # data condition. Raising reaches WhereCondition.affected_shapes's rescue, which formats
          # a stacktrace per change and widens to every shape under the condition, discarding the
          # exact answers of the other indexes on it.
          :error ->
            Filter.mark_routing_inexact()
            all_shape_ids(filter, condition_id, field)
        end
    end
  end

  defp shapes_affected_by_record_array(ctx, values) do
    # Allow the root, one probe per element and one full suffix walk, plus a fixed allowance,
    # or what a scan of the field's paths costs if that is more. A walk that ends within this is
    # the unbounded walk. One that does not has spent more than the scan it falls back to, so
    # the route takes at most twice the steps of the unbounded walk.
    # Discover conditions before evaluating them: a fallback can discard partial discovery
    # without repeating nested routing or changing its inexactness flag.
    scan_cost = :ets.lookup_element(ctx.table, {:scan_cost, ctx.condition_id, ctx.field}, 2, 0)
    budget = max(1 + 2 * length(values) + @extra_record_probes, scan_cost)

    case collect_tree_conditions(ctx, [], values, budget, []) do
      {_, conditions} ->
        Enum.reduce(Enum.reverse(conditions), MapSet.new(), &route_condition(ctx, &1, &2))

      :spent ->
        pattern = {{ctx.condition_id, ctx.field, :"$1"}, :"$2", :_}
        batch = :ets.match(ctx.filter.incl_index_paths_table, pattern, @path_batch_size)
        route_path_batches(ctx, batch, row_elements(values), MapSet.new())
    end
  end

  defp collect_tree_conditions(_ctx, _path, _values, 0, _conditions), do: :spent

  defp collect_tree_conditions(ctx, path, values, budget, conditions) do
    node_key = {ctx.condition_id, ctx.field, path}

    case :ets.lookup(ctx.table, node_key) do
      [] ->
        {budget - 1, conditions}

      [{_, node}] ->
        conditions = if node.condition_id, do: [node.condition_id | conditions], else: conditions
        collect_child_conditions(ctx, node.child_count, path, values, budget - 1, conditions)
    end
  end

  # Probe by row values so a short array does not scan a node's potentially many children.
  defp collect_child_conditions(_ctx, 0, _path, _values, budget, conditions),
    do: {budget, conditions}

  defp collect_child_conditions(_ctx, _child_count, _path, [], budget, conditions),
    do: {budget, conditions}

  defp collect_child_conditions(ctx, child_count, path, [value | values], budget, conditions) do
    with {budget, conditions} <-
           collect_tree_conditions(ctx, path ++ [value], values, budget, conditions) do
      collect_child_conditions(ctx, child_count, path, values, budget, conditions)
    end
  end

  defp route_path_batches(_ctx, :"$end_of_table", _values, shapes), do: shapes

  defp route_path_batches(ctx, {paths, continuation}, row, shapes) do
    # Canonicalisation sorts and deduplicates both arrays, so testing path membership reaches
    # the same conditions as looking for the path as a subsequence in the trie.
    shapes =
      Enum.reduce(paths, shapes, fn [id, hashes], shapes ->
        if row_path?(ctx, id, hashes, row),
          do: route_condition(ctx, id, shapes),
          else: shapes
      end)

    route_path_batches(ctx, :ets.match(continuation), row, shapes)
  end

  # A membership test hashes the value, which costs its size, and a shape's element can be far
  # longer than any of the row's. The scan tests the hashes stored with each path against the
  # row's, which costs no more than hashing the row's own values, as the walk does, and reads a
  # path itself only when every hash is the row's.
  defp row_elements(values) do
    {MapSet.new(values), Map.new(values, &{element_hash(&1), true})}
  end

  defp row_path?(ctx, id, hashes, {values, row_hashes}) do
    Enum.all?(hashes, &is_map_key(row_hashes, &1)) and
      ctx.filter.incl_index_paths_table
      |> :ets.lookup_element({ctx.condition_id, ctx.field, id}, 3)
      |> Enum.all?(&MapSet.member?(values, &1))
  end

  defp element_hash(value), do: :erlang.phash2(value, @element_hash_range)

  defp route_condition(ctx, condition_id, shapes) do
    MapSet.union(shapes, WhereCondition.affected_shapes(ctx.filter, condition_id, ctx.record))
  end

  defp value_from_record(record, field, type) do
    Env.parse_const(@env, record[field], type)
  end

  # The tree keys each node on an array element by term equality, so a where-clause element and the
  # record element Postgres calls equal must reduce to one term or the shape's node is never reached.
  # `IndexValue.index_value/2` is the same per-type canonicalisation the equality index routes on,
  # applied here per flattened element so both the stored path and the change's probe agree on
  # membership.
  defp canonical_elements(values, {:array, subtype}) do
    values
    |> Enum.map(&IndexValue.index_value(subtype, &1))
    |> Enum.sort()
    |> Enum.dedup()
  end

  defp canonical_elements(values, _type) do
    values |> Enum.sort() |> Enum.dedup()
  end

  def all_shape_ids(%Filter{incl_index_paths_table: table} = filter, condition_id, field) do
    table
    |> :ets.select([{{{condition_id, field, :"$1"}, :_, :_}, [], [:"$1"]}])
    |> Enum.reduce(MapSet.new(), fn node_condition_id, ids ->
      MapSet.union(ids, WhereCondition.all_shape_ids(filter, node_condition_id))
    end)
  end
end
