defmodule Electric.Shapes.Consumer.Subqueries.MoveBroadcast do
  # Builds the control messages that tell materializers which tag positions moved.

  alias Electric.Postgres.Inspector
  alias Electric.Replication.Eval.Parser.{Func, Ref, RowExpr}
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Consumer.Subqueries.RefResolver
  alias Electric.Shapes.Consumer.Subqueries.ShapeInfo
  alias Electric.Shapes.Shape
  alias Electric.Shapes.SubqueryTags

  @type move_value() :: {term(), term()}
  @type txid() :: pos_integer()

  def query_patterns?(plan, dep_index, {outs, ins}, view, :replace) do
    MapSet.member?(PgInterop.Sublink.keys(view), nil) or
      query_patterns?(plan, dep_index, outs ++ ins, view, :replace)
  end

  def query_patterns?(plan, dep_index, values, _view, kind) do
    scaled_tags?(plan, dep_index, kind) or
      (plan.dependency_polarities[dep_index] == :negated and
         (Enum.any?(values, fn {value, _} -> PgInterop.Sublink.null_key?(value) end) or
            Enum.any?(
              plan.dependency_positions[dep_index],
              &needs_composite_query?(plan.positions[&1], kind)
            )))
  end

  # A tag hashes the row's own text, and `numeric` text keeps its scale: a row keyed `1.00` does
  # not carry the tag of a moved `1.0`, which Postgres finds equal. A float keeps the sign of its
  # zero (`-0` and `0`), an interval its spelling (`1 day` and `24:00:00`), a `char(n)` its padding
  # and a `jsonb` value the scale of its numbers the same way. Only a query can name its tag.
  # A move-in snapshot sends every row the client lacks, so its tags matter only to a row the
  # client holds through another disjunct.
  defp scaled_tags?(plan, dep_index, kind) do
    (kind != :move_in or length(plan.disjuncts) > 1) and
      Enum.any?(plan.dependency_positions[dep_index], &scaled_key?(plan.positions[&1]))
  end

  defp scaled_key?(%{ast: %Func{args: [_, %Ref{type: {:array, type}}]}}), do: scaled_type?(type)
  defp scaled_key?(_), do: false

  defp scaled_type?(type) when type in [:numeric, :float4, :float8, :interval, :bpchar, :jsonb],
    do: true

  defp scaled_type?({:array, type}), do: scaled_type?(type)
  defp scaled_type?({:row, types}), do: Enum.any?(types, &scaled_type?/1)
  defp scaled_type?(_), do: false

  def allow_direct_text_pairs(plan, shape, stack_id) do
    positions =
      Map.new(plan.positions, fn
        {pos,
         %{
           negated?: true,
           dependency_index: dep_index,
           ast: %Func{
             args: [
               %RowExpr{
                 type: {:row, [:text, :text]},
                 elements: [%Ref{path: [a]}, %Ref{path: [b]}]
               },
               _
             ]
           }
         } = info} ->
          dependency = Enum.fetch!(shape.shape_dependencies, dep_index)
          inspector = Inspector.for_stack(stack_id)

          direct? =
            deterministic_columns?(shape, [a, b], inspector) and
              deterministic_columns?(
                dependency,
                dependency.explicitly_selected_columns,
                inspector
              )

          {pos, Map.put(info, :direct_text_pair?, direct?)}

        position ->
          position
      end)

    %{plan | positions: positions}
  end

  defp deterministic_columns?(shape, names, inspector) do
    case Inspector.load_column_info(shape.root_table_id, inspector) do
      {:ok, columns} ->
        Enum.all?(names, fn name ->
          Enum.any?(columns, &match?(%{name: ^name, deterministic_collation: true}, &1))
        end)

      _ ->
        false
    end
  end

  @integer_types [:int2, :int4, :int8]
  defp needs_composite_query?(
         %{ast: %Func{args: [%RowExpr{type: {:row, [a, b]}}, _]}},
         :move_out
       )
       when a in @integer_types and b in @integer_types,
       do: false

  defp needs_composite_query?(%{direct_text_pair?: true}, :move_out), do: false
  defp needs_composite_query?(%{tag_columns: {:hash_together, _}}, _), do: true
  defp needs_composite_query?(_, _), do: false

  def ambiguous_text_pair?(%ShapeInfo{shape: %{composite_tag_encoding: :length_prefixed}}, _, _),
    do: false

  def ambiguous_text_pair?(%ShapeInfo{dnf_plan: plan, shape: shape}, dep_index, values) do
    Enum.any?(plan.dependency_positions[dep_index], fn position ->
      case plan.positions[position] do
        %{direct_text_pair?: true, tag_columns: {:hash_together, [_col_a, col_b]}} = info ->
          Enum.any?(values, fn {_, value} ->
            Enum.any?(tag_values(info, shape, "move-out", value), fn {a, b} ->
              shared_tag_input?(col_b, a, b)
            end)
          end)

        _ ->
          false
      end
    end)
  end

  # A NULL first field has a fixed width. A non-NULL first field can end at
  # any column marker after its prefix, if the suffix encodes a second field.
  defp shared_tag_input?(_col_b, nil, _b), do: false

  defp shared_tag_input?(col_b, a, b) do
    separator = col_b <> ":"
    first = SubqueryTags.namespace_value(a)
    input = first <> separator <> SubqueryTags.namespace_value(b)
    original_split = byte_size(first)
    value_prefix = SubqueryTags.value_prefix()
    prefix_size = byte_size(value_prefix)
    null_suffix = separator <> SubqueryTags.null_sentinel()
    null_split = byte_size(input) - byte_size(null_suffix)

    new_null_split? =
      null_split >= prefix_size and null_split != original_split and
        String.ends_with?(input, null_suffix)

    new_null_split? or
      other_value_split?(input, separator <> value_prefix, prefix_size, original_split)
  end

  defp other_value_split?(input, marker, start, original_split) do
    case :binary.match(input, marker, scope: {start, byte_size(input) - start}) do
      {^original_split, _} ->
        other_value_split?(input, marker, original_split + 1, original_split)

      {_, _} ->
        true

      :nomatch ->
        false
    end
  end

  # Crossing the empty-set boundary flips the negated scalar predicate for a
  # NULL left key even when the changed right keys are all non-NULL.
  def include_null_tag([{value, _} | _] = values, _, _) when is_tuple(value), do: values

  def include_null_tag(values, before, after_move) do
    was_empty? = MapSet.size(before) == 0
    is_empty? = MapSet.size(after_move) == 0

    if was_empty? != is_empty?,
      do: [{nil, nil} | values],
      else: values
  end

  def effects_for_buffered_move(%{kind: :replace, patterns: nil} = move, shape_info) do
    {outs, ins} = move.values

    [
      make(shape_info, move.dep_index, outs, move.txids, move.txids8, "move-in"),
      make(shape_info, move.dep_index, ins, move.txids, move.txids8, "move-out")
    ]
  end

  def effects_for_buffered_move(%{kind: :replace, patterns: patterns} = move, _shape_info) do
    [
      control(patterns.move_in, move.txids, move.txids8, "move-in"),
      control(patterns.move_out, move.txids, move.txids8, "move-out")
    ]
  end

  def effects_for_buffered_move(active_move, %ShapeInfo{} = shape_info) do
    [
      case active_move.patterns do
        nil ->
          values =
            if shape_info.dnf_plan.dependency_polarities[active_move.dep_index] == :negated do
              ref = active_move.subquery_ref

              include_null_tag(
                active_move.values,
                active_move.views_before_move[ref],
                active_move.views_after_move[ref]
              )
            else
              active_move.values
            end

          make(
            shape_info,
            active_move.dep_index,
            values,
            active_move.txids,
            active_move.txids8,
            "move-in"
          )

        patterns ->
          event = if active_move.kind == :move_in, do: "move-in", else: "move-out"
          control(patterns, active_move.txids, active_move.txids8, event)
      end
    ]
  end

  # The patterns the move-out controls of these removals carry, by position, for
  # `removed_by?/3`: `removals` are the removed values by dependency, which leave the views from
  # `views` to `drained_views`. Nil when they carry none.
  @spec removal_patterns(ShapeInfo.t(), %{non_neg_integer() => [move_value()]}, map(), map()) ::
          %{non_neg_integer() => MapSet.t()} | nil
  def removal_patterns(%ShapeInfo{} = shape_info, removals, views, drained_views) do
    plan = shape_info.dnf_plan

    patterns =
      for {dep_index, values} <- removals,
          ref = RefResolver.ref_from_dep_index!(shape_info.ref_resolver, dep_index),
          values =
            if(plan.dependency_polarities[dep_index] == :negated,
              do: include_null_tag(values, views[ref], drained_views[ref]),
              else: values
            ),
          %{pos: pos, value: value} <- move_out_patterns(shape_info, dep_index, values),
          reduce: %{} do
        acc -> Map.update(acc, pos, MapSet.new([value]), &MapSet.put(&1, value))
      end

    if patterns != %{}, do: patterns
  end

  # Whether one of these tags carries a value those patterns name, so that their move-out
  # controls reach the row.
  @spec matched_by?([String.t()], %{non_neg_integer() => MapSet.t()}) :: boolean()
  def matched_by?(tags, patterns) do
    Enum.any?(tags, fn tag -> Enum.any?(tag_slots(tag), &pattern_slot?(&1, patterns)) end)
  end

  # Whether a row with these tags and active conditions leaves the shape once the move-out
  # controls of those patterns apply, as a client applying them finds: the positions whose value
  # a pattern names deactivate, and the row stays while one disjunct keeps each of its positions
  # active.
  @spec removed_by?([String.t()], [boolean()], %{non_neg_integer() => MapSet.t()}) :: boolean()
  def removed_by?(tags, active_conditions, patterns) do
    disjuncts = Enum.map(tags, &tag_slots/1)

    deactivated =
      for slots <- disjuncts,
          {pos, _} = slot <- slots,
          pattern_slot?(slot, patterns),
          into: MapSet.new(),
          do: pos

    MapSet.size(deactivated) > 0 and
      not Enum.any?(disjuncts, fn slots ->
        Enum.all?(slots, fn {pos, _} ->
          Enum.at(active_conditions, pos) == true and not MapSet.member?(deactivated, pos)
        end)
      end)
  end

  defp tag_slots(tag) do
    for {slot, pos} <- tag |> String.split("/") |> Enum.with_index(), slot != "", do: {pos, slot}
  end

  defp pattern_slot?({pos, slot}, patterns),
    do: patterns |> Map.get(pos, MapSet.new()) |> MapSet.member?(slot)

  defp move_out_patterns(shape_info, dep_index, values) do
    %Effects.AppendControl{message: %{headers: %{patterns: patterns}}} =
      make(shape_info, dep_index, values, [], [], "move-out")

    patterns
  end

  @spec effect_for_move_out(
          non_neg_integer(),
          [move_value()],
          [txid()],
          [txid()],
          ShapeInfo.t()
        ) :: %Effects.AppendControl{}
  def effect_for_move_out(dep_index, values, txids, txids8, %ShapeInfo{} = shape_info) do
    make(shape_info, dep_index, values, txids, txids8, "move-out")
  end

  defp make(
         %ShapeInfo{dnf_plan: plan, shape: shape, stack_id: stack_id, shape_handle: shape_handle},
         dep_index,
         values,
         txids,
         txids8,
         event
       ) do
    positions = Map.get(plan.dependency_positions, dep_index, [])
    encoding = Shape.composite_tag_encoding(shape)

    patterns =
      for pos <- positions,
          info = plan.positions[pos],
          {typed_value, original_value} <- values,
          info.negated? or not PgInterop.Sublink.null_key?(typed_value),
          tag_value <- tag_values(info, shape, event, original_value) do
        %{pos: pos, value: make_hash(info, stack_id, shape_handle, tag_value, encoding)}
      end

    control(patterns, txids, txids8, event)
  end

  # A complete RHS pair can only deactivate its own condition or that of a
  # matching LHS pair with NULL fields. An ordinary NOT NULL constraint can
  # be dropped without a logical replication event.
  defp tag_values(
         %{negated?: true, tag_columns: {:hash_together, [col_a, col_b]}},
         shape,
         "move-out",
         {a, b}
       ) do
    # For keyless tables root_pk contains every column as a synthetic key.
    pk = if shape.root_column_count > length(shape.root_pk), do: shape.root_pk, else: []

    for x <- with_possible_null(a, col_a in pk),
        y <- with_possible_null(b, col_b in pk),
        do: {x, y}
  end

  defp tag_values(_, _, _, value), do: [value]

  defp with_possible_null(value, true), do: [value]
  defp with_possible_null(value, false), do: [value, nil]

  defp control(patterns, txids, txids8, event) do
    headers = %{event: event, patterns: patterns, txids: txids}

    headers =
      if txids8 == [],
        do: headers,
        else: Map.put(headers, :txids8, Enum.map(txids8, &Integer.to_string/1))

    %Effects.AppendControl{message: %{headers: headers}}
  end

  defp make_hash(%{tag_columns: [_col]}, stack_id, shape_handle, value, _encoding) do
    SubqueryTags.make_value_hash(stack_id, shape_handle, value)
  end

  defp make_hash(
         %{tag_columns: {:hash_together, cols}},
         stack_id,
         shape_handle,
         original_value,
         encoding
       ) do
    fields = Enum.zip(cols, Tuple.to_list(original_value))
    SubqueryTags.make_composite_hash(stack_id, shape_handle, fields, encoding)
  end
end
