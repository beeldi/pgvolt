defmodule Electric.Shapes.Querying do
  alias Electric.Replication.Eval
  alias Electric.Replication.Eval.Expr
  alias Electric.Replication.Eval.Parser.Func
  alias Electric.Replication.Eval.SqlGenerator
  alias Electric.Replication.PostgresInterop.Casting
  alias Electric.ShapeCache.LogChunker
  alias Electric.Shapes.DnfPlan
  alias Electric.Shapes.Shape
  alias Electric.Shapes.SubqueryTags
  alias Electric.Telemetry.OpenTelemetry
  alias Electric.Utils
  alias PgInterop.Sublink

  @value_prefix SubqueryTags.value_prefix()
  @null_sentinel SubqueryTags.null_sentinel()

  def query_move_in(conn, stack_id, shape_handle, shape, {where, params}, opts \\ []) do
    table = Utils.relation_to_sql(shape.root_table)

    metadata =
      metadata_sql(
        shape,
        stack_id,
        shape_handle,
        opts |> Keyword.put(:start_param_idx, length(params) + 1)
      )

    {json_like_select, metadata_params} =
      json_like_select(shape, %{"is_move_in" => true}, stack_id, shape_handle, metadata)

    key_select = key_select(shape)
    tag_select = Enum.join(metadata.tags_sqls, ", ")

    query =
      Postgrex.prepare!(
        conn,
        table,
        ~s|SELECT #{key_select}, ARRAY[#{tag_select}]::text[], #{json_like_select} FROM #{table} WHERE #{where}|
      )

    Postgrex.stream(conn, query, params ++ metadata_params)
    |> Stream.flat_map(& &1.rows)
  end

  # A nullable key can change NOT IN for tags besides the right-hand key that
  # moved, and a numeric key matches rows that hold its value at another scale.
  # Query the affected tags per position at the same boundary as the log
  # splice, including rows visible through another disjunct.
  def move_patterns(
        conn,
        stack_id,
        shape_handle,
        shape,
        plan,
        dep_index,
        before,
        after_move,
        kind
      ) do
    table = Utils.relation_to_sql(shape.root_table)
    refs = shape.where.used_refs

    patterns =
      for pos <- Map.fetch!(plan.dependency_positions, dep_index),
          info = Map.fetch!(plan.positions, pos),
          {old_views, new_views} = moved_views(info, kind, before, after_move),
          {old_sql, old_params, next} = position_to_sql(info, old_views, refs, 1),
          {new_sql, new_params, _} = position_to_sql(info, new_views, refs, next),
          tag_sql =
            tag_slot_sql(info, stack_id, shape_handle, Shape.composite_tag_encoding(shape)),
          sql = move_patterns_sql(kind, info, table, tag_sql, old_sql, new_sql),
          row <- Postgrex.query!(conn, sql, old_params ++ new_params).rows do
        case row do
          [tag] -> %{pos: pos, value: tag}
          [tag, was_active?] -> {was_active?, %{pos: pos, value: tag}}
        end
      end

    if kind == :replace do
      by_direction = Enum.group_by(patterns, &elem(&1, 0), &elem(&1, 1))
      %{move_out: Map.get(by_direction, true, []), move_in: Map.get(by_direction, false, [])}
    else
      patterns
    end
  end

  # Only a row keyed by a moved value changes an `IN`, so the side that holds the moved values
  # binds just them: with an index on the key, Postgres reads their rows, not the whole shape.
  defp moved_views(%{negated?: false, subquery_ref: ref}, :move_out, before, after_move),
    do: {Map.put(before, ref, moved_view(before, after_move, ref)), after_move}

  defp moved_views(%{negated?: false, subquery_ref: ref}, :move_in, before, after_move),
    do: {before, Map.put(after_move, ref, moved_view(after_move, before, ref))}

  defp moved_views(_info, _kind, before, after_move), do: {before, after_move}

  defp moved_view(views, other_views, ref) do
    view = Map.get(views, ref, MapSet.new())
    other = other_views |> Map.get(ref, MapSet.new()) |> Sublink.keys()

    case view do
      %Sublink{} -> Enum.reduce(other, view, &Sublink.delete(&2, &1))
      %MapSet{} -> MapSet.difference(view, other)
    end
  end

  # A negated position holds while its `IN` is false, a positive one while it is true.
  defp move_patterns_sql(:replace, info, table, tag_sql, old_sql, new_sql) do
    active = if info.negated?, do: "IS FALSE", else: "IS TRUE"

    """
    SELECT DISTINCT #{tag_sql}, (#{old_sql}) #{active} FROM #{table}
    WHERE ((#{old_sql}) #{active}) <> ((#{new_sql}) #{active})
    """
  end

  defp move_patterns_sql(kind, info, table, tag_sql, old_sql, new_sql) do
    {old_test, new_test} =
      if kind == :move_in,
        do: {inactive_sql(info, old_sql), active_sql(info, new_sql)},
        else: {active_sql(info, old_sql), inactive_sql(info, new_sql)}

    """
    SELECT DISTINCT #{tag_sql} FROM #{table}
    WHERE #{old_test} AND #{new_test}
    """
  end

  # A WHERE keeps only the rows its condition makes true, so a bare `IN` selects the rows
  # `IS TRUE` would, and Postgres can read them from an index on the key.
  defp active_sql(%{negated?: true}, sql), do: "(#{sql}) IS FALSE"
  defp active_sql(_info, sql), do: "(#{sql})"

  defp inactive_sql(%{negated?: true}, sql), do: "(#{sql}) IS NOT FALSE"
  defp inactive_sql(_info, sql), do: "(#{sql}) IS NOT TRUE"

  def query_subset(conn, stack_id, shape_handle, shape, subset, headers \\ []) do
    # When querying a subset, we select same columns as the base shape
    table = Utils.relation_to_sql(shape.root_table)

    where =
      case {shape.where, subset.where} do
        {nil, nil} ->
          ""

        {nil, %{query: where}} ->
          " WHERE " <> where

        {%{query: where}, nil} ->
          " WHERE " <> where

        {%{query: base_where}, %{query: where}} ->
          " WHERE (" <> base_where <> ") AND (" <> where <> ")"
      end

    order_by = if order_by = subset.order_by, do: " ORDER BY " <> order_by, else: ""
    limit = if limit = subset.limit, do: " LIMIT #{limit}", else: ""
    offset = if offset = subset.offset, do: " OFFSET #{offset}", else: ""

    metadata = metadata_sql(shape, stack_id, shape_handle)

    {json_like_select, params} =
      json_like_select(shape, headers, stack_id, shape_handle, metadata)

    query =
      Postgrex.prepare!(
        conn,
        table,
        ~s|SELECT #{json_like_select} FROM #{table} #{where} #{order_by} #{limit} #{offset}|
      )

    Postgrex.stream(conn, query, params)
    |> Stream.flat_map(& &1.rows)
  rescue
    e in Postgrex.Error ->
      case e.postgres do
        # invalid_text_representation - e.g. invalid enum value
        %{code: :invalid_text_representation, message: message} ->
          # This is a type of error we expect, because we allow enums in subset where clauses
          # even though we can't validate them fully.
          raise __MODULE__.QueryError, message: message

        _ ->
          reraise e, __STACKTRACE__
      end
  end

  defmodule QueryError do
    defexception [:message]
  end

  @doc """
  Streams the initial data for a shape. Query results are returned as a stream of JSON strings, as prepared on PostgreSQL.
  """
  @type json_iodata :: iodata()

  @type json_result_stream :: Enumerable.t(json_iodata())

  @spec stream_initial_data(
          DBConnection.t(),
          String.t(),
          String.t(),
          Shape.t(),
          non_neg_integer()
        ) ::
          json_result_stream()
  def stream_initial_data(
        conn,
        stack_id,
        shape_handle,
        shape,
        chunk_bytes_threshold \\ LogChunker.default_chunk_size_threshold()
      )

  def stream_initial_data(_, _, _, %Shape{log_mode: :changes_only}, _chunk_bytes_threshold) do
    []
  end

  def stream_initial_data(
        conn,
        stack_id,
        shape_handle,
        %Shape{root_table: root_table} = shape,
        chunk_bytes_threshold
      ) do
    OpenTelemetry.with_span("shape_read.stream_initial_data", [], stack_id, fn ->
      table = Utils.relation_to_sql(root_table)

      where =
        if not is_nil(shape.where), do: " WHERE " <> Expr.sql(shape.where), else: ""

      metadata = metadata_sql(shape, stack_id, shape_handle)
      {json_like_select, params} = json_like_select(shape, [], stack_id, shape_handle, metadata)

      query =
        Postgrex.prepare!(conn, table, ~s|SELECT #{json_like_select} FROM #{table} #{where}|)

      Postgrex.stream(conn, query, params)
      |> Stream.flat_map(& &1.rows)
      |> Stream.transform(0, fn [line], chunk_size ->
        # Reason to add 1 byte to expected length is to account for  `\n` breaks when the data is written.
        case LogChunker.fit_into_chunk(
               IO.iodata_length(line) + 1,
               chunk_size,
               chunk_bytes_threshold
             ) do
          {:ok, new_chunk_size} ->
            {[line], new_chunk_size}

          {:threshold_exceeded, new_chunk_size} ->
            {[line, :chunk_boundary], new_chunk_size}
        end
      end)
    end)
  end

  defp key_select(%Shape{root_table: root_table, root_pk: pk_cols}) do
    ~s['#{escape_relation(root_table)}' || '/' || #{join_primary_keys(pk_cols)}]
  end

  # Converts a tag structure to something PG select can fill, but returns a list of separate strings for each tag
  # - it's up to the caller to interpolate them into the query correctly
  defp make_tags(%Shape{tag_structure: tag_structure} = shape, stack_id, shape_handle) do
    encoding = Shape.composite_tag_encoding(shape)

    Enum.map(tag_structure, fn pattern ->
      Enum.map(pattern, fn
        column_name when is_binary(column_name) ->
          tag_hash_sql([column_name], stack_id, shape_handle, encoding)

        {:hash_together, _columns} = tag_columns ->
          tag_hash_sql(tag_columns, stack_id, shape_handle, encoding)
      end)
      |> Enum.join("|| '/' ||")
    end)
  end

  defp json_like_select(
         %Shape{
           root_table: root_table,
           selected_columns: columns,
           locale: locale,
           localized_columns: localized_columns,
           localized_backing_columns: backing_columns
         } = shape,
         additional_headers,
         _stack_id,
         _shape_handle,
         metadata
       ) do
    key_part = build_key_part(shape)
    value_part = build_value_part(columns, locale, localized_columns, backing_columns)
    headers_part = build_headers_part(root_table, additional_headers, metadata)

    # We're building a JSON string that looks like this:
    #
    # {
    #   "key": "\"public\".\"test_table\"/\"1\"",
    #   "value": {
    #     "id": "1",
    #     "name": "John Doe",
    #     "email": "john.doe@example.com",
    #     "nullable": null
    #   },
    #   "headers": {"operation": "insert", "relation": ["public", "test_table"]}
    # }
    query =
      ~s['{' || #{key_part} || ',' || #{value_part} || ',' || #{headers_part} || '}']

    {query, metadata.params}
  end

  defp build_headers_part(rel, headers, metadata) when is_list(headers),
    do: build_headers_part(rel, Map.new(headers), metadata)

  defp build_headers_part({relation, table}, additional_headers, metadata) do
    headers = %{operation: "insert", relation: [relation, table]}

    headers =
      headers
      |> Map.merge(additional_headers)
      |> Jason.encode!()
      |> Utils.escape_quotes(?')

    headers =
      if metadata.tags_sqls != [] do
        "{" <> json = headers

        ~s/{"active_conditions":#{active_conditions_json_expr(metadata)},"tags":#{tags_json_expr(metadata.tags_sqls)},/ <>
          json
      else
        headers
      end

    ~s['"headers":#{headers}']
  end

  defp build_key_part(shape) do
    # Because relation part of the key is known at query building time, we can use $1 to inject escaped version of the relation
    ~s['"key":' || ] <> pg_escape_string_for_json(key_select(shape))
  end

  # This is a bespoke derivation of the record from its contents for Postgres but it must
  # exactly match the algorithm implemented in `Electric.Replication.Changes.build_key/3`.
  defp join_primary_keys(pk_cols) do
    pk_cols
    |> Enum.map(&pg_cast_column_to_text/1)
    |> Enum.map(&~s['"' || replace(#{&1}, '/', '//') || '"'])
    # NULL values are not allowed in PKs, but they are possible on pk-less tables where we consider all columns to be PKs
    |> Enum.map(&~s[coalesce(#{&1}, '_')])
    |> Enum.join(~s[ || '/' || ])
  end

  defp build_value_part(columns, locale, localized_columns, backing_columns) do
    # A whole-table shape selects the backing columns too. Emitting them here would show every language in
    # the snapshot and one language afterwards, since the live path drops them.
    column_parts =
      columns
      |> Enum.reject(&(&1 in backing_columns))
      |> Enum.map(&build_column_part(&1, locale, localized_columns))

    ~s['"value":{' || #{Enum.join(column_parts, " || ',' || ")} || '}']
  end

  defp build_column_part(column, locale, localized_columns) do
    escaped_name = escape_sql_json_interpolation(column)
    escaped_value = escape_column_value(column, locale, localized_columns)

    # Since `||` returns NULL if any of the arguments is NULL, we need to use `coalesce` to handle NULL values
    ~s['"#{escaped_name}":' || #{pg_coalesce_json_string(escaped_value)}]
  end

  defp escape_sql_json_interpolation(str) do
    str
    |> String.replace(~S|"|, ~S|\"|)
    |> String.replace(~S|'|, ~S|''|)
  end

  defp escape_relation(relation) do
    relation |> Utils.relation_to_sql(true) |> String.replace(~S|'|, ~S|''|)
  end

  defp escape_column_value(column, locale, localized_columns) do
    column
    |> pg_cast_column_to_text(locale, localized_columns)
    |> pg_escape_string_for_json()
    |> pg_coalesce_json_string()
  end

  # Key parts and comparison expressions read the stored column, never a projection: a shape must route and
  # compare on the same value whatever language its reader asked for.
  defp pg_cast_column_to_text(column), do: pg_cast_column_to_text(column, nil, [])

  defp pg_cast_column_to_text(column, locale, localized_columns) do
    col = pg_column_reference(column, column in localized_columns, locale)
    # In PostgreSQL, casting bpchar (char(n)) to text strips trailing spaces.
    # Use concat() for bpchar columns to preserve the space padding, since
    # concat() converts its argument to text without trimming.
    ~s[CASE WHEN #{col} IS NULL THEN NULL::text WHEN pg_typeof(#{col}) = 'character'::regtype THEN concat(#{col}, '') ELSE #{col}::text END]
  end

  # Postgres holds the projection: a stored generated column per language, filled at write time. Reading it
  # under the column's own name is what lets the reader keep its schema — Electric selects columns, it cannot
  # alias them, so the rename has to happen here.
  # Takes the raw column name and escapes once: escaping the base and then the combined identifier would
  # double every quote a column name contains, and PostgreSQL would resolve a different column.
  @doc false
  def pg_column_reference(column, false, _locale), do: ~s["#{Utils.escape_quotes(column)}"]

  def pg_column_reference(column, true, locale),
    do: ~s["#{Utils.escape_quotes(locale_column_name(column, locale))}"]

  @doc false
  def locale_column_name(column, locale), do: "#{column}_#{locale}"

  defp pg_escape_string_for_json(str), do: ~s[to_json(#{str})::text]
  defp pg_coalesce_json_string(str), do: ~s[coalesce(#{str} , 'null')]

  defp metadata_sql(shape, stack_id, shape_handle, opts \\ []) do
    case dnf_plan_for_metadata(shape, opts) do
      %DnfPlan{} = plan ->
        tags_sqls = tags_sql(plan, stack_id, shape_handle, Shape.composite_tag_encoding(shape))

        {active_conditions_sqls, params} =
          case Keyword.get(opts, :views) do
            nil ->
              {active_conditions_sql(plan), []}

            views ->
              {sqls, params, _next_idx} =
                active_conditions_sql_for_views(
                  plan,
                  views,
                  shape.where.used_refs,
                  Keyword.get(opts, :start_param_idx, 1)
                )

              {sqls, params}
          end

        %{tags_sqls: tags_sqls, active_conditions_sqls: active_conditions_sqls, params: params}

      nil ->
        %{
          tags_sqls: make_tags(shape, stack_id, shape_handle),
          active_conditions_sqls: nil,
          params: []
        }
    end
  end

  defp dnf_plan_for_metadata(shape, opts) do
    case Keyword.get(opts, :dnf_plan) do
      %DnfPlan{} = plan ->
        plan

      nil ->
        if shape.shape_dependencies == [] do
          nil
        else
          {:ok, %DnfPlan{} = plan} = DnfPlan.compile(shape)
          plan
        end
    end
  end

  def move_in_where_clause(plan, dep_index, views_before_move, views_after_move, used_refs) do
    impacted = Map.get(plan.dependency_disjuncts, dep_index, [])
    all_idxs = Enum.to_list(0..(length(plan.disjuncts) - 1))
    unaffected = all_idxs -- impacted

    {candidate_sql, candidate_params, next_param} =
      build_disjuncts_sql(
        plan,
        impacted,
        views_after_move,
        used_refs,
        1
      )

    {impacted_before_sql, impacted_before_params, next_param} =
      build_disjuncts_sql(
        plan,
        impacted,
        views_before_move,
        used_refs,
        next_param
      )

    {unaffected_sql, unaffected_params, _} =
      build_disjuncts_sql(
        plan,
        unaffected,
        views_before_move,
        used_refs,
        next_param
      )

    where =
      case join_sql(" OR ", [impacted_before_sql, unaffected_sql]) do
        nil -> candidate_sql
        excl -> "(#{candidate_sql}) AND (#{excl}) IS NOT TRUE"
      end

    {where, candidate_params ++ impacted_before_params ++ unaffected_params}
  end

  def active_conditions_sql(plan) do
    Enum.map(0..(plan.position_count - 1), fn pos ->
      info = plan.positions[pos]
      base_sql = info.sql

      if info.negated? do
        "COALESCE(NOT (#{base_sql})::boolean, false)"
      else
        "COALESCE((#{base_sql})::boolean, false)"
      end
    end)
  end

  def active_conditions_sql_for_views(plan, views, used_refs, start_param_idx \\ 1) do
    {sqls, params, next_param_idx} =
      Enum.reduce(0..(plan.position_count - 1), {[], [], start_param_idx}, fn pos,
                                                                              {sqls, params,
                                                                               param_idx} ->
        info = Map.fetch!(plan.positions, pos)

        {base_sql, sql_params, next_param_idx} =
          position_to_sql(info, views, used_refs, param_idx)

        sql =
          if info.negated? do
            "COALESCE(NOT (#{base_sql})::boolean, false)"
          else
            "COALESCE((#{base_sql})::boolean, false)"
          end

        {[sql | sqls], params ++ sql_params, next_param_idx}
      end)

    {Enum.reverse(sqls), params, next_param_idx}
  end

  def tags_sql(plan, stack_id, shape_handle, encoding \\ :length_prefixed) do
    Enum.map(plan.disjuncts, fn conj ->
      positions_in_disjunct = MapSet.new(conj, &elem(&1, 0))

      slot_sqls =
        Enum.map(0..(plan.position_count - 1), fn pos ->
          if MapSet.member?(positions_in_disjunct, pos) do
            tag_slot_sql(plan.positions[pos], stack_id, shape_handle, encoding)
          else
            "''"
          end
        end)

      Enum.join(slot_sqls, " || '/' || ")
    end)
  end

  defp build_disjuncts_sql(
         _plan,
         [],
         _views,
         _used_refs,
         pidx
       ) do
    {nil, [], pidx}
  end

  defp build_disjuncts_sql(
         plan,
         disjunct_idxs,
         views,
         used_refs,
         pidx
       ) do
    {sqls, params, next_pidx} =
      Enum.reduce(disjunct_idxs, {[], [], pidx}, fn didx, {sqls, params, pi} ->
        conj = Enum.at(plan.disjuncts, didx)

        {conj_sql, conj_params, next_pi} =
          build_conjunction_sql(plan, conj, views, used_refs, pi)

        {[conj_sql | sqls], params ++ conj_params, next_pi}
      end)

    sql = join_sql(" OR ", Enum.reverse(sqls))

    {sql, params, next_pidx}
  end

  defp build_conjunction_sql(plan, conj, views, used_refs, pidx) do
    {parts, params, next_pi} =
      Enum.reduce(conj, {[], [], pidx}, fn {pos, polarity}, {parts, params, pi} ->
        info = plan.positions[pos]

        {sql, ps, next_pi} = position_to_sql(info, views, used_refs, pi)

        sql = if polarity == :negated, do: "NOT (#{sql})", else: sql

        {[sql | parts], params ++ ps, next_pi}
      end)

    sql = parts |> Enum.reverse() |> Enum.join(" AND ")
    {sql, params, next_pi}
  end

  defp position_to_sql(%{subquery?: false} = info, _, _, pidx) do
    {info.sql, [], pidx}
  end

  defp position_to_sql(
         %{subquery?: true} = info,
         views,
         used_refs,
         pidx
       ) do
    lhs_sql = lhs_sql_from_ast(info.ast)
    ref_type = Map.get(used_refs, info.subquery_ref)

    view = Map.get(views, info.subquery_ref, MapSet.new())
    values = view |> Sublink.keys() |> MapSet.to_list()

    case ref_type do
      {:array, {:row, col_types}} ->
        columns =
          case values do
            [] ->
              Enum.map(col_types, fn _ -> [] end)

            _ ->
              view
              |> composite_query_values(values, col_types)
              |> Utils.unzip_any()
              |> Tuple.to_list()
          end

        {sql, params} = composite_membership_source(col_types, columns, pidx)
        {"#{lhs_sql} IN (#{sql})", params, pidx + length(col_types)}

      # `= ANY` over an array of arrays compares the column with their elements, so an array key
      # binds as text, one element per key, as an array field of a composite key does.
      {:array, {:array, _} = element_type} ->
        {sql, params} = composite_membership_source([element_type], [values], pidx)
        {"#{lhs_sql} IN (#{sql})", params, pidx + 1}

      {:array, element_type} ->
        type_cast = Eval.type_to_pg_cast(element_type)

        if Enum.any?(values, &text_key?/1) do
          sql = "#{lhs_sql} = ANY ($#{pidx}::text[]::#{type_cast}[])"
          {sql, [Enum.map(values, &key_text(&1, element_type))], pidx + 1}
        else
          sql = "#{lhs_sql} = ANY ($#{pidx}::#{type_cast}[])"
          {sql, [Enum.map(values, &value_to_postgrex(&1, element_type))], pidx + 1}
        end
    end
  end

  # A key Postgres sent in a form the evaluator does not parse is its text (see `SublinkKey`), so a
  # list of keys holding one binds as text, each key written as Postgres reads it.
  defp text_key?(value), do: match?(%Eval.SublinkKey.Text{}, value)

  defp key_text(%Eval.SublinkKey.Text{text: text}, _type), do: text
  defp key_text(value, type), do: array_element_text(value, type)

  # The evaluator's numeric representation is a float. Use the original
  # Postgres text for SQL comparisons so the bind retains the exact key.
  # Postgres bounds that text, so Decimal's default 34-digit parse limit
  # would only refuse values Postgres stores.
  @numeric_parse_limits [max_digits: :infinity, max_exponent: :infinity]

  defp composite_query_values(%Sublink{originals: originals}, values, types)
       when is_map(originals) do
    numeric_positions = for {:numeric, idx} <- Enum.with_index(types), do: idx
    numeric_array_positions = for {{:array, :numeric}, idx} <- Enum.with_index(types), do: idx

    Enum.map(values, fn value ->
      original = Map.fetch!(originals, value)

      value =
        Enum.reduce(numeric_positions, value, fn idx, value ->
          decimal = if text = elem(original, idx), do: Decimal.new(text, @numeric_parse_limits)
          put_elem(value, idx, decimal)
        end)

      # An array field binds as text already, so it takes the array literal Postgres sent.
      Enum.reduce(numeric_array_positions, value, fn idx, value ->
        put_elem(value, idx, elem(original, idx) && {:array_text, elem(original, idx)})
      end)
    end)
  end

  defp composite_query_values(_, values, _), do: values

  defp composite_membership_source(types, columns, pidx) do
    {binds, fields} =
      types
      |> Enum.zip(columns)
      |> Enum.with_index(pidx)
      |> Enum.map(fn {{type, values}, index} -> composite_column(type, values, index) end)
      |> Enum.unzip()

    {select, params} = Enum.unzip(fields)
    source = "unnest(#{Enum.join(binds, ", ")})"

    column_names = Enum.map(pidx..(pidx + length(types) - 1)//1, &"key_#{&1}")

    if select != column_names do
      column_names = Enum.join(column_names, ", ")
      {"SELECT #{Enum.join(select, ", ")} FROM #{source} AS rhs(#{column_names})", params}
    else
      {"SELECT * FROM #{source}", params}
    end
  end

  # unnest flattens all array dimensions. Bind each array-valued field as one
  # text element and cast it after unnesting to keep the fields of a row intact.
  defp composite_column({:array, element_type} = type, values, index) do
    params = Enum.map(values, &array_parameter(&1, element_type))
    {"$#{index}::text[]", {"key_#{index}::#{Eval.type_to_pg_cast(type)}", params}}
  end

  defp composite_column(type, values, index) do
    if Enum.any?(values, &text_key?/1) do
      params = Enum.map(values, &key_text(&1, type))
      {"$#{index}::text[]", {"key_#{index}::#{Eval.type_to_pg_cast(type)}", params}}
    else
      params = Enum.map(values, &value_to_postgrex(&1, type))
      {"$#{index}::#{Eval.type_to_pg_cast(type)}[]", {"key_#{index}", params}}
    end
  end

  defp array_parameter(nil, _type), do: nil
  defp array_parameter({:array_text, text}, _type), do: text

  defp array_parameter(%PgInterop.Array.Bounded{elements: elements} = array, type),
    do: PgInterop.Array.bounds_prefix(array) <> array_parameter(elements, type)

  defp array_parameter([head | _] = values, type) when is_list(head) do
    "{" <> Enum.map_join(values, ",", &array_parameter(&1, type)) <> "}"
  end

  defp array_parameter(values, type) do
    values |> Enum.map(&array_element_text(&1, type)) |> PgInterop.Array.serialize()
  end

  defp array_element_text(nil, _type), do: nil
  defp array_element_text(%Eval.SublinkKey.Text{text: text}, _type), do: text
  defp array_element_text(value, :uuid), do: Casting.uuid_to_string(value)
  defp array_element_text(value, :interval), do: PgInterop.Interval.format(value)
  defp array_element_text(value, _type), do: to_string(value)

  defp value_to_postgrex(value, type) do
    Eval.value_to_postgrex(value, type)
  end

  defp lhs_sql_from_ast(%Func{name: "sublink_membership_check", args: [testexpr, _]}) do
    SqlGenerator.to_sql(testexpr)
  end

  defp tag_slot_sql(
         %{subquery?: true, tag_columns: tag_columns},
         stack_id,
         shape_handle,
         encoding
       ) do
    tag_hash_sql(tag_columns, stack_id, shape_handle, encoding)
  end

  defp tag_slot_sql(%{subquery?: false}, _stack_id, _shape_handle, _encoding) do
    "'1'"
  end

  defp tag_hash_sql([col], stack_id, shape_handle, _encoding) do
    col_sql = pg_cast_column_to_text(col)
    namespaced = pg_namespace_value_sql(col_sql)
    ~s[md5('#{stack_id}#{shape_handle}' || #{namespaced})]
  end

  defp tag_hash_sql({:hash_together, cols}, stack_id, shape_handle, encoding) do
    ~s[md5('#{stack_id}#{shape_handle}' || #{composite_tag_input_sql(cols, encoding)})]
  end

  @doc false
  def composite_tag_input_sql(columns, :legacy) do
    Enum.map_join(columns, " || ", fn col_name ->
      col = pg_cast_column_to_text(col_name)
      ~s['#{Utils.escape_quotes(col_name, ?')}:' || #{pg_namespace_value_sql(col)}]
    end)
  end

  # octet_length counts the bytes md5 hashes, in the server encoding as for single-column tags.
  def composite_tag_input_sql(columns, :length_prefixed) do
    Enum.map_join(columns, " || ", fn col_name ->
      namespaced = col_name |> pg_cast_column_to_text() |> pg_namespace_value_sql()
      "octet_length(#{namespaced})::text || ':' || #{namespaced}"
    end)
  end

  defp join_sql(separator, sqls) do
    case Enum.reject(sqls, &is_nil/1) do
      [] -> nil
      [single] -> single
      multiple -> Enum.map_join(multiple, separator, &"(#{&1})")
    end
  end

  defp active_conditions_json_expr(%{active_conditions_sqls: nil, tags_sqls: tags_sqls}) do
    List.duplicate(true, length(tags_sqls)) |> Jason.encode!()
  end

  defp active_conditions_json_expr(%{active_conditions_sqls: sqls}) do
    "' || to_json(ARRAY[" <> Enum.join(sqls, ", ") <> "]::boolean[])::text || '"
  end

  defp tags_json_expr(tags_sqls) do
    "' || to_json(ARRAY[" <> Enum.join(tags_sqls, ", ") <> "]::text[])::text || '"
  end

  # Generates SQL to namespace a value for tag hashing.
  # This MUST produce identical output to SubqueryTags.namespace_value/1 for
  # the same input values, or Elixir-side and SQL-side tag computation will diverge.
  defp pg_namespace_value_sql(col_sql) do
    ~s[CASE WHEN #{col_sql} IS NULL THEN '#{@null_sentinel}' ELSE '#{@value_prefix}' || #{col_sql} END]
  end
end
