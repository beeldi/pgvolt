defmodule Electric.Shapes.Shape do
  @moduledoc """
  Struct describing the requested shape
  """
  alias Electric.Replication.Eval.Expr
  alias Electric.Replication.Eval.Runner
  alias Electric.Postgres.Inspector
  alias Electric.Replication.Eval.Parser
  alias Electric.Replication.Eval.Walker
  alias Electric.Replication.Changes
  alias Electric.Shapes.DnfPlan
  alias Electric.Shapes.SubqueryTags
  alias Electric.Shapes.WhereClause
  alias Electric.Telemetry.RequestValues
  alias Electric.Utils
  alias Electric.Shapes.Shape.Validators
  require Logger

  defprotocol Comparable do
    @fallback_to_any true

    @spec comparable(t()) :: t()
    def comparable(term)
  end

  defimpl Comparable, for: Any do
    def comparable(term), do: term
  end

  @default_replica :default

  @enforce_keys [:root_table, :root_table_id]
  defstruct [
    :root_table,
    :root_table_id,
    :root_pk,
    :root_column_count,
    :where,
    :selected_columns,
    :explicitly_selected_columns,
    :queryable_columns,
    locale: nil,
    localized_columns: [],
    localized_backing_columns: [],
    shape_dependencies: [],
    shape_dependencies_handles: [],
    tag_structure: [],
    composite_tag_encoding: :length_prefixed,
    subquery_comparison_expressions: %{},
    log_mode: :full,
    source_positions: :legacy,
    source_generation: nil,
    flags: %{},
    storage: %{compaction: :disabled},
    replica: @default_replica
  ]

  @type handle() :: String.t()

  @type replica() :: :full | :default
  @type table_info() :: %{
          columns: [Inspector.column_info(), ...],
          pk: [String.t(), ...]
        }
  @type storage_config :: %{
          compaction: :enabled | :disabled
        }
  @type log_mode() :: :changes_only | :full
  @type flag() ::
          :selects_all_columns | :selects_generated_columns | :non_primitive_columns_in_where
  @type t() :: %__MODULE__{
          root_table: Electric.relation(),
          root_table_id: Electric.relation_id(),
          root_pk: [String.t(), ...],
          root_column_count: non_neg_integer(),
          flags: %{optional(flag()) => boolean()},
          where: Electric.Replication.Eval.Expr.t() | nil,
          selected_columns: [String.t(), ...],
          explicitly_selected_columns: [String.t(), ...],
          queryable_columns: [String.t(), ...] | nil,
          tag_structure: [String.t() | [String.t(), ...]],
          composite_tag_encoding: SubqueryTags.encoding(),
          replica: replica(),
          storage: storage_config() | nil,
          shape_dependencies: [t(), ...],
          log_mode: log_mode(),
          source_positions: :legacy | :full_xid,
          source_generation: String.t() | nil
        }

  @type json_relation() :: [String.t(), ...]
  @type json_table_info() :: table_info() | json_relation()
  @type json_table_list() :: [json_table_info(), ...]
  @type json_safe() :: %{
          version: non_neg_integer(),
          root_table: json_relation(),
          root_table_id: non_neg_integer(),
          root_pks: [String.t(), ...],
          root_column_count: non_neg_integer(),
          where: String.t(),
          selected_columns: [String.t(), ...],
          flags: %{optional(flag()) => boolean()},
          replica: String.t(),
          storage: storage_config() | nil,
          shape_dependencies: [json_safe(), ...],
          composite_tag_encoding: SubqueryTags.encoding(),
          log_mode: log_mode(),
          source_positions: :legacy | :full_xid,
          source_generation: String.t() | nil
        }
  @type comparable() :: term()

  @doc """
  Return a comparable representation of the shape.

  This is used to compare shapes for equality as an ETS key - and thus it'll be
  matched in some cases, not just compared equal. This representation must
  therefore not contain any maps (as they are matched when one is missing a key
  for example).

  This representation must contain all the information that identifies
  user-specified properties of the shape. We're omitting storage configuration
  and other internal state.
  """
  @spec comparable(t()) :: comparable()
  def comparable(%__MODULE__{} = shape) do
    legacy =
      {:shape, {shape.root_table_id, shape.root_table}, shape.root_pk,
       Comparable.comparable(shape.where), shape.selected_columns,
       shape.explicitly_selected_columns,
       Enum.flat_map(shape.flags, fn {k, v} -> if(v, do: [k], else: []) end) |> Enum.sort(),
       shape.replica, shape.log_mode}

    # Two languages must never share a handle, but a shape asking for none must keep the identity it had:
    # widening the tuple unconditionally would recreate every existing shape, localized or not.
    comparable =
      case {Map.get(shape, :locale), Map.get(shape, :localized_columns, [])} do
        {nil, _} -> legacy
        {_, []} -> legacy
        {locale, localized} -> {:localized, legacy, locale, localized}
      end

    if Map.get(shape, :source_positions, :legacy) == :full_xid,
      do: {:full_xid, Map.get(shape, :source_generation), comparable},
      else: comparable
  end

  @doc """
  Key identifying shapes whose conversion and encoding of a change are byte-identical, so that a
  fan-out group can convert and encode once instead of once per participating shape.

  Membership is deliberately absent from the key. By the time a change reaches a shape the router
  has already decided membership exactly -- it evaluates the residual where clause itself rather
  than over-approximating -- so re-deriving it per shape recomputes a settled answer. What is left
  of `convert_change/3` for a shape in a class is the column projection, which depends only on
  `selected_columns`, plus row metadata that is constant when there are no move tags.

  Returns `nil` for shapes that must convert individually: those with subquery dependencies, whose
  routing is conservative by design and whose membership genuinely is refined per shape, and those
  with move tags, whose values are derived per shape handle.
  """
  @spec fanout_class(t()) :: term() | nil
  def fanout_class(%__MODULE__{tag_structure: [], shape_dependencies: []} = shape) do
    legacy =
      {shape.root_table, is_nil(shape.where), Map.get(shape.flags, :selects_all_columns, false),
       shape.selected_columns, shape.root_pk, shape.replica, shape.log_mode,
       Map.get(shape, :source_positions, :legacy), Map.get(shape, :source_generation)}

    case {Map.get(shape, :locale), Map.get(shape, :localized_columns, [])} do
      {nil, _} -> legacy
      {_, []} -> legacy
      {locale, localized} -> {:localized, legacy, locale, localized}
    end
  end

  def fanout_class(%__MODULE__{}), do: nil

  defguard has_dependencies(shape) when shape.shape_dependencies != []

  defguard are_deps_filled(shape)
           when shape.shape_dependencies == [] or shape.shape_dependencies_handles != []

  def dependency_handles_known?(%__MODULE__{} = shape),
    do: shape.shape_dependencies_handles != []

  # The shape catalog decodes stored structs as Erlang terms, without applying struct defaults.
  def composite_tag_encoding(%__MODULE__{} = shape),
    do: Map.get(shape, :composite_tag_encoding, :legacy)

  def hash(%__MODULE__{} = shape) do
    {_comparable, hash} = comparable_hash(shape)
    hash
  end

  def comparable_hash(%__MODULE__{} = shape) do
    comparable = comparable(shape)
    {comparable, :erlang.phash2(comparable)}
  end

  @doc """
  Collision-resistant identity for a shape, for any comparison that decides whether a handle may be
  served a shape's data.

  `hash/1` is a bucketing key, not an identity: `:erlang.phash2/1` ranges over 2^27, so distinct
  shapes start sharing a value after roughly 13k of them and a colliding pair is cheap to construct
  offline.
  """
  @spec comparable_digest(t()) :: binary()
  def comparable_digest(%__MODULE__{} = shape), do: shape |> comparable() |> digest_comparable()

  @spec digest_comparable(term()) :: binary()
  def digest_comparable(comparable) do
    :crypto.hash(:sha256, :erlang.term_to_binary(comparable, [:deterministic]))
  end

  def generate_id(%__MODULE__{} = shape) do
    hash = hash(shape)

    # Use microseconds to essentially avoid collisions within the same
    # millisecond when we have a hash collision
    {hash, "#{hash}-#{DateTime.utc_now() |> DateTime.to_unix(:microsecond)}"}
  end

  @doc """
  List all relations that are a part of this shape, as oid-name tuples.
  """
  @spec list_relations(t()) :: [Electric.oid_relation()]
  def list_relations(%__MODULE__{} = shape) do
    [{shape.root_table_id, shape.root_table}]
  end

  def new!(table, opts \\ []) do
    case new(table, opts) do
      {:ok, shape} -> shape
      {:error, {_field, [message | _]}} -> raise message
      {:error, {_field, message}} when is_binary(message) -> raise message
    end
  end

  def pk(%__MODULE__{root_pk: root_pk}, _relation \\ nil), do: root_pk

  @schema_options [
    relation: [type: {:tuple, [:string, :string]}, required: true],
    where: [type: :any],
    columns: [type: {:or, [{:list, :string}, nil]}],
    queryable_columns: [type: {:or, [{:list, :string}, nil]}],
    params: [type: {:map, :string, :string}, default: %{}],
    autofill_pk_select?: [type: :boolean, default: false],
    replica: [
      type: {:custom, __MODULE__, :verify_replica, []},
      default: :default
    ],
    inspector: [
      type: :mod_arg,
      default: {Electric.Postgres.Inspector, Electric.DbPool}
    ],
    feature_flags: [type: {:list, :string}, default: Electric.Config.get_env(:feature_flags)],
    storage: [
      type: {
        :or,
        [
          nil,
          map: [compaction: [type: {:in, [:enabled, :disabled]}, default: :enabled]]
        ]
      },
      default: nil,
      type_spec: quote(do: nil | Electric.Shapes.Shape.storage_config())
    ],
    log_mode: [type: {:in, [:changes_only, :full]}, default: :full],
    source_positions: [type: {:in, [:legacy, :full_xid]}, default: :legacy],
    source_generation: [type: {:or, [:string, nil]}, default: nil],
    locale: [type: {:custom, __MODULE__, :validate_locale, []}, default: nil],
    localized_columns: [type: {:list, :string}, default: []]
  ]
  @shape_schema NimbleOptions.new!(@schema_options)

  def schema_options do
    @schema_options
  end

  # The locale reaches a single-quoted SQL literal in the snapshot query, and it comes from the caller.
  # A language tag is letters, digits, hyphens and underscores; anything else is refused here rather than
  # escaped further down, so no second caller can forget.
  @locale_format ~r/^[A-Za-z0-9_-]{1,35}$/

  @doc false
  def validate_locale(nil), do: {:ok, nil}

  def validate_locale(locale) when is_binary(locale) do
    if Regex.match?(@locale_format, locale),
      do: {:ok, locale},
      else: {:error, "expected a language tag, got: #{inspect(locale)}"}
  end

  def validate_locale(other), do: {:error, "expected a string or nil, got: #{inspect(other)}"}

  def default_replica_mode, do: @default_replica

  def new(table, opts) when is_binary(table) and is_list(opts) do
    case Electric.Postgres.Identifiers.parse_relation(table) do
      {:ok, relation} ->
        opts
        |> Keyword.put(:relation, relation)
        |> new()

      {:error, reason} ->
        {:error, {:table, [reason]}}
    end
  end

  # We get (table, columns, where) -> we prepare SELECT columns FROM table WHERE where
  # We parse the complete SELECT statement, and get a topologically sorted list of subqueries
  # (including the root query)
  # Then for each subquery we create a shape struct, and put it into context so that next shape
  def new(opts) when is_list(opts) or is_map(opts) do
    opts = Map.new(opts)

    if Map.get(opts, :select) do
      with {:ok, opts} <- make_opts_from_select(opts[:select], opts) do
        make_new(Map.drop(opts, [:select]))
      end
    else
      make_new(opts)
    end
  end

  defp make_new(opts) when is_list(opts) or is_map(opts) do
    with {:ok, opts} <- NimbleOptions.validate(opts, @shape_schema),
         opts = Map.new(opts),
         :ok <- validate_source_positions(opts),
         inspector = Map.fetch!(opts, :inspector),
         {:ok, {oid, table} = relation} <- validate_relation(opts, inspector),
         {:ok, column_info, pk_cols} <- load_column_info(relation, inspector),
         {:ok, supported_features} <- load_supported_features(inspector),
         {:ok, queryable_columns} <- validate_queryable_columns(column_info, pk_cols, opts),
         {:ok, selected_columns, explicitly_selected_columns} <-
           validate_selected_columns(
             column_info,
             pk_cols,
             supported_features,
             Map.put(opts, :queryable_columns, queryable_columns)
           ),
         refs = Inspector.columns_to_expr(column_info),
         {:ok, where, shape_dependencies} <-
           validate_where_clause(Map.get(opts, :where), opts, refs) do
      flags =
        [
          if(is_nil(Map.get(opts, :columns)) and is_nil(Map.get(opts, :queryable_columns)),
            do: :selects_all_columns
          ),
          if(
            any_columns_generated?(
              column_info,
              selected_columns ++
                localized_backing_columns(
                  column_info,
                  localized_columns(opts, selected_columns, column_info)
                )
            ),
            do: :selects_generated_columns
          ),
          if(any_columns_non_primitive?(column_info, where),
            do: :non_primitive_columns_in_where
          )
        ]
        |> Enum.reject(&is_nil/1)
        |> Map.new(fn k -> {k, true} end)

      {:ok,
       %__MODULE__{
         root_table: table,
         root_table_id: oid,
         root_column_count: length(column_info),
         root_pk: pk_cols,
         flags: flags,
         where: where,
         selected_columns: selected_columns,
         explicitly_selected_columns: explicitly_selected_columns,
         queryable_columns: queryable_columns,
         replica: Map.get(opts, :replica, :default),
         storage: Map.get(opts, :storage) || %{compaction: :disabled},
         shape_dependencies: shape_dependencies,
         log_mode: Map.fetch!(opts, :log_mode),
         source_positions: Map.fetch!(opts, :source_positions),
         source_generation: Map.fetch!(opts, :source_generation),
         locale: Map.get(opts, :locale),
         localized_columns: localized_columns(opts, selected_columns, column_info),
         localized_backing_columns:
           localized_backing_columns(
             column_info,
             localized_columns(opts, selected_columns, column_info)
           )
       }
       |> fill_tag_structure()}
    end
  end

  @doc """
  The generated columns that exist only to hold one language of a localized one. Taken from the catalogue
  rather than from the name: a real column called `name_short` beside a localized `name` shares the prefix
  but is not a translation, and dropping it would lose data.
  """
  @spec localized_backing_columns([map()], [String.t()]) :: [String.t()]
  def localized_backing_columns(_column_info, []), do: []

  def localized_backing_columns(column_info, localized_columns) do
    column_info
    |> Enum.filter(fn column ->
      Map.get(column, :is_generated, false) and column.name not in localized_columns and
        Enum.any?(localized_columns, &String.starts_with?(column.name, "#{&1}_"))
    end)
    |> Enum.map(& &1.name)
    |> Enum.sort()
  end

  @doc """
  The catalogue as a reader of this shape sees it. A localized column carries the type of the generated
  column holding its language, under its own name, since that is the value the shape serves; the backing
  columns the shape hides are left out. Without this the response schema would announce `jsonb` for a value
  that is plain text, and a client parsing by that schema would fail on it.
  """
  @spec projected_column_info(t(), [map()]) :: [map()]
  def projected_column_info(%__MODULE__{localized_columns: []}, column_info), do: column_info

  def projected_column_info(%__MODULE__{} = shape, column_info) do
    by_name = Map.new(column_info, &{&1.name, &1})

    column_info
    |> Enum.reject(&(&1.name in shape.localized_backing_columns))
    |> Enum.map(fn column ->
      case column.name in shape.localized_columns &&
             Map.get(by_name, "#{column.name}_#{shape.locale}") do
        %{} = backing -> %{backing | name: column.name, pk_position: column.pk_position}
        _ -> column
      end
    end)
  end

  @doc """
  The columns this shape projects to a single locale: those the caller declared as locale-keyed JSON and
  that the shape actually selects. Declaring a column the shape does not carry is a caller mistake that
  would otherwise sit unnoticed in the shape identity. Under `queryable_columns`, the column backing the
  locale has to be queryable too.
  """
  @spec localized_columns(map(), [String.t()], [map()]) :: [String.t()]
  def localized_columns(opts, selected_columns, column_info) do
    case Map.get(opts, :locale) do
      nil ->
        []

      locale ->
        queryable_columns = Map.get(opts, :queryable_columns)

        opts
        |> Map.get(:localized_columns, [])
        |> Enum.filter(
          &(&1 in selected_columns and
              backed_by_generated_column?(column_info, &1, locale, queryable_columns))
        )
        |> Enum.sort()
    end
  end

  # A locale nobody migrated the table for, or a typo in one, would otherwise have the snapshot query name a
  # column that does not exist and fail with undefined_column. The column keeps its stored object instead,
  # which the reader resolves as it did before any projection. A request held to `queryable_columns` sees
  # only those columns, so a backing column left off that list is missing for it in the same way: else the
  # locale, which the caller picks, would serve any generated `<column>_<suffix>` under a queryable name.
  defp backed_by_generated_column?(column_info, column, locale, queryable_columns) do
    backing = "#{column}_#{locale}"

    (is_nil(queryable_columns) or backing in queryable_columns) and
      Enum.any?(column_info, fn candidate ->
        candidate.name == backing and Map.get(candidate, :is_generated, false)
      end)
  end

  defp fill_tag_structure(shape) do
    {tag_structure, comparison_expressions} = SubqueryTags.move_in_tag_structure(shape)

    %{
      shape
      | tag_structure: tag_structure,
        subquery_comparison_expressions: comparison_expressions
    }
  end

  defp validate_where_clause(nil, _opts, _refs), do: {:ok, nil, []}

  defp validate_where_clause(where, %{inspector: inspector} = opts, refs) do
    with {:ok, where} <- Parser.parse_query(where),
         {:ok, subqueries} <- Parser.extract_subqueries(where),
         {:ok, shape_dependencies, sublink_dependency_indexes} <-
           build_shape_dependencies(subqueries, opts),
         {:ok, dependency_refs} <- build_dependency_refs(shape_dependencies, inspector),
         all_refs =
           Map.merge(refs, expand_dependency_refs(dependency_refs, sublink_dependency_indexes)),
         :ok <- Validators.validate_parameters(opts[:params]),
         {:ok, where} <-
           Parser.validate_where_ast(where,
             params: opts[:params],
             refs: all_refs,
             sublink_queries:
               expand_sublink_queries(shape_dependencies, sublink_dependency_indexes)
           ),
         {:ok, where} <- canonicalize_where_sublink_refs(where, sublink_dependency_indexes),
         {:ok, where} <- Validators.validate_where_return_type(where) do
      {:ok, where, shape_dependencies}
    else
      {:error, {part, reason}} -> {:error, {part, reason}}
      {:error, reason} -> {:error, {:where, reason}}
    end
  end

  defp validate_source_positions(%{source_positions: :full_xid, source_generation: generation})
       when not is_binary(generation) or generation == "" do
    {:error,
     {:source_positions, "Full transaction source positions require a verified source generation"}}
  end

  defp validate_source_positions(%{source_positions: :full_xid, storage: %{compaction: :enabled}}) do
    {:error, {:source_positions, "Full transaction source positions do not support compaction"}}
  end

  defp validate_source_positions(_opts), do: :ok

  defp make_opts_from_select(select, opts) do
    with {:ok, {columns, from, where}} <- Parser.extract_parts_from_select(select) do
      {:ok,
       opts |> Map.put(:columns, columns) |> Map.put(:where, where) |> Map.put(:relation, from)}
    end
  end

  defp build_shape_dependencies(subqueries, opts) do
    # A dependency shape decides subquery membership, so it must read the stored value: projecting it would
    # compare on one language, and its table may carry no backing column at all.
    shared_opts =
      Map.drop(opts, [
        :where,
        :columns,
        :queryable_columns,
        :relation,
        :locale,
        :localized_columns
      ])

    subqueries
    |> Enum.with_index()
    |> Utils.reduce_while_ok({[], %{}, %{}}, fn {subquery, occurrence_idx},
                                                {shape_dependencies, dependency_index_by_shape,
                                                 occurrence_to_dependency} ->
      with {:ok, {columns, _from, _where}} <- Parser.extract_parts_from_select(subquery),
           {:ok, shape_dependency} <-
             shared_opts
             |> Map.put(:select, subquery)
             |> Map.put(:autofill_pk_select?, true)
             |> Map.put(:log_mode, :full)
             |> new() do
        # The row left of a subquery pairs with its select list in the order the list names its
        # columns, so a dependency keeps that order where a shape sorts its own.
        shape_dependency = %{shape_dependency | explicitly_selected_columns: columns}
        comparable_shape = comparable(shape_dependency)

        case dependency_index_by_shape do
          %{^comparable_shape => dependency_idx} ->
            {:ok,
             {shape_dependencies, dependency_index_by_shape,
              Map.put(occurrence_to_dependency, occurrence_idx, dependency_idx)}}

          %{} ->
            dependency_idx = length(shape_dependencies)

            {:ok,
             {shape_dependencies ++ [shape_dependency],
              Map.put(dependency_index_by_shape, comparable_shape, dependency_idx),
              Map.put(occurrence_to_dependency, occurrence_idx, dependency_idx)}}
        end
      end
    end)
    |> case do
      {:ok, {shape_dependencies, _dependency_index_by_shape, occurrence_to_dependency}} ->
        {:ok, shape_dependencies, occurrence_to_dependency}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp build_dependency_refs(shape_dependencies, inspector) do
    shape_dependencies
    |> Enum.with_index()
    |> Utils.reduce_while_ok(%{}, fn {shape, i}, acc ->
      relation = {shape.root_table_id, shape.root_table}

      with {:ok, column_info, _} <- load_column_info(relation, inspector) do
        types = Inspector.columns_to_expr(column_info)

        type =
          shape.explicitly_selected_columns
          |> Enum.uniq()
          |> Enum.map(&Map.fetch!(types, [&1]))
          |> case do
            [type] -> type
            multiple -> {:row, multiple}
          end

        {:ok, Map.put(acc, ["$sublink", "#{i}"], {:array, type})}
      end
    end)
  end

  defp expand_dependency_refs(dependency_refs, sublink_dependency_indexes) do
    Map.new(sublink_dependency_indexes, fn {occurrence_idx, dependency_idx} ->
      {["$sublink", "#{occurrence_idx}"],
       Map.fetch!(dependency_refs, ["$sublink", "#{dependency_idx}"])}
    end)
  end

  defp expand_sublink_queries(shape_dependencies, sublink_dependency_indexes) do
    canonical_queries = extract_sublink_queries(shape_dependencies)

    Map.new(sublink_dependency_indexes, fn {occurrence_idx, dependency_idx} ->
      {occurrence_idx, Map.fetch!(canonical_queries, dependency_idx)}
    end)
  end

  defp canonicalize_where_sublink_refs(%Expr{} = where, sublink_dependency_indexes) do
    with {:ok, eval} <-
           Walker.fold(
             where.eval,
             fn
               %Parser.Ref{path: ["$sublink", idx]} = ref, _children, occurrence_to_dependency ->
                 dependency_idx =
                   occurrence_to_dependency
                   |> Map.fetch!(String.to_integer(idx))
                   |> Integer.to_string()

                 {:ok, %{ref | path: ["$sublink", dependency_idx]}}

               node, children, _occurrence_to_dependency when map_size(children) == 0 ->
                 {:ok, node}

               node, children, _occurrence_to_dependency ->
                 {:ok, Map.merge(node, children)}
             end,
             sublink_dependency_indexes
           ) do
      {:ok, %{where | eval: eval, used_refs: Parser.find_refs(eval)}}
    end
  end

  defp extract_sublink_queries(shapes) do
    Enum.with_index(shapes, fn %__MODULE__{} = shape, i ->
      columns = Enum.map_join(shape.explicitly_selected_columns, ", ", &Utils.quote_name/1)

      base = "SELECT " <> columns <> " FROM " <> Utils.relation_to_sql(shape.root_table)

      where = if shape.where, do: " WHERE " <> shape.where.query, else: ""

      {i, base <> where}
    end)
    |> Map.new()
  end

  @spec validate_selected_columns(
          [Inspector.column_info()],
          [String.t()],
          Inspector.supported_features(),
          map()
        ) ::
          {:ok, needed :: [String.t(), ...], selected :: [String.t(), ...]}
          | {:error, {:columns, [String.t()]}}

  # When an explicit list of columns was included in the shape request, make sure that they are
  # valid, they cover all the PK columns and none of them is a generated column.
  defp validate_selected_columns(
         column_info,
         pk_cols,
         %{supports_generated_column_replication: supports_generated_column_replication},
         %{columns: columns_to_select} = opts
       )
       when is_list(columns_to_select) do
    autofill_pk_select? = Map.fetch!(opts, :autofill_pk_select?)

    missing_pk_cols = pk_cols -- columns_to_select
    queryable_columns = Map.fetch!(opts, :queryable_columns)
    selectable_column_names = queryable_columns || Enum.map(column_info, & &1.name)
    invalid_cols = columns_to_select -- selectable_column_names
    generated_cols = Enum.filter(column_info, &(&1.is_generated and &1.name in columns_to_select))

    err_msg =
      cond do
        missing_pk_cols != [] and not autofill_pk_select? ->
          "The list of columns must include all primary key columns, missing: " <>
            Enum.join(missing_pk_cols, ", ")

        invalid_cols != [] ->
          "The following columns are not found on the table: " <> Enum.join(invalid_cols, ", ")

        generated_cols != [] and not supports_generated_column_replication ->
          "The following columns are generated and cannot be included in the shape: " <>
            (generated_cols |> Enum.map(& &1.name) |> Enum.join(", "))

        columns_to_select == [] ->
          "The list of columns must not be empty"

        true ->
          nil
      end

    all_columns = Enum.uniq(columns_to_select ++ pk_cols)

    if is_nil(err_msg) do
      {:ok, Enum.sort(all_columns), Enum.sort(columns_to_select)}
    else
      {:error, {:columns, [err_msg]}}
    end
  end

  # No explicit column list was included in the shape request. Only check for the presence of
  # generated columns in the table schema.
  defp validate_selected_columns(
         column_info,
         _pk_cols,
         %{supports_generated_column_replication: supports_generated_column_replication},
         opts
       ) do
    columns_to_select = Map.fetch!(opts, :queryable_columns) || Enum.map(column_info, & &1.name)
    generated_cols = Enum.filter(column_info, &(&1.is_generated and &1.name in columns_to_select))

    if generated_cols == [] or supports_generated_column_replication do
      all_columns = Enum.sort(columns_to_select)
      {:ok, all_columns, all_columns}
    else
      err_msg =
        "The following columns are generated and cannot be included in the shape: " <>
          (generated_cols |> Enum.map(& &1.name) |> Enum.join(", ")) <>
          ". You can exclude them from the shape by explicitly listing which columns " <>
          "to fetch in the 'columns' query param"

      {:error, {:columns, [err_msg]}}
    end
  end

  defp validate_queryable_columns(column_info, pk_cols, opts) when is_map(opts) do
    case Map.get(opts, :queryable_columns) do
      nil -> {:ok, nil}
      queryable_columns -> validate_queryable_columns(column_info, pk_cols, queryable_columns)
    end
  end

  defp validate_queryable_columns(column_info, pk_cols, queryable_columns)
       when is_list(queryable_columns) do
    all_column_names = Enum.map(column_info, & &1.name)

    missing_pk_cols = pk_cols -- queryable_columns
    invalid_cols = queryable_columns -- all_column_names

    err_msg =
      cond do
        missing_pk_cols != [] ->
          "The list of queryable columns must include all primary key columns, missing: " <>
            Enum.join(missing_pk_cols, ", ")

        invalid_cols != [] ->
          "The following queryable columns are not found on the table: " <>
            Enum.join(invalid_cols, ", ")

        queryable_columns == [] ->
          "The list of queryable columns must not be empty"

        true ->
          nil
      end

    if is_nil(err_msg) do
      {:ok, Enum.sort(queryable_columns)}
    else
      {:error, {:queryable_columns, [err_msg]}}
    end
  end

  defp table_not_found_error(relation),
    do:
      {:error,
       {:table,
        [
          "Table #{Electric.Utils.inspect_relation(relation)} does not exist. " <>
            "If the table name contains capitals or special characters you must quote it."
        ]}}

  defp connection_not_available_error(),
    do:
      {:error,
       {:connection_not_available,
        [
          "Cannot connect to the database to verify the shape. Please try again later."
        ]}}

  defp load_column_info({oid, relation}, inspector) do
    case Inspector.load_column_info(oid, inspector) do
      :table_not_found ->
        # Rare but technically possible if a `clean` call was made to the inspector between
        # validating the relation and here.
        table_not_found_error(relation)

      {:error, :connection_not_available} ->
        connection_not_available_error()

      {:ok, column_info} ->
        Logger.debug(
          "Table #{inspect(relation)} found with #{length(column_info)} columns. \n" <>
            "Column info: #{inspect(column_info)}"
        )

        pk_cols = Inspector.get_pk_cols(column_info)

        {:ok, column_info, pk_cols}
    end
  end

  defp load_supported_features(inspector) do
    case Inspector.load_supported_features(inspector) do
      {:ok, features} -> {:ok, features}
      {:error, :connection_not_available} -> connection_not_available_error()
    end
  end

  defp any_columns_non_primitive?(_, nil), do: false

  defp any_columns_non_primitive?(column_info, where) do
    unqualified_refs =
      Expr.unqualified_refs(where)

    column_info
    |> Enum.filter(&(&1.name in unqualified_refs))
    |> Enum.any?(fn
      %{type_kind: kind} when kind in [:enum, :domain, :composite] -> true
      _ -> false
    end)
  end

  defp any_columns_generated?(column_info, selected_columns) when is_list(selected_columns) do
    column_info
    |> Enum.filter(&(&1.name in selected_columns))
    |> Enum.any?(fn
      %{is_generated: true} -> true
      _ -> false
    end)
  end

  @spec validate_relation(map(), term()) ::
          {:ok, Electric.oid_relation()} | {:error, {:table, [String.t()]}}
  defp validate_relation(%{relation: relation}, inspector) do
    # Parse identifier locally first to avoid hitting PG for invalid tables
    case Inspector.load_relation_oid(relation, inspector) do
      {:ok, rel} -> {:ok, rel}
      {:error, :connection_not_available} -> connection_not_available_error()
      :table_not_found -> table_not_found_error(relation)
      {:error, err} -> {:error, {:table, [err]}}
    end
  end

  def verify_replica(mode) when mode in [:full, "full"], do: {:ok, :full}
  def verify_replica(mode) when mode in [:default, "default"], do: {:ok, :default}

  def verify_replica(invalid),
    do:
      {:error,
       "Invalid value for replica: #{inspect(invalid)}. Expecting one of `full` or `default`"}

  @doc """
  List tables that are a part of this shape.
  """
  @spec affected_tables(t()) :: [Electric.relation()]
  def affected_tables(%__MODULE__{root_table: table}) do
    [table]
  end

  @doc """
  Convert a change to be correctly represented within the shape.

  New or deleted changes are either propagated as-is, or filtered out completely.
  Updates, on the other hand, may be converted to an "new record" or a "deleted record"
  if the previous/new version of the updated row isn't in the shape.
  """
  def convert_change(shape, change, opts \\ []) do
    opts = Map.new(opts)
    do_convert_change(shape, change, opts)
  end

  @doc """
  Convert an insert or delete whose shape membership routing has already decided.

  `convert_change/3` re-evaluates the where clause to derive membership, but on the router's
  shared-conversion path that answer is already known: the change reaches this shape because
  `Filter.affected_shapes_exact/2` reported it as an exact match, not as a shape routing
  could not rule out. Re-running the predicate interpreter there is the single largest cost
  on the routing path at storm fan-out.

  Restricted to fan-out-class shapes (no move tags, no dependencies) and to inserts and
  deletes, where membership is the only thing the where clause decides — the exact
  eligibility the router's prepare path already enforces. The empty metadata matches what
  `convert_change/3` computes for a tag-less shape.
  """
  def convert_routed_change(%__MODULE__{root_table: table}, %{relation: relation})
      when table != relation,
      do: []

  def convert_routed_change(
        %__MODULE__{
          tag_structure: [],
          shape_dependencies: [],
          where: nil,
          locale: nil,
          flags: %{selects_all_columns: true}
        },
        change
      )
      when is_struct(change, Changes.NewRecord) or is_struct(change, Changes.DeletedRecord) do
    # Mirrors the whole-table clause of `do_convert_change/3`: no column projection at
    # all, so columns the inspector didn't know at shape creation still pass through.
    [%{change | move_tags: [], active_conditions: []}]
  end

  def convert_routed_change(
        %__MODULE__{tag_structure: [], shape_dependencies: []} = shape,
        change
      )
      when is_struct(change, Changes.NewRecord) or is_struct(change, Changes.DeletedRecord) do
    [
      %{change | move_tags: [], active_conditions: []}
      |> filter_change_columns(shape)
    ]
  end

  # Already converted by the router on behalf of this change's whole fan-out class. Membership and
  # column projection are both settled, and re-deriving either would only reproduce them. This
  # clause has to come first: the `where: nil, selects_all_columns: true` clause below matches any
  # change at all, so without it a prepared change would fall through to `from_change/4`, and the
  # clauses after that match on concrete change structs and would raise.
  defp do_convert_change(%__MODULE__{}, %Changes.PreparedChange{} = prepared, _), do: [prepared]

  defp do_convert_change(%__MODULE__{root_table: table}, %{relation: relation}, _)
       when table != relation,
       do: []

  defp do_convert_change(
         %__MODULE__{where: nil, locale: nil, flags: %{selects_all_columns: true}} = shape,
         change,
         opts
       ) do
    # If the change actually doesn't change any columns, we can skip it - this is possible on Postgres but we don't care for those.
    if is_struct(change, Changes.UpdatedRecord) and MapSet.size(change.changed_columns) == 0 do
      []
    else
      [fill_move_tags(change, shape, opts[:stack_id], opts[:shape_handle])]
    end
  end

  defp do_convert_change(%__MODULE__{}, %Changes.TruncatedRelation{} = change, _),
    do: [change]

  defp do_convert_change(
         %__MODULE__{} = shape,
         %Changes.NewRecord{record: record} = change,
         opts
       ) do
    {_old_refs, new_refs} = opts[:extra_refs] || {%{}, %{}}

    case project_row_metadata(shape, record, new_refs, opts) do
      {:ok, true, metadata} ->
        [change |> put_row_metadata(metadata) |> filter_change_columns(shape)]

      {:ok, false, _metadata} ->
        []
    end
  end

  defp do_convert_change(
         %__MODULE__{} = shape,
         %Changes.DeletedRecord{old_record: record} = change,
         opts
       ) do
    {old_refs, _new_refs} = opts[:extra_refs] || {%{}, %{}}

    case project_row_metadata(shape, record, old_refs, opts) do
      {:ok, true, metadata} ->
        [change |> put_row_metadata(metadata) |> filter_change_columns(shape)]

      {:ok, false, _metadata} ->
        []
    end
  end

  defp do_convert_change(
         %__MODULE__{} = shape,
         %Changes.UpdatedRecord{old_record: old_record, record: record} = change,
         opts
       ) do
    {old_refs, new_refs} = opts[:extra_refs] || {%{}, %{}}

    {:ok, old_included?, old_metadata} = project_row_metadata(shape, old_record, old_refs, opts)
    {:ok, new_included?, new_metadata} = project_row_metadata(shape, record, new_refs, opts)

    converted_changes =
      case {old_included?, new_included?} do
        {true, true} ->
          [
            put_updated_metadata(change, new_metadata,
              removed_move_tags: old_metadata.move_tags -- new_metadata.move_tags
            )
          ]

        {true, false} ->
          [
            Changes.convert_update(change, to: :deleted_record)
            |> put_row_metadata(old_metadata)
          ]

        {false, true} ->
          [
            Changes.convert_update(change, to: :new_record)
            |> put_row_metadata(new_metadata)
          ]

        {false, false} ->
          []
      end

    converted_changes
    |> Enum.map(&filter_change_columns(&1, shape))
    |> Enum.filter(&should_keep_change?/1)
  end

  defp project_row_metadata(
         %__MODULE__{where: where} = shape,
         record,
         refs,
         %{dnf_plan: %DnfPlan{} = dnf_plan, stack_id: stack_id, shape_handle: shape_handle}
       ) do
    encoding = composite_tag_encoding(shape)

    case get_row_metadata(dnf_plan, record, refs, where, stack_id, shape_handle, encoding) do
      {:ok, included?, move_tags, active_conditions} ->
        {:ok, included?, %{move_tags: move_tags, active_conditions: active_conditions}}
    end
  end

  defp project_row_metadata(
         %__MODULE__{where: where, tag_structure: tag_structure} = shape,
         record,
         refs,
         opts
       ) do
    {:ok,
     WhereClause.includes_record?(where, record, WhereClause.subquery_member_from_refs(refs)),
     %{
       move_tags: make_tags_from_pattern(shape, record, opts[:stack_id], opts[:shape_handle]),
       active_conditions: make_active_conditions(tag_structure)
     }}
  end

  # Projection comes first: a shape selecting ["id", "name"] does not list the backing "name_EN", so
  # filtering ahead of the rename would discard the very column the rename reads.
  defp filter_change_columns(change, %__MODULE__{} = shape) do
    change
    |> Changes.project_locale(
      shape.locale,
      shape.localized_columns,
      Map.get(shape, :localized_backing_columns, [])
    )
    |> then(
      &if(shape.selected_columns,
        do: Changes.filter_columns(&1, shape.selected_columns),
        else: &1
      )
    )
  end

  defp put_row_metadata(change, %{move_tags: move_tags, active_conditions: active_conditions}) do
    %{change | move_tags: move_tags, active_conditions: active_conditions}
  end

  defp put_updated_metadata(
         change,
         %{move_tags: move_tags, active_conditions: active_conditions},
         opts
       ) do
    %{
      change
      | move_tags: move_tags,
        removed_move_tags: Keyword.get(opts, :removed_move_tags, []),
        active_conditions: active_conditions
    }
  end

  def fill_move_tags(change, %__MODULE__{tag_structure: []}, _, _), do: change

  def fill_move_tags(%Changes.NewRecord{move_tags: [_ | _]} = change, _, _, _), do: change
  def fill_move_tags(%Changes.UpdatedRecord{move_tags: [_ | _]} = change, _, _, _), do: change
  def fill_move_tags(%Changes.DeletedRecord{move_tags: [_ | _]} = change, _, _, _), do: change

  def fill_move_tags(
        %Changes.NewRecord{record: record} = change,
        %__MODULE__{tag_structure: tag_structure} = shape,
        stack_id,
        shape_handle
      ) do
    move_tags = make_tags_from_pattern(shape, record, stack_id, shape_handle)
    active_conditions = make_active_conditions(tag_structure)
    %{change | move_tags: move_tags, active_conditions: active_conditions}
  end

  def fill_move_tags(
        %Changes.UpdatedRecord{record: record, old_record: old_record} = change,
        %__MODULE__{tag_structure: tag_structure} = shape,
        stack_id,
        shape_handle
      ) do
    move_tags = make_tags_from_pattern(shape, record, stack_id, shape_handle)

    old_move_tags =
      make_tags_from_pattern(shape, old_record, stack_id, shape_handle) --
        move_tags

    active_conditions = make_active_conditions(tag_structure)

    %{
      change
      | move_tags: move_tags,
        removed_move_tags: old_move_tags,
        active_conditions: active_conditions
    }
  end

  def fill_move_tags(
        %Changes.DeletedRecord{old_record: record} = change,
        %__MODULE__{tag_structure: tag_structure} = shape,
        stack_id,
        shape_handle
      ) do
    active_conditions = make_active_conditions(tag_structure)

    %{
      change
      | move_tags: make_tags_from_pattern(shape, record, stack_id, shape_handle),
        active_conditions: active_conditions
    }
  end

  def get_row_metadata(
        dnf_plan,
        record,
        views,
        where_expr,
        stack_id,
        shape_handle,
        encoding \\ :length_prefixed
      ) do
    {:ok, ref_values} = Runner.record_to_ref_values(where_expr.used_refs, record)
    active_conditions = compute_active_conditions(dnf_plan, Map.merge(ref_values, views))

    tags = compute_tags(dnf_plan, record, stack_id, shape_handle, encoding)
    included? = compute_inclusion(dnf_plan, active_conditions)
    {:ok, included?, tags, active_conditions}
  end

  defp compute_active_conditions(dnf_plan, refs) do
    Enum.map(0..(dnf_plan.position_count - 1), fn pos ->
      info = dnf_plan.positions[pos]
      pos_expr = Expr.wrap_parser_part(info.ast)

      case Runner.execute(pos_expr, refs) do
        # Negation must not turn "could not be evaluated" into inclusion: a row would enter the
        # shape because its predicate was unreadable, which is a leak rather than a match.
        {:error, _func} ->
          # The same signal `WhereClause.includes_record?/3` gives a shape without subqueries.
          :telemetry.execute(
            [:electric, :shape, :unevaluable_row_excluded],
            %{count: 1},
            %{refs: pos_expr.used_refs}
          )

          false

        # `NOT unknown` is unknown, not true: a predicate evaluating to SQL NULL never activates a
        # position, negated or not, or the negation admits a row the 3VL snapshot SQL excludes.
        {:ok, nil} ->
          false

        {:ok, value} ->
          active? = value not in [nil, false]
          if info.negated?, do: not active?, else: active?
      end
    end)
  end

  defp compute_tags(dnf_plan, record, stack_id, shape_handle, encoding) do
    Enum.map(dnf_plan.disjuncts, fn conj ->
      positions_in_disjunct = MapSet.new(conj, &elem(&1, 0))

      Enum.map(0..(dnf_plan.position_count - 1), fn pos ->
        if MapSet.member?(positions_in_disjunct, pos) do
          compute_tag_slot(dnf_plan.positions[pos], record, stack_id, shape_handle, encoding)
        else
          ""
        end
      end)
      |> Enum.join("/")
    end)
  end

  defp compute_tag_slot(
         %{subquery?: true, tag_columns: [col]},
         record,
         stack_id,
         shape_handle,
         _encoding
       ) do
    SubqueryTags.make_value_hash(stack_id, shape_handle, Map.get(record, col))
  end

  defp compute_tag_slot(
         %{subquery?: true, tag_columns: {:hash_together, cols}},
         record,
         stack_id,
         shape_handle,
         encoding
       ) do
    composite_tag_hash(cols, record, stack_id, shape_handle, encoding)
  end

  defp compute_tag_slot(%{subquery?: false}, _record, _stack_id, _shape_handle, _encoding) do
    "1"
  end

  defp compute_inclusion(dnf_plan, active_conditions) do
    Enum.any?(dnf_plan.disjuncts, fn conj ->
      Enum.all?(conj, fn {pos, _polarity} ->
        Enum.at(active_conditions, pos)
      end)
    end)
  end

  defp make_active_conditions([]), do: []
  defp make_active_conditions(tag_structure), do: List.duplicate(true, length(tag_structure))

  defp make_tags_from_pattern(
         %__MODULE__{tag_structure: patterns} = shape,
         record,
         stack_id,
         shape_handle
       ) do
    encoding = composite_tag_encoding(shape)

    Enum.map(patterns, fn pattern ->
      Enum.map(pattern, fn
        column_name when is_binary(column_name) ->
          SubqueryTags.make_value_hash(stack_id, shape_handle, Map.get(record, column_name))

        {:hash_together, columns} ->
          composite_tag_hash(columns, record, stack_id, shape_handle, encoding)
      end)
      |> Enum.join("/")
    end)
  end

  defp composite_tag_hash(columns, record, stack_id, shape_handle, encoding) do
    fields = Enum.map(columns, &{&1, Map.get(record, &1)})
    SubqueryTags.make_composite_hash(stack_id, shape_handle, fields, encoding)
  end

  defp should_keep_change?(%Changes.UpdatedRecord{removed_move_tags: removed_move_tags})
       when removed_move_tags != [], do: true

  defp should_keep_change?(%Changes.UpdatedRecord{
         old_record: record,
         record: record,
         active_conditions: [_ | _]
       }),
       do: true

  defp should_keep_change?(%Changes.UpdatedRecord{old_record: record, record: record}),
    do: false

  defp should_keep_change?(_), do: true

  # If neither oid nor schema/table name matches, then shape is not affected
  def is_affected_by_relation_change?(
        %__MODULE__{root_table_id: id1, root_table: {schema1, table1}},
        %Changes.Relation{id: id2, schema: schema2, table: table2}
      )
      when id1 != id2 and (schema1 != schema2 or table1 != table2),
      do: false

  # If relation OID matches, but qualified table name does not, then shape is affected
  def is_affected_by_relation_change?(
        %__MODULE__{root_table_id: id, root_table: {shape_schema, shape_table}},
        %Changes.Relation{id: id, schema: schema, table: table}
      )
      when shape_schema != schema or shape_table != table,
      do: true

  # If qualified table is the same but OID is different, it affects this shape as
  # it means that its root table has been renamed or deleted
  def is_affected_by_relation_change?(
        %__MODULE__{root_table: {schema, table}, root_table_id: old_id},
        %Changes.Relation{schema: schema, table: table, id: new_id}
      )
      when old_id !== new_id,
      do: true

  # If shape selects all columns, but number of columns has changed, it affects this shape
  def is_affected_by_relation_change?(
        %__MODULE__{flags: %{selects_all_columns: true}, root_column_count: old_column_count},
        %Changes.Relation{columns: new_columns}
      )
      when length(new_columns) != old_column_count,
      do: true

  def is_affected_by_relation_change?(
        %__MODULE__{selected_columns: columns} = shape,
        %Changes.Relation{affected_columns: affected_columns}
      ) do
    # The backing columns are read by the snapshot without appearing in the selection, so a change to one of
    # them would otherwise leave the shape live while its rows stop being projectable.
    Enum.any?(
      columns ++ Map.get(shape, :localized_backing_columns, []),
      &(&1 in affected_columns)
    )
  end

  @doc false
  @spec to_json_safe(t()) :: map()
  def to_json_safe(%__MODULE__{} = shape) do
    %{
      version: 1,
      root_table: Tuple.to_list(shape.root_table),
      root_table_id: shape.root_table_id,
      root_pks: shape.root_pk,
      root_column_count: shape.root_column_count,
      flags: shape.flags,
      where: shape.where,
      selected_columns: shape.selected_columns,
      explicitly_selected_columns: shape.explicitly_selected_columns,
      queryable_columns: shape.queryable_columns,
      storage: shape.storage,
      replica: shape.replica,
      shape_dependencies: Enum.map(shape.shape_dependencies, &to_json_safe/1),
      composite_tag_encoding: composite_tag_encoding(shape),
      log_mode: shape.log_mode,
      source_positions: Map.get(shape, :source_positions, :legacy),
      source_generation: Map.get(shape, :source_generation),
      locale: Map.get(shape, :locale),
      localized_columns: Map.get(shape, :localized_columns, []),
      localized_backing_columns: Map.get(shape, :localized_backing_columns, [])
    }
  end

  @spec from_json_safe(map()) :: {:ok, t()} | {:error, String.t()}
  def from_json_safe(
        %{
          "version" => 1,
          "root_table" => [schema, name],
          "root_table_id" => root_table_id,
          "root_pks" => root_pks,
          "root_column_count" => root_column_count,
          "flags" => flags,
          "where" => where,
          "selected_columns" => selected_columns,
          "storage" => storage,
          "replica" => replica
        } = data
      ) do
    with {:ok, shape_dependencies} <-
           Utils.map_while_ok(Map.get(data, "shape_dependencies", []), &from_json_safe/1),
         {:ok, where} <-
           if(where != nil,
             do: Expr.from_json_safe(where, extract_sublink_queries(shape_dependencies)),
             else: {:ok, nil}
           ) do
      {:ok,
       %__MODULE__{
         root_table: {schema, name},
         root_table_id: root_table_id,
         root_pk: root_pks,
         root_column_count: root_column_count,
         flags: Map.new(flags, fn {k, v} -> {String.to_existing_atom(k), v} end),
         where: where,
         selected_columns: selected_columns,
         explicitly_selected_columns:
           Map.get(data, "explicitly_selected_columns", selected_columns),
         queryable_columns: Map.get(data, "queryable_columns"),
         storage: storage_config_from_json(storage),
         replica: String.to_existing_atom(replica),
         shape_dependencies: shape_dependencies,
         composite_tag_encoding:
           String.to_existing_atom(Map.get(data, "composite_tag_encoding", "legacy")),
         log_mode: String.to_existing_atom(Map.get(data, "log_mode", "full")),
         source_positions: String.to_existing_atom(Map.get(data, "source_positions", "legacy")),
         source_generation: Map.get(data, "source_generation"),
         locale: Map.get(data, "locale"),
         localized_columns: Map.get(data, "localized_columns", []),
         localized_backing_columns: Map.get(data, "localized_backing_columns", [])
       }
       |> fill_tag_structure()}
    end
  end

  # This implementation is kept for backwards compatibility, because we're currently not doing
  # cleanup of old shape files if the definition is malformed.
  def from_json_safe(
        %{
          "root_table" => [schema, name],
          "root_table_id" => root_table_id,
          "where" => where,
          "selected_columns" => selected_columns,
          "table_info" => info
        } = data
      )
      when not is_map_key(data, "version") do
    table_info =
      Enum.reduce(info, %{}, fn [[schema, name], table_info], info ->
        %{"columns" => columns, "pk" => pk} = table_info

        Map.put(info, {schema, name}, %{
          columns: Enum.map(columns, fn column -> Map.new(column, &column_info_from_json/1) end),
          pk: pk
        })
      end)

    %{columns: column_info, pk: pk} = Map.fetch!(table_info, {schema, name})

    {:ok, where} =
      case where do
        nil ->
          {:ok, nil}

        where ->
          refs = Inspector.columns_to_expr(column_info)
          {:ok, where} = Parser.parse_query(where)
          Parser.validate_where_ast(where, params: Map.get(data, "params", %{}), refs: refs)
      end

    actual_selected_columns = selected_columns || Enum.map(column_info, & &1.name)

    flags =
      Enum.reject(
        [
          if(is_nil(selected_columns), do: :selects_all_columns),
          if(any_columns_generated?(column_info, actual_selected_columns),
            do: :selects_generated_columns
          ),
          if(any_columns_non_primitive?(column_info, where),
            do: :non_primitive_columns_in_where
          )
        ],
        &is_nil/1
      )
      |> Map.new(&{&1, true})

    {:ok,
     %__MODULE__{
       root_table: {schema, name},
       root_table_id: root_table_id,
       root_pk: pk,
       root_column_count: length(column_info),
       flags: flags,
       where: where,
       selected_columns: actual_selected_columns,
       queryable_columns: Map.get(data, "queryable_columns"),
       composite_tag_encoding: :legacy,
       replica: String.to_atom(Map.get(data, "replica", "default")),
       storage: storage_config_from_json(Map.get(data, "storage"))
     }}
  end

  defp storage_config_from_json(nil), do: %{compaction: :disabled}
  defp storage_config_from_json(%{"compaction" => "enabled"}), do: %{compaction: :enabled}
  defp storage_config_from_json(%{"compaction" => "disabled"}), do: %{compaction: :disabled}

  defp column_info_from_json({"type_id", [id, mod]}), do: {:type_id, {id, mod}}
  defp column_info_from_json({"type_kind", kind}), do: {:type_kind, String.to_existing_atom(kind)}
  defp column_info_from_json({"type", type}), do: {:type, String.to_atom(type)}
  defp column_info_from_json({key, value}), do: {String.to_atom(key), value}

  @doc false
  def log_description(%__MODULE__{} = shape) do
    if RequestValues.enabled?(), do: inspect(shape.root_table), else: inspect(shape)
  end

  def otel_attrs(handle, shape, extra \\ []) do
    where = if(shape.where, do: shape.where.query)

    where_attrs =
      if RequestValues.enabled?(),
        do: [
          "shape.where.present": not is_nil(where),
          "shape.where.bytes": RequestValues.bytes(where)
        ],
        else: ["shape.where": where]

    Keyword.merge(
      ["shape.handle": handle, "shape.root_table": shape.root_table] ++ where_attrs,
      extra_attrs(extra)
    )
  end

  # Mapping function for known attrs to avoid repeatedly concatenating OTel attribute names at runtime.
  defp extra_attrs([{:query_reason, query_reason} | rest]),
    do: [{:"shape.query_reason", query_reason} | extra_attrs(rest)]

  defp extra_attrs([]), do: []
end

defimpl Inspect, for: Electric.Shapes.Shape do
  import Inspect.Algebra

  def inspect(%Electric.Shapes.Shape{} = shape, opts) do
    %{root_table: {schema, table}, root_table_id: root_table_id} = shape

    kwlist = []

    # some tests have invalid, unparsed, where clauses
    kwlist =
      case shape.where do
        %{query: query} -> [{:where, query} | kwlist]
        query when is_binary(query) -> [{:where, query} | kwlist]
        nil -> kwlist
      end

    kwlist =
      case shape.flags do
        %{selects_all_columns: true} ->
          kwlist

        _ ->
          [{:columns, shape.explicitly_selected_columns} | kwlist]
      end

    kwlist =
      case {shape.shape_dependencies, shape.shape_dependencies_handles} do
        {[], _} ->
          kwlist

        {deps, []} ->
          [{:deps, deps} | kwlist]

        {deps, handles} ->
          [{:deps, Enum.zip(handles, deps)} | kwlist]
      end

    base =
      concat([
        to_doc(Shape, opts),
        ".new!({",
        to_doc(root_table_id, opts),
        ", ",
        color_doc(concat([~S|"|, schema, ".", table, ~S|"|]), :string, opts),
        "}"
      ])

    if kwlist != [] do
      base
      |> concat(", ")
      |> container_doc(Enum.reverse(kwlist), ")", opts, fn {key, value}, opts ->
        concat([to_string(key), ": ", to_doc(value, opts)])
      end)
    else
      concat(base, ")")
    end
  end
end

defimpl Jason.Encoder, for: Electric.Shapes.Shape do
  def encode(shape, opts) do
    shape
    |> Electric.Shapes.Shape.to_json_safe()
    |> Jason.Encode.map(opts)
  end
end
