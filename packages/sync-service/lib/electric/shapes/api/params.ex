defmodule Electric.Shapes.Api.Params do
  use Ecto.Schema

  alias Electric.Replication.LogOffset
  alias Electric.Shapes.Api
  alias Electric.Shapes.DnfPlan
  alias Electric.Shapes.Shape

  import Ecto.Changeset

  @tmp_compaction_flag :experimental_compaction

  @primary_key false
  defmodule ColumnList do
    use Ecto.Type

    def type, do: {:array, :string}

    def cast([_ | _] = columns) do
      validate_column_names(columns)
    end

    def cast(columns) when is_binary(columns) do
      with {:error, reason} <- Electric.Plug.Utils.parse_columns_param(columns) do
        {:error, message: reason}
      end
    end

    def cast(_), do: :error

    def load([_ | _] = columns), do: {:ok, columns}

    def dump([_ | _] = columns), do: {:ok, columns}

    defp validate_column_names(columns) do
      Enum.reduce_while(columns, {:ok, columns}, fn
        "", _acc -> {:halt, {:error, message: "Invalid zero-length identifier"}}
        _, acc -> {:cont, acc}
      end)
    end
  end

  defmodule JsonOrMapStringParams do
    @moduledoc """
    Custom Ecto type that accepts params as either:
    1. A JSON string (e.g., "{\"1\":\"value1\",\"2\":\"value2\"}")
    2. A map with string values (for backwards compatibility)
    """
    use Ecto.Type

    def type, do: {:map, :string}

    # Decoded as an ordered object, since a plain decode keeps the last of a repeated key while
    # a proxy that checked the params may have read the first.
    def cast(params) when is_binary(params) do
      case Jason.decode(params, objects: :ordered_objects) do
        {:ok, %Jason.OrderedObject{values: pairs}} ->
          if length(Enum.uniq_by(pairs, &elem(&1, 0))) == length(pairs),
            do: stringify_values(pairs),
            else: {:error, message: "params must not repeat a key"}

        {:ok, _} ->
          {:error, message: "params must be a JSON object"}

        {:error, _} ->
          {:error, message: "params must be valid JSON"}
      end
    end

    def cast(params) when is_map(params) do
      # Backwards compatibility: accept map directly
      stringify_values(params)
    end

    def cast(_), do: :error

    # A JSON object value can itself be an object or an array, and `to_string/1` raises on those
    # rather than answering with something a changeset can carry — so casting them turned a
    # malformed request into a crash instead of a 400.
    defp stringify_values(params) do
      Enum.reduce_while(params, {:ok, %{}}, fn
        {key, value}, {:ok, acc}
        when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value) ->
          {:cont, {:ok, Map.put(acc, to_string(key), to_string(value))}}

        _pair, _acc ->
          {:halt, {:error, message: "params values must be strings, numbers, booleans or null"}}
      end)
    end

    def load(data) when is_map(data), do: {:ok, data}
    def load(_), do: :error

    def dump(data) when is_map(data), do: {:ok, data}
    def dump(_), do: :error
  end

  defmodule SubsetParams do
    use Ecto.Schema
    alias Electric.Shapes.Shape

    embedded_schema do
      field(:order_by, :string)
      field(:limit, :integer)
      field(:offset, :integer)
      field(:where, :string)
      field(:params, JsonOrMapStringParams, default: %{})

      field(:result, :any, virtual: true)
    end

    def changeset(struct, params, shape_definition, api) do
      struct
      |> cast(params, __schema__(:fields) -- [:result])
      |> validate_number(:limit, greater_than: 0)
      |> validate_number(:offset, greater_than_or_equal_to: 0)
      |> validate_ordered_when_limited()
      |> cast_subset(shape_definition, api)
    end

    defp cast_subset(%Ecto.Changeset{valid?: false} = changeset, _shape_definition, _api),
      do: changeset

    defp cast_subset(changeset, shape_definition, api) do
      case Shape.Subset.new(shape_definition, changeset.changes, api) do
        {:ok, subset} ->
          put_change(changeset, :result, subset)

        {:error, {field, reason}} ->
          add_error(changeset, field, reason)
      end
    end

    defp validate_ordered_when_limited(changeset) do
      if changed?(changeset, :limit) or changed?(changeset, :offset) do
        validate_required(changeset, [:order_by],
          message: "order_by is required when limit or offset is present"
        )
      else
        changeset
      end
    end

    def extract_result({:ok, data}, from: key) do
      {:ok,
       Map.update!(data, key, fn
         nil -> nil
         %{result: result} -> result
       end)}
    end

    def extract_result(data, _), do: data
  end

  embedded_schema do
    field(:table, :string)
    field(:offset, :string)
    field(:handle, :string)
    field(:live, :boolean, default: false)
    field(:where, :string)
    field(:columns, ColumnList)
    field(:queryable_columns, ColumnList)
    field(:locale, :string)
    field(:localized_columns, ColumnList)
    field(:shape_definition, :string)
    field(:replica, Ecto.Enum, values: [:default, :full], default: :default)
    field(:params, {:map, :string}, default: %{})
    field(@tmp_compaction_flag, :boolean, default: false)
    field(:live_sse, :boolean, default: false)
    field(:log, Ecto.Enum, values: [:changes_only, :full], default: :full)
    field(:source_positions, Ecto.Enum, values: [:legacy, :full_xid], default: :legacy)
    field(:wait_for_lsn, :integer)

    embeds_one(:subset, SubsetParams)
  end

  @type t() :: %__MODULE__{}

  def compaction_enabled?(%__MODULE__{} = params) do
    Map.fetch!(params, @tmp_compaction_flag)
  end

  def validate(%Api{subset_max_rows: max_rows} = api, params) when is_integer(max_rows) do
    case Ecto.Type.cast(:integer, subset_limit(params)) do
      {:ok, limit} when is_integer(limit) and limit > max_rows ->
        errors = %{subset: %{limit: ["must be at most #{max_rows}"]}}
        {:error, Api.Response.invalid_request(api, errors: errors)}

      _ ->
        validate_params(api, params)
    end
  end

  def validate(%Api{} = api, params), do: validate_params(api, params)

  defp subset_limit(%{"subset" => subset}) when is_map(subset), do: subset_limit_value(subset)
  defp subset_limit(%{subset: subset}) when is_map(subset), do: subset_limit_value(subset)
  defp subset_limit(_params), do: nil

  defp subset_limit_value(subset), do: Map.get(subset, "limit", Map.get(subset, :limit))

  defp validate_params(api, params) do
    params
    |> cast_params()
    |> validate_required([:offset])
    |> cast_offset()
    |> validate_handle_with_offset()
    |> validate_live_with_offset()
    |> validate_live_sse()
    |> validate_source_positions(api)
    |> cast_root_table(api)
    |> validate_source_positions(api)
    |> validate_wait_for_lsn()
    |> cast_subset(api)
    |> apply_action(:validate)
    |> SubsetParams.extract_result(from: :subset)
    |> convert_error(api)
  end

  # we allow deletion by shape definition, shape definition and handle or just
  # handle
  def validate_for_delete(%Electric.Shapes.Api{} = api, params) do
    params
    |> cast_params()
    |> case do
      # if the params specify a table then the request includes the shape
      # definition (and maybe handle) for a deletion we don't need to validate
      # the offset or live flags etc
      %{changes: %{table: _table}} = changeset ->
        changeset
        |> validate_required([:table])
        |> cast_root_table(api)
        |> apply_action(:validate)
        |> convert_error(api)

      # if no table is specified, then just validate that there's a handle
      changeset ->
        changeset
        |> validate_required([:handle],
          message: "can't be blank when shape definition is missing"
        )
        |> apply_action(:validate)
        |> convert_error(api)
    end
  end

  defp cast_params(params) do
    %__MODULE__{}
    |> cast(params, __schema__(:fields) -- [:shape_definition, :subset])
  end

  defp convert_error({:ok, params}, _api), do: {:ok, params}

  defp convert_error({:error, changeset}, api) do
    reason =
      traverse_errors(changeset, fn {msg, opts} ->
        Regex.replace(~r"%{(\w+)}", msg, fn _, key ->
          # A message can carry text the request supplied — an unknown column echoed back is one —
          # so the placeholder name is not ours to trust. Resolving it through `to_existing_atom`
          # raised on anything that was not already an atom, turning a rejected request into a 500.
          opts
          |> Enum.find_value(key, fn {opt, value} -> if Atom.to_string(opt) == key, do: value end)
          |> to_string()
        end)
      end)

    response =
      case reason do
        %{connection_not_available: [msg]} ->
          Api.Response.error(api, msg, status: 503, retry_after: 1)

        %{source_positions: [msg]} ->
          Api.Response.error(api, msg, status: 426)

        _ ->
          Api.Response.invalid_request(api, errors: reason)
      end

    {:error, response}
  end

  def cast_offset(%Ecto.Changeset{valid?: false} = changeset), do: changeset

  def cast_offset(%Ecto.Changeset{} = changeset) do
    offset = fetch_change!(changeset, :offset)

    case LogOffset.from_string(offset) do
      {:ok, offset} ->
        put_change(changeset, :offset, offset)

      {:error, message} ->
        add_error(changeset, :offset, message)
    end
  end

  def validate_handle_with_offset(%Ecto.Changeset{valid?: false} = changeset),
    do: changeset

  def validate_handle_with_offset(%Ecto.Changeset{} = changeset) do
    offset = fetch_change!(changeset, :offset)

    cond do
      offset in [LogOffset.before_all(), :now] ->
        delete_change(changeset, :handle)

      true ->
        validate_required(changeset, [:handle], message: "can't be blank when offset != -1")
    end
  end

  def validate_live_with_offset(%Ecto.Changeset{valid?: false} = changeset), do: changeset

  def validate_live_with_offset(%Ecto.Changeset{} = changeset) do
    offset = fetch_change!(changeset, :offset)

    cond do
      offset == LogOffset.before_all() ->
        validate_exclusion(changeset, :live, [true], message: "can't be true when offset == -1")

      offset == :now ->
        validate_exclusion(changeset, :live, [true],
          message: "can't be true when offset is 'now'"
        )

      true ->
        changeset
    end
  end

  def validate_live_sse(%Ecto.Changeset{valid?: false} = changeset), do: changeset

  def validate_live_sse(%Ecto.Changeset{} = changeset) do
    live = get_field(changeset, :live)

    if live do
      changeset
    else
      validate_exclusion(changeset, :live_sse, [true],
        message: "can't be true unless live is also true"
      )
    end
  end

  def validate_wait_for_lsn(%Ecto.Changeset{valid?: false} = changeset), do: changeset

  # An empty value casts to nil, which would read as a request that does not wait. A shape with
  # subqueries buffers transactions behind its move-ins, past the position they were fanned out
  # at, so no position can vouch for its log.
  def validate_wait_for_lsn(%Ecto.Changeset{} = changeset) do
    case {get_field(changeset, :wait_for_lsn), get_field(changeset, :shape_definition)} do
      {nil, _shape} ->
        if Map.has_key?(changeset.params, "wait_for_lsn"),
          do: add_error(changeset, :wait_for_lsn, "is invalid"),
          else: changeset

      {_lsn, %Shape{shape_dependencies: [_ | _]}} ->
        add_error(changeset, :wait_for_lsn, "can't be used on a shape with subqueries")

      {_lsn, _shape} ->
        changeset
        |> validate_number(:wait_for_lsn,
          greater_than_or_equal_to: 0,
          less_than_or_equal_to: 0xFFFFFFFFFFFFFFFF
        )
        |> validate_exclusion(:live_sse, [true],
          message: "can't be true together with wait_for_lsn"
        )
    end
  end

  def cast_root_table(%Ecto.Changeset{valid?: false} = changeset, _), do: changeset

  def cast_root_table(%Ecto.Changeset{} = changeset, %Api{shape: nil} = api) do
    changeset
    |> validate_required([:table])
    |> define_shape(api)
  end

  def cast_root_table(%Ecto.Changeset{} = changeset, %Api{shape: %Shape{} = shape}) do
    if get_field(changeset, :source_positions) == :full_xid and
         Map.get(shape, :source_positions, :legacy) != :full_xid do
      add_error(changeset, :source_positions, "does not match the configured shape")
    else
      changeset
      |> put_change(:shape_definition, shape)
      |> put_change(:source_positions, Map.get(shape, :source_positions, :legacy))
    end
  end

  defp validate_source_positions(%Ecto.Changeset{valid?: false} = changeset, _api), do: changeset

  defp validate_source_positions(changeset, api) do
    if get_field(changeset, :source_positions) == :full_xid do
      cond do
        get_field(changeset, @tmp_compaction_flag) ->
          add_error(
            changeset,
            @tmp_compaction_flag,
            "cannot be enabled with full transaction source positions"
          )

        "full_xid_source_positions" not in api.feature_flags ->
          add_error(
            changeset,
            :source_positions,
            "full transaction source positions are not enabled"
          )

        not Electric.StackConfig.lookup(api.stack_id, :full_xid_source_positions_ready, false) ->
          add_error(
            changeset,
            :connection_not_available,
            "full transaction source provenance is not ready"
          )

        not is_binary(Electric.StackConfig.lookup(api.stack_id, :full_xid_source_generation)) ->
          add_error(
            changeset,
            :connection_not_available,
            "full transaction source generation is not ready"
          )

        match?(%Shape{}, get_field(changeset, :shape_definition)) and
            get_field(changeset, :shape_definition).source_generation !=
              Electric.StackConfig.lookup(api.stack_id, :full_xid_source_generation) ->
          add_error(
            changeset,
            :connection_not_available,
            "the shape belongs to an earlier source generation"
          )

        true ->
          changeset
      end
    else
      changeset
    end
  end

  defp define_shape(%Ecto.Changeset{valid?: false} = changeset, _api) do
    changeset
  end

  defp define_shape(%Ecto.Changeset{} = changeset, api) do
    table = fetch_change!(changeset, :table)
    where = fetch_field!(changeset, :where)
    columns = get_change(changeset, :columns, nil)
    queryable_columns = get_change(changeset, :queryable_columns, nil)
    locale = fetch_field!(changeset, :locale)
    localized_columns = get_change(changeset, :localized_columns, [])
    replica = fetch_field!(changeset, :replica)
    params = fetch_field!(changeset, :params)
    compaction_enabled? = fetch_field!(changeset, @tmp_compaction_flag)
    source_positions = fetch_field!(changeset, :source_positions)

    case Shape.new(
           table,
           where: where,
           params: params,
           columns: columns,
           queryable_columns: queryable_columns,
           locale: locale,
           localized_columns: localized_columns,
           replica: replica,
           inspector: api.inspector,
           feature_flags: api.feature_flags,
           storage: %{
             compaction:
               if(compaction_enabled? and source_positions == :legacy,
                 do: :enabled,
                 else: :disabled
               )
           },
           log_mode: fetch_field!(changeset, :log),
           source_positions: source_positions,
           source_generation:
             if(source_positions == :full_xid,
               do: Electric.StackConfig.lookup(api.stack_id, :full_xid_source_generation),
               else: nil
             )
         ) do
      {:ok, shape} ->
        case DnfPlan.compile(shape) do
          {:error, reason} ->
            add_error(changeset, :where, reason)

          _ok ->
            put_change(changeset, :shape_definition, shape)
        end

      {:error, :connection_not_available} ->
        add_error(
          changeset,
          :connection,
          "Cannot connect to the database to verify the table. Please try again later."
        )

      {:error, {field, reasons}} ->
        Enum.reduce(List.wrap(reasons), changeset, fn
          {message, keys}, changeset ->
            add_error(changeset, field, message, keys)

          message, changeset when is_binary(message) ->
            add_error(changeset, field, message)
        end)

      {:error, %NimbleOptions.ValidationError{message: message, key: key}} ->
        add_error(changeset, key, message)
    end
  end

  defp cast_subset(%Ecto.Changeset{valid?: false} = changeset, _api), do: changeset

  defp cast_subset(%Ecto.Changeset{} = changeset, api) do
    cast_embed(changeset, :subset,
      with:
        &SubsetParams.changeset(
          &1,
          &2,
          Ecto.Changeset.fetch_change!(changeset, :shape_definition),
          api
        ),
      required: false
    )
  end
end
