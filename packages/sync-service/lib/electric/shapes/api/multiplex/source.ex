defmodule Electric.Shapes.Api.Multiplex.Source do
  @moduledoc false

  alias Electric.Shapes
  alias Electric.Shapes.Api

  @callback active?(Api.t(), term()) :: boolean()
  @callback validate_shape(Api.t(), Electric.shape_handle(), binary(), map(), term()) ::
              :ok | :mismatch | :invalid | :unavailable
  @callback lookup(Api.t(), Electric.shape_handle(), term()) ::
              {:ok, Electric.Replication.LogOffset.t()} | :not_found
  @callback fetch_shape(Api.t(), Electric.shape_handle(), term()) ::
              {:ok, Electric.Shapes.Shape.t()} | :not_found
  @callback subscribe(Api.t(), Electric.shape_handle(), reference(), term()) :: :ok
  @callback unsubscribe(Api.t(), Electric.shape_handle(), term()) :: :ok

  def active?(%Api{stack_id: stack_id}, _opts) do
    Electric.StatusMonitor.service_status(stack_id) == :active
  end

  # The definition is parsed the way the HTTP path parses its query, so a watch
  # names the same shape the authorized shape request for its handle would. A
  # definition carrying a subset, in any of the query's spellings, is refused: the
  # shape endpoint answers a subset request at once with its rows, so a watch for
  # it has nothing to wait for, and its no-change answer is not that response.
  def validate_shape(%Api{stack_id: stack_id} = api, handle, offset, definition, _opts) do
    if Enum.any?(Map.keys(definition), &subset_key?/1) do
      :invalid
    else
      params = Map.merge(definition, %{"handle" => handle, "offset" => offset})

      case Api.Params.validate(api, params) do
        {:ok, %Api.Params{wait_for_lsn: lsn}} when lsn != nil ->
          :invalid

        {:ok, %Api.Params{shape_definition: shape}} ->
          if Shapes.validate_shape_handle(stack_id, handle, shape) == :ok,
            do: :ok,
            else: :mismatch

        {:error, %Api.Response{status: 503}} ->
          :unavailable

        {:error, _response} ->
          :invalid
      end
    end
  end

  # A bracketed key is a subset field only where the query's decoding nests it under `subset`:
  # `subset[trace` stays a key of its own, which the shape endpoint ignores.
  defp subset_key?("subset__" <> _field), do: true

  defp subset_key?(key) when is_binary(key),
    do: Electric.Plug.Utils.query_param_root(key) == "subset"

  defp subset_key?(_key), do: false

  def lookup(%Api{stack_id: stack_id}, handle, _opts) do
    with {:ok, latest_offset} <- Shapes.fetch_latest_offset(stack_id, handle) do
      {:ok, latest_offset}
    else
      _ -> :not_found
    end
  end

  def fetch_shape(%Api{stack_id: stack_id}, handle, _opts) do
    case Shapes.fetch_shape_by_handle(stack_id, handle) do
      {:ok, shape} -> {:ok, shape}
      _ -> :not_found
    end
  end

  def subscribe(%Api{stack_id: stack_id}, handle, ref, _opts) do
    ^ref = Electric.StackSupervisor.subscribe_to_shape_events(stack_id, handle, ref)
    :ok
  end

  def unsubscribe(%Api{stack_id: stack_id}, handle, _opts) do
    registry = Electric.StackSupervisor.registry_name(stack_id)

    if GenServer.whereis(registry) != nil do
      Registry.unregister(registry, handle)
    end

    :ok
  end
end
