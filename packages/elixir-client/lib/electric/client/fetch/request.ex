defmodule Electric.Client.Fetch.Request do
  use GenServer

  alias Electric.Client
  alias Electric.Client.Fetch
  alias Electric.Client.Util

  require Logger

  @redacted_value "[REDACTED]"
  @sensitive_header_names MapSet.new(
                            ~w(authorization proxy-authorization cookie set-cookie x-api-key x-auth-token x-electric-secret electric-secret)
                          )
  @sensitive_param_names MapSet.new(~w(secret api_secret token access_token api_key password))

  defstruct [
    :stream_id,
    :endpoint,
    :shape_handle,
    :live,
    :next_cursor,
    # `replica` does not belong here, it's part of the shape definition that we
    # receive via the client params. the other keys in this struct are part
    # of consuming a stream rather than defining the shape.
    # kept for backwards compatibility
    replica: :default,
    method: :get,
    offset: Client.Offset.before_all(),
    params: %{},
    headers: %{},
    authenticated: false
  ]

  @type params :: %{String.t() => String.t()}
  @type headers :: %{String.t() => [String.t()] | String.t()}

  fields = [
    stream_id: quote(do: term()),
    method: quote(do: :get | :head | :delete | :options),
    endpoint: quote(do: URI.t()),
    offset: quote(do: Electric.Client.offset()),
    shape_handle: quote(do: Electric.Client.shape_handle() | nil),
    replica: quote(do: Electric.Client.replica()),
    live: quote(do: boolean()),
    next_cursor: quote(do: Electric.Client.cursor()),
    params: quote(do: params()),
    headers: quote(do: headers())
  ]

  @type unauthenticated :: %__MODULE__{unquote_splicing(fields), authenticated: false}
  @type authenticated :: %__MODULE__{unquote_splicing(fields), authenticated: true}
  @type t :: unauthenticated() | authenticated()

  # the base url should come from the client
  attrs = Keyword.delete(fields, :endpoint)

  attr_types =
    attrs
    |> Enum.reduce(nil, fn
      {name, spec}, nil -> quote(do: unquote({name, spec}))
      {name, spec}, acc -> quote(do: unquote({name, spec}) | unquote(acc))
    end)

  @type attr :: unquote(attr_types)
  @type attrs :: [attr()] | %{unquote_splicing(attrs)}

  @doc false
  def name(request_id) do
    {:via, Registry, {Electric.Client.Registry, {__MODULE__, request_id}}}
  end

  @doc """
  Returns the URL for the Request.
  """
  @spec url(t()) :: binary()
  def url(%__MODULE__{} = request, opts \\ []) do
    request
    |> uri(opts)
    |> URI.to_string()
  end

  @doc """
  Returns the %URI{} for the Request.
  """
  @spec uri(t()) :: URI.t()
  def uri(%__MODULE__{} = request, opts \\ []) do
    %{endpoint: endpoint} = request

    if Keyword.get(opts, :query, true) do
      # Convert map to _ordered_ list of query parameters
      # to ensure consistent caching
      query =
        request
        |> params()
        |> Map.to_list()
        |> List.keysort(0)
        |> URI.encode_query(:rfc3986)

      %{endpoint | query: query}
    else
      endpoint
    end
  end

  @doc false
  @spec params(t()) :: params()
  def params(%__MODULE__{} = request) do
    %{
      replica: replica,
      live: live?,
      shape_handle: shape_handle,
      offset: offset,
      next_cursor: cursor,
      params: params
    } = request

    (params || %{})
    |> Map.merge(%{"offset" => to_string(offset)})
    |> Util.map_put_if("replica", to_string(replica), replica != :default)
    |> Util.map_put_if("handle", shape_handle, is_binary(shape_handle))
    |> Util.map_put_if("live", "true", live?)
    |> Util.map_put_if("cursor", to_string(cursor), !is_nil(cursor))
  end

  @doc false
  @spec loggable(t()) :: map()
  def loggable(%__MODULE__{} = request) do
    %{
      method: request.method,
      endpoint: redact_endpoint(request.endpoint),
      shape_handle: request.shape_handle,
      headers: redact(request.headers, @sensitive_header_names),
      params: redact(params(request), @sensitive_param_names)
    }
  end

  defp redact(map, sensitive_names) do
    Map.new(map, fn {name, value} ->
      if MapSet.member?(sensitive_names, String.downcase(to_string(name))),
        do: {name, @redacted_value},
        else: {name, value}
    end)
  end

  defp redact_endpoint(%URI{} = uri) do
    query =
      case uri.query do
        nil ->
          nil

        query ->
          query |> URI.decode_query() |> redact(@sensitive_param_names) |> URI.encode_query()
      end

    userinfo = if is_nil(uri.userinfo), do: nil, else: @redacted_value

    URI.to_string(%{uri | userinfo: userinfo, query: query})
  end

  @doc false
  def child_spec({request_id, _request, _client, _monitor_pid} = args) do
    %{
      id: {__MODULE__, request_id},
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary,
      type: :worker
    }
  end

  @doc false
  def start_link({request_id, request, client, monitor_pid}) do
    GenServer.start_link(__MODULE__, {request_id, request, client, monitor_pid})
  end

  @impl true
  def init({request_id, request, client, monitor_pid}) do
    Logger.debug(fn ->
      "Starting request for #{inspect(loggable(request))}"
    end)

    state = %{
      request_id: request_id,
      request: request,
      client: client,
      monitor_pid: monitor_pid
    }

    {:ok, state, {:continue, :request}}
  end

  @impl true
  def handle_continue(:request, state) do
    %{client: client, request: request} = state
    %{fetch: {fetcher, fetcher_opts}} = client

    authenticated_request = Client.authenticate_request(client, request)

    try do
      case fetcher.fetch(authenticated_request, fetcher_opts) do
        {:ok, %Fetch.Response{status: status} = response} when status in 200..299 ->
          reply(response, state)

        {:ok, %Fetch.Response{} = response} ->
          # Turn HTTP errors into errors
          reply({:error, response}, state)

        error ->
          reply(error, state)
      end
    rescue
      error ->
        reply({:error, error}, state)
    end

    {:stop, :normal, state}
  end

  defp reply(response, %{monitor_pid: monitor_pid}) do
    Fetch.Monitor.reply(monitor_pid, response)
  end
end
