defmodule Electric.Plug.Router do
  use Plug.Router, copy_opts_to_assign: :config
  use Electric.Telemetry

  with_telemetry Sentry.PlugCapture do
    use Sentry.PlugCapture
  end

  alias Electric.Plug.Utils.CORSHeaderPlug
  alias Electric.Plug.Utils.PassAssignToOptsPlug

  plug Plug.RequestId, assign_as: :plug_request_id
  plug :server_header, Electric.version()
  plug :add_stack_id_to_metadata
  plug :capture_head_request
  # converts HEAD requests to GET requests
  plug Plug.Head
  plug RemoteIp
  plug :match
  plug Electric.Plug.LabelProcessPlug
  plug Electric.Plug.TraceContextPlug
  plug Plug.Telemetry, event_prefix: [:electric, :routing]
  plug Plug.Logger, log: :debug

  plug :put_sentry_request_context
  plug :authenticate
  plug :put_cors_headers
  plug :dispatch

  match "/", via: [:get, :head], do: send_resp(conn, 200, "")

  get "/v1/shape",
    to: PassAssignToOptsPlug,
    init_opts: [plug: Electric.Plug.ServeShapePlug, assign_key: :config]

  get "/v1/shape/multiplex",
    to: PassAssignToOptsPlug,
    init_opts: [plug: Electric.Plug.ShapeMultiplexPlug, assign_key: :config]

  post "/v1/shape",
    to: PassAssignToOptsPlug,
    init_opts: [plug: Electric.Plug.ServeShapePlug, assign_key: :config]

  delete "/v1/shape",
    to: PassAssignToOptsPlug,
    init_opts: [plug: Electric.Plug.DeleteShapePlug, assign_key: :config]

  options "/v1/shape", to: Electric.Plug.OptionsShapePlug

  post "/v1/admin/shape-generations/reclaim",
    to: PassAssignToOptsPlug,
    init_opts: [plug: Electric.Plug.ReclaimShapeGenerationsPlug, assign_key: :config]

  get "/v1/health", to: Electric.Plug.HealthCheckPlug

  match _, do: send_resp(conn, 404, "Not found")

  def server_header(conn, version),
    do: conn |> Plug.Conn.put_resp_header("electric-server", "ElectricSQL/#{version}")

  defp put_sentry_request_context(conn, opts),
    do: Electric.Telemetry.Sentry.put_request_context(conn, opts)

  def capture_head_request(%Plug.Conn{method: "HEAD"} = conn, _),
    do: put_private(conn, :electric_head_request, true)

  def capture_head_request(conn, _), do: conn

  # OPTIONS requests should not be authenticated
  def authenticate(%Plug.Conn{method: "OPTIONS"} = conn, _opts), do: conn

  # Gate on the route `plug :match` already resolved, never on `request_path` or `path_info`. Neither
  # of those is what dispatch uses: `request_path` is the raw target, and `path_info` keeps segments
  # percent-encoded, so "/v1/%73hape" stays ["v1", "%73hape"] and misses a match on ["v1", "shape"]
  # while the router decodes at match time and serves it. The prefix form gates anything routing under
  # /v1/shape, so a future subpath is covered by default rather than by remembering to add it.
  def authenticate(
        %Plug.Conn{private: %{plug_route: {<<"/v1/shape", _::binary>>, _}}} = conn,
        _opts
      ) do
    api_secret = conn.assigns.config[:secret]

    if is_nil(api_secret) do
      # We're in insecure mode, so we don't need to authenticate
      conn
    else
      conn = conn |> fetch_query_params()

      # A bearer header wins over the query string, which proxies and load balancers may log. Keep
      # `api_secret` for backwards compatibility; we'll remove it when we release v2.
      presented =
        bearer_token(conn) || conn.query_params["secret"] || conn.query_params["api_secret"]

      if secret_matches?(api_secret, presented) do
        conn
      else
        conn
        |> send_resp(401, Jason.encode!(%{message: "Unauthorized - Invalid API secret"}))
        |> halt()
      end
    end
  end

  # Admin routes take the secret from the authorization header only, never from the query string a
  # proxy or load balancer may log, and do not exist in insecure mode.
  def authenticate(
        %Plug.Conn{private: %{plug_route: {<<"/v1/admin/", _::binary>>, _}}} = conn,
        _opts
      ) do
    if secret_matches?(conn.assigns.config[:secret], bearer_token(conn)) do
      conn
    else
      conn
      |> send_resp(401, Jason.encode!(%{message: "Unauthorized - Invalid API secret"}))
      |> halt()
    end
  end

  # For unmatched routes, just pass through
  def authenticate(conn, _opts), do: conn

  defp bearer_token(conn) do
    case get_req_header(conn, "authorization") do
      ["Bearer " <> token] -> token
      _ -> nil
    end
  end

  # `nil` already means insecure mode above, so a blank secret here is a misconfiguration -- honouring
  # it would make `?secret=` a skeleton key. Config validation rejects it at boot; the gate refuses it
  # too so it does not depend on how the config was assembled. Constant-time because `==` returns at
  # the first differing byte, leaking the secret to anyone timing the 401.
  defp secret_matches?(api_secret, presented)
       when is_binary(api_secret) and is_binary(presented) do
    String.trim(api_secret) != "" and Plug.Crypto.secure_compare(api_secret, presented)
  end

  defp secret_matches?(_api_secret, _presented), do: false

  # Same basis as `authenticate/2` above, and for the same reason.
  def put_cors_headers(
        %Plug.Conn{private: %{plug_route: {<<"/v1/shape", _::binary>>, _}}} = conn,
        _opts
      ),
      do: CORSHeaderPlug.call(conn, %{methods: ["GET", "POST", "HEAD", "DELETE", "OPTIONS"]})

  def put_cors_headers(conn, _opts),
    do: CORSHeaderPlug.call(conn, %{methods: ["GET", "HEAD"]})

  def add_stack_id_to_metadata(conn, _) do
    Logger.metadata(stack_id: conn.assigns.config[:stack_id])
    Electric.Telemetry.Sentry.set_tags_context(stack_id: conn.assigns.config[:stack_id])
    conn
  end
end
