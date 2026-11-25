defmodule Electric.Plug.Utils do
  @moduledoc """
  Utility functions for Electric endpoints, e.g. for parsing and validating
  path and query parameters.
  """

  alias Electric.Telemetry.RequestValues

  @redacted_query_value "[REDACTED]"
  @sensitive_query_param_names MapSet.new(
                                 ~w(secret api_secret token access_token api_key password)
                               )

  @redacted_header_value "[REDACTED]"
  @sensitive_header_names MapSet.new(
                            ~w(authorization proxy-authorization cookie set-cookie x-api-key x-auth-token x-electric-secret electric-secret)
                          )

  @doc """
  Parse columns parameter from a string consisting of a comma separated list
  of potentially quoted column names into a sorted list of strings.

  ## Examples
      iex> Electric.Plug.Utils.parse_columns_param("")
      {:error, "Invalid zero-length delimited identifier"}
      iex> Electric.Plug.Utils.parse_columns_param("foo,")
      {:error, "Invalid zero-length delimited identifier"}
      iex> Electric.Plug.Utils.parse_columns_param("id")
      {:ok, ["id"]}
      iex> Electric.Plug.Utils.parse_columns_param("id,name")
      {:ok, ["id", "name"]}
      iex> Electric.Plug.Utils.parse_columns_param(~S|"PoT@To",PoTaTo|)
      {:ok, ["PoT@To", "potato"]}
      iex> Electric.Plug.Utils.parse_columns_param(~S|"PoTaTo,sunday",foo|)
      {:ok, ["PoTaTo,sunday", "foo"]}
      iex> Electric.Plug.Utils.parse_columns_param(~S|"fo""o",bar|)
      {:ok, [~S|fo"o|, "bar"]}
      iex> Electric.Plug.Utils.parse_columns_param(~S|"id,"name"|)
      {:error, ~S|Invalid unquoted identifier contains special characters: "id|}
  """
  @spec parse_columns_param(binary()) :: {:ok, [String.t(), ...]} | {:error, term()}

  def parse_columns_param(columns) when is_binary(columns) do
    columns
    |> split_on_unquoted_commas()
    |> Enum.reduce_while([], fn column, acc ->
      case Electric.Postgres.Identifiers.parse(column) do
        {:ok, casted_column} -> {:cont, [casted_column | acc]}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn result ->
      case result do
        # TODO: convert output to MapSet?
        parsed_cols when is_list(parsed_cols) -> {:ok, Enum.reverse(parsed_cols)}
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  def parse_columns_param([col | _] = columns) when is_binary(col) do
    {:ok, columns}
  end

  # Split on commas outside double-quoted identifiers in one linear pass. The prior regex ran a
  # per-comma lookahead to end-of-string (quadratic: seconds of CPU on a large body). A comma
  # delimits when an even number of quotes follows it -- a comma inside a quoted identifier is
  # literal.
  defp split_on_unquoted_commas(columns) do
    split_fields(columns, count_quotes(columns, 0), 0, [], [])
  end

  defp count_quotes(<<>>, count), do: count
  defp count_quotes(<<?", rest::binary>>, count), do: count_quotes(rest, count + 1)
  defp count_quotes(<<_byte, rest::binary>>, count), do: count_quotes(rest, count)

  defp split_fields(<<>>, _total, _seen, field, fields),
    do: Enum.reverse([finish_field(field) | fields])

  defp split_fields(<<?", rest::binary>>, total, seen, field, fields),
    do: split_fields(rest, total, seen + 1, [?" | field], fields)

  defp split_fields(<<?,, rest::binary>>, total, seen, field, fields) do
    if rem(seen, 2) == rem(total, 2),
      do: split_fields(rest, total, seen, [], [finish_field(field) | fields]),
      else: split_fields(rest, total, seen, [?, | field], fields)
  end

  defp split_fields(<<byte, rest::binary>>, total, seen, field, fields),
    do: split_fields(rest, total, seen, [byte | field], fields)

  defp finish_field(field), do: field |> Enum.reverse() |> IO.iodata_to_binary()

  # Plug keeps the last value of a repeated query parameter and matches names exactly. A proxy
  # in front may read the first value (`URLSearchParams.get/1`), match undecoded names regardless
  # of case (nginx's `$arg_*`), or read names as PHP does, then forward the query string
  # unchanged. The two would disagree on the shape requested, so each parameter that defines it
  # must appear once, spelled exactly, in one form. `subset__where` and `subset[where]` are the
  # same parameter to the service, and any `subset[...]` key drops every `subset__` one.
  @single_valued_shape_params ~w(table where params columns replica log queryable_columns
                                 locale localized_columns subset)
  @ambiguous_shape_param_error "can't be repeated or spelled another way"

  @doc """
  Answers 400 and halts when the query string repeats a parameter that defines the shape, or
  spells one in a form only a lenient parser reads as it. `also` names a route's own parameters
  that pick its target, such as the `handle` of a delete.
  """
  @spec reject_ambiguous_shape_params(Plug.Conn.t(), Electric.Shapes.Api.t(), [String.t()]) ::
          Plug.Conn.t()
  def reject_ambiguous_shape_params(%Plug.Conn{} = conn, api, also \\ []) do
    case ambiguous_shape_params(conn.query_string, @single_valued_shape_params ++ also) do
      [] ->
        conn

      params ->
        errors = Map.new(params, &{&1, [@ambiguous_shape_param_error]})
        response = Electric.Shapes.Api.Response.invalid_request(api, errors: errors)

        conn
        |> Electric.Shapes.Api.Response.send(response)
        |> Plug.Conn.halt()
    end
  end

  # Splits and decodes names as `Plug.Conn.Query.decode/1` does, so `t%61ble` is `table`.
  # `params[1]` given twice is a repeat, and so are `params` and `params[1]`; `params[1]` and
  # `params[2]` are not.
  defp ambiguous_shape_params(query_string, single_valued) do
    query_string
    |> :binary.split("&", [:global])
    |> Enum.reject(&(&1 == ""))
    |> Enum.map(&read_param_name/1)
    |> Enum.filter(fn {path, _, _} -> query_param_root(path) in single_valued end)
    |> Enum.group_by(fn {path, _, _} -> query_param_root(path) end)
    |> Enum.filter(fn {_root, names} -> ambiguous_param?(names) end)
    |> Enum.map(&elem(&1, 0))
  end

  defp read_param_name(pair) do
    raw = pair |> :binary.split("=") |> hd()
    name = URI.decode_www_form(raw)
    lenient = lenient_param_name(name)
    # nginx compares undecoded names, so only the brackets of `params[1]` may be escaped.
    unescaped? = String.replace(raw, ~w(%5B %5b %5D %5d), &URI.decode/1) == name

    {canonical_param_path(lenient), lenient == name and unescaped?,
     String.starts_with?(lenient, "subset__")}
  end

  # A name as a lenient reader may see it: regardless of case (nginx), and as PHP reads it,
  # without leading spaces, cut at a NUL byte, with `.`, spaces and an unclosed `[` as `_`,
  # and without what follows its last closed `[...]` (`params[1]x` is `params[1]`).
  defp lenient_param_name(name) do
    [name | _] =
      name |> String.trim_leading(" ") |> String.downcase(:ascii) |> :binary.split(<<0>>)

    case :binary.split(name, "[") do
      [head, rest] ->
        if String.contains?(rest, "]"),
          do: String.replace(head, [".", " "], "_") <> php_indices("[" <> rest),
          else: String.replace(name, [".", " ", "["], "_")

      [_] ->
        String.replace(name, [".", " "], "_")
    end
  end

  defp php_indices("[" <> rest) do
    case :binary.split(rest, "]") do
      [index, tail] -> "[" <> index <> "]" <> php_indices(tail)
      [_] -> ""
    end
  end

  defp php_indices(_), do: ""

  # One path a prefix of another is two values for one parameter. Sorted, such a pair is
  # adjacent: anything between a path and one it prefixes starts with it too.
  defp ambiguous_param?(names) do
    paths = names |> Enum.map(fn {path, _, _} -> path end) |> Enum.sort()
    forms = names |> Enum.map(fn {_, _, dunder?} -> dunder? end) |> Enum.uniq()

    Enum.any?(names, fn {_, exact?, _} -> not exact? end) or length(forms) > 1 or
      Enum.any?(Enum.chunk_every(paths, 2, 1, :discard), fn [a, b] ->
        String.starts_with?(b, a)
      end)
  end

  # `subset__params[1]` reaches the service as `subset[params][1]`.
  defp canonical_param_path("subset__" <> rest) do
    root = query_param_root(rest)
    nested = binary_part(rest, byte_size(root), byte_size(rest) - byte_size(root))
    "subset[#{root}]#{nested}"
  end

  defp canonical_param_path(name), do: name

  # The name Plug stores a value under: `params` for `params[1]`.
  @doc false
  def query_param_root(name) do
    with true <- byte_size(name) > 0,
         ?] <- :binary.last(name),
         {pos, 1} when pos > 0 <- :binary.match(name, "[") do
      binary_part(name, 0, pos)
    else
      _ -> name
    end
  end

  @doc """
  Calculate the next interval that should be used for long polling based on the
  current time and previous interval used.

  Timestamp returned is in seconds and uses a custom epoch of 9th of October 2024, UTC.
  """
  @oct9th2024 ~U[2024-10-09 00:00:00Z]
  @spec get_next_interval_timestamp(integer(), binary() | nil) :: integer()
  def get_next_interval_timestamp(long_poll_timeout_ms, prev_interval \\ nil)

  # A sub-second timeout leaves no whole-second bucket to derive an interval from, but the cursor still
  # has to move: a client that is handed the value it just sent reads the response as a replay of the
  # one before it. Returning a constant here made every response carry the same cursor, so the property
  # the clause below exists to guarantee disappeared on a config change rather than on a code change.
  def get_next_interval_timestamp(long_poll_timeout_ms, prev_interval)
      when div(long_poll_timeout_ms, 1000) == 0,
      do: parse_prev_interval(prev_interval) + Enum.random(1..3_600)

  def get_next_interval_timestamp(long_poll_timeout_ms, prev_interval) do
    long_poll_timeout_sec = div(long_poll_timeout_ms, 1000)
    diff_in_seconds = DateTime.diff(DateTime.utc_now(), @oct9th2024, :second)
    next_interval = ceil(diff_in_seconds / long_poll_timeout_sec) * long_poll_timeout_sec
    prev_int = parse_prev_interval(prev_interval)

    if next_interval > prev_int do
      next_interval
    else
      prev_int + Enum.random(1..3_600)
    end
  end

  defp parse_prev_interval(nil), do: 0

  defp parse_prev_interval(prev_interval) when is_binary(prev_interval) do
    case Integer.parse(prev_interval) do
      {n, ""} -> n
      _ -> 0
    end
  end

  alias OpenTelemetry.SemConv, as: SC
  @max_telemetry_string_graphemes 2000
  @telemetry_body_subset_keys ~w(where order_by limit offset)
  @telemetry_body_subset_params_key "params"

  # Request headers are attacker-controlled and unbounded, so an unauthenticated request could mint one
  # span attribute per header and evict real attributes past the OTEL per-span cap. Only these names
  # become attributes; response headers stay unfiltered because the server, not the caller, sets them.
  @telemetry_request_header_names MapSet.new(
                                    ~w(accept accept-encoding accept-language cache-control connection
                                       content-length content-type if-none-match origin range referer
                                       user-agent x-forwarded-for x-forwarded-host x-forwarded-proto
                                       x-request-id)
                                  )

  # Query-param names are attacker-controlled and unbounded, exactly like request headers: without a
  # bound, one unauthenticated request could mint a span attribute per param and evict real attributes
  # past the OTEL per-span cap. Only these names become attributes; sensitive values are still redacted
  # below. Match is case-sensitive because query params are.
  @telemetry_query_param_names MapSet.new(
                                 ~w(table offset handle live where columns queryable_columns
                                    shape_definition replica params experimental_compaction
                                    live_sse experimental_live_sse log cursor expired_handle
                                    cache-buster secret api_secret
                                    subset__where subset__order_by subset__limit subset__offset
                                    subset__params subset__where_expr subset__order_by_expr)
                               )

  @doc false
  def redact_query_string(nil), do: nil
  def redact_query_string(""), do: ""

  def redact_query_string(query_string) when is_binary(query_string) do
    query_string
    |> String.split("&", trim: false)
    |> Enum.map_join("&", &redact_query_segment/1)
  end

  def common_open_telemetry_attrs(%Plug.Conn{assigns: assigns} = conn) do
    redact_values? = RequestValues.enabled?()

    {sanitized_query_string, query_string_attrs} =
      if redact_values? do
        query = if conn.query_string != "", do: conn.query_string
        {nil, RequestValues.summary("http.query_string", query)}
      else
        query = redact_query_string(conn.query_string)
        {query, %{"http.query_string" => query}}
      end

    query_params_map =
      if is_struct(conn.query_params, Plug.Conn.Unfetched) do
        %{}
      else
        conn.query_params
        |> Enum.reduce(%{}, fn {k, v}, attrs ->
          cond do
            redact_values? and k == "subset" ->
              Map.merge(attrs, RequestValues.summary("http.query_param.subset", v))

            telemetry_query_param?(k) ->
              Map.merge(attrs, query_param_attrs(k, v, redact_values?))

            true ->
              attrs
          end
        end)
      end

    body_params_map =
      assigns
      |> Map.get(:body_params, %{})
      |> telemetry_body_params(redact_values?)

    %{
      "error.type" => assigns[:error_str],
      "http.request_id" => assigns[:plug_request_id],
      SC.ClientAttributes.client_address() => conn.remote_ip,
      SC.ServerAttributes.server_address() => conn.host,
      SC.ServerAttributes.server_port() => conn.port,
      SC.HTTPAttributes.http_request_method() => conn.method,
      SC.HTTPAttributes.http_response_status_code() => conn.status,
      SC.Incubating.HTTPAttributes.http_response_size() => assigns[:streaming_bytes_sent],
      SC.NetworkAttributes.network_transport() => :tcp,
      SC.NetworkAttributes.network_local_port() => conn.port,
      SC.UserAgentAttributes.user_agent_original() => user_agent(conn),
      SC.Incubating.URLAttributes.url_path() => conn.request_path,
      SC.URLAttributes.url_scheme() => conn.scheme,
      SC.URLAttributes.url_full() =>
        %URI{
          scheme: to_string(conn.scheme),
          host: conn.host,
          port: conn.port,
          path: conn.request_path,
          query: sanitized_query_string
        }
        |> to_string()
    }
    |> Map.merge(query_string_attrs)
    |> Map.filter(fn {_k, v} -> not is_nil(v) end)
    |> Map.merge(Map.get(conn.private, :telemetry_span_attrs, %{}))
    |> Map.merge(query_params_map)
    |> Map.merge(body_params_map)
    |> Map.merge(request_header_attrs(conn.req_headers, redact_values?))
    |> Map.merge(header_attrs("http.response.header.", conn.resp_headers))
  end

  defp query_param_attrs(key, value, redact_values?) do
    prefix = "http.query_param.#{key}"

    if redact_values? and
         (key in ~w(where params shape_definition) or String.starts_with?(key, "subset__")) do
      RequestValues.summary(prefix, value)
    else
      %{prefix => redact_query_param_value(key, value)}
    end
  end

  defp telemetry_body_params(params, true) when is_map(params) do
    case Map.get(params, "subset", params) do
      subset when is_map(subset) ->
        Enum.reduce([@telemetry_body_subset_params_key | @telemetry_body_subset_keys], %{}, fn
          key, attrs ->
            case Map.fetch(subset, key) do
              {:ok, value} ->
                Map.merge(attrs, RequestValues.summary("http.body_param.subset.#{key}", value))

              :error ->
                attrs
            end
        end)

      _ ->
        %{}
    end
  end

  defp telemetry_body_params(params, false) when is_map(params) do
    Enum.reduce(@telemetry_body_subset_keys, %{}, fn key, attrs ->
      prefix = "http.body_param.subset.#{key}"

      case Map.fetch(params, key) do
        {:ok, value} ->
          Map.merge(attrs, body_param_scalar_attr(value, prefix))

        :error ->
          attrs
      end
    end)
    |> Map.merge(subset_params_telemetry_attr(params))
  end

  defp telemetry_body_params(_, _), do: %{}

  defp subset_params_telemetry_attr(params) do
    case Map.fetch(params, @telemetry_body_subset_params_key) do
      {:ok, value} ->
        %{
          "http.body_param.subset.params" =>
            value
            |> inspect()
            |> truncate_telemetry_string()
        }

      :error ->
        %{}
    end
  end

  defp body_param_scalar_attr(nil, _prefix), do: %{}

  defp body_param_scalar_attr(value, prefix) do
    case scalar_attr_value(value) do
      {:ok, telemetry_value} -> %{prefix => telemetry_value}
      :skip -> %{}
    end
  end

  defp scalar_attr_value(value) when is_binary(value) do
    {:ok, truncate_telemetry_string(value)}
  end

  defp scalar_attr_value(value) when is_boolean(value) or is_number(value), do: {:ok, value}
  defp scalar_attr_value(_value), do: :skip

  defp truncate_telemetry_string(value) do
    value
    |> trim_invalid_utf8()
    |> String.slice(0, @max_telemetry_string_graphemes)
  end

  defp trim_invalid_utf8(value) do
    if String.valid?(value) do
      value
    else
      value
      |> String.chunk(:valid)
      |> Enum.filter(&String.valid?/1)
      |> Enum.join()
    end
  end

  defp user_agent(%Plug.Conn{} = conn) do
    case Plug.Conn.get_req_header(conn, "user-agent") do
      [] -> ""
      [head | _] -> head
    end
  end

  defp redact_query_segment(segment) do
    case String.split(segment, "=", parts: 2) do
      [key, _value] ->
        if sensitive_query_param?(decode_query_key(key)) do
          key <> "=" <> @redacted_query_value
        else
          segment
        end

      [_key] ->
        segment
    end
  end

  defp redact_query_param_value(key, value) do
    if sensitive_query_param?(key), do: @redacted_query_value, else: value
  end

  defp request_header_attrs(headers, redact_values?) do
    header_attrs(
      "http.request.header.",
      Enum.filter(headers, fn {name, _value} ->
        telemetry_request_header?(name) and
          not (redact_values? and RequestValues.url_header?(name))
      end)
    )
  end

  defp telemetry_request_header?(name) when is_binary(name) do
    MapSet.member?(@telemetry_request_header_names, String.downcase(name))
  end

  defp telemetry_query_param?(name) when is_binary(name) do
    MapSet.member?(@telemetry_query_param_names, name)
  end

  defp header_attrs(prefix, headers) do
    Map.new(headers, fn {name, value} -> {prefix <> name, redact_header_value(name, value)} end)
  end

  defp redact_header_value(name, value) do
    if sensitive_header?(name), do: @redacted_header_value, else: value
  end

  defp sensitive_header?(name) when is_binary(name) do
    MapSet.member?(@sensitive_header_names, String.downcase(name))
  end

  defp sensitive_query_param?(key) when is_binary(key) do
    MapSet.member?(@sensitive_query_param_names, String.downcase(key))
  end

  defp decode_query_key(key) do
    URI.decode_www_form(key)
  rescue
    ArgumentError -> key
  end

  defmodule CORSHeaderPlug do
    @behaviour Plug
    import Plug.Conn
    def init(opts), do: opts

    # A cacheable response (`cache-control: public`) carries per-principal rows whenever a deployment
    # authenticates in a header rather than in the URL, so the credential has to be part of the
    # shared-cache key or one principal is served another's cached rows (CWE-524). `Vary` is that key:
    # a request sending none of these headers still collapses to a single variant, so a URL-authenticated
    # deployment keeps its hit rate while a header-authenticated one gets one variant per credential.
    @vary_request_headers ~w(origin authorization cookie x-api-key x-auth-token x-electric-secret electric-secret)
    @vary_header Enum.join(@vary_request_headers, ", ")

    def call(conn, opts),
      do:
        conn
        |> put_resp_header("access-control-allow-origin", get_allowed_origin(conn, opts))
        |> put_resp_header("vary", @vary_header)
        |> put_resp_header("access-control-expose-headers", headers_to_expose())
        |> put_resp_header("access-control-allow-methods", get_allowed_methods(conn, opts))

    defp get_allowed_methods(_conn, opts), do: Access.get(opts, :methods, []) |> Enum.join(", ")

    defp get_allowed_origin(conn, opts) do
      Access.get(opts, :origin, request_origin(conn))
    end

    # A repeated `Origin` is a malformed request, not a second origin to honour, and the unmatched
    # list crashed the plug before authentication ran. `*` is what a request sending no origin at all
    # already receives, so a sender gains nothing by repeating the header.
    defp request_origin(conn) do
      case Plug.Conn.get_req_header(conn, "origin") do
        [origin] -> origin
        _ -> "*"
      end
    end

    defp headers_to_expose do
      Enum.join(Electric.Shapes.Api.Response.electric_headers(), ",")
    end
  end

  defmodule PassAssignToOptsPlug do
    @behaviour Plug
    def init(plug: plug, assign_key: key) when is_atom(plug), do: {plug, key}
    def call(conn, {plug, key}), do: plug.call(conn, plug.init(conn.assigns[key]))
  end
end
