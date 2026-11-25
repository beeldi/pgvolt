if Code.ensure_loaded?(Electric.Shapes.Api) do
  defmodule Electric.Client.Embedded do
    alias Electric.Client.Fetch
    alias Electric.Shapes.Api

    @behaviour Electric.Client.Fetch

    @impl Electric.Client.Fetch
    def validate_opts(opts) do
      case Keyword.fetch(opts, :api) do
        {:ok, %Api{}} ->
          {:ok, opts}

        :error ->
          {:error, "missing required api: %#{Api}{} setting"}
      end
    end

    @impl Electric.Client.Fetch
    def fetch(%Fetch.Request{method: :delete} = request, opts) do
      {:ok, api} = Keyword.fetch(opts, :api)

      timestamp = DateTime.utc_now()

      with {:ok, request} <- Api.validate_for_delete(api, request_to_params(request)),
           %Api.Response{} = response <- Api.delete_shape(request) do
        {:ok, translate_response(response, timestamp)}
      end
    end

    def fetch(%Fetch.Request{method: :get} = request, opts) do
      {:ok, api} = Keyword.fetch(opts, :api)

      timestamp = DateTime.utc_now()

      case Api.validate(api, request_to_params(request)) do
        {:ok, request} ->
          %Api.Response{} = response = Api.serve_shape_log(request)
          {:ok, translate_response(response, timestamp)}

        {:error, response} ->
          {:error, translate_response(response, timestamp)}
      end
    end

    defp translate_response(%Api.Response{} = response, timestamp) do
      %Fetch.Response{
        status: response.status,
        last_offset: convert_offset(response.offset),
        shape_handle: response.handle,
        schema: Api.schema(response),
        # Embedded mode has no CDN, so cursor isn't needed for cache busting,
        # but we set a dummy value to satisfy header validation.
        next_cursor: 0,
        request_timestamp: timestamp,
        # Ensure we enumerate the response body in the same process that made the
        # original api request so the cleanup shape process cleanup works
        body: response.body |> Enum.to_list() |> List.flatten()
      }
    end

    defp request_to_params(%Fetch.Request{} = request) do
      request.params
      |> nest_bracketed_keys()
      |> Map.merge(%{
        "offset" => to_string(request.offset),
        "handle" => request.shape_handle,
        "live" => request.live,
        "replica" => request.replica
      })
    end

    # Where-clause values arrive as `params[1]`, `params[2]`, ... Over HTTP, Plug nests them
    # into `%{"params" => %{"1" => ...}}` before `Electric.Shapes.Api` sees them; embedded
    # mode skips Plug, so nest them here, one level deep.
    defp nest_bracketed_keys(params) do
      Enum.reduce(params, %{}, fn {key, value}, acc ->
        case bracketed_key(key) do
          {outer, inner} ->
            Map.update(acc, outer, %{inner => value}, fn
              %{} = nested -> Map.put(nested, inner, value)
              _scalar -> %{inner => value}
            end)

          nil ->
            Map.put_new(acc, key, value)
        end
      end)
    end

    defp bracketed_key(key) when is_binary(key) do
      case Regex.run(~r/\A([^\[\]]+)\[([^\[\]]+)\]\z/, key) do
        [_, outer, inner] -> {outer, inner}
        nil -> nil
      end
    end

    defp bracketed_key(_key), do: nil

    defp convert_offset(nil) do
      nil
    end

    defp convert_offset(%Electric.Replication.LogOffset{} = server_offset) do
      to_string(server_offset)
    end
  end
end
