defmodule Electric.Plug.ReclaimShapeGenerationsPlug do
  @moduledoc """
  `POST /v1/admin/shape-generations/reclaim`: one page of `Electric.ShapeCache.GenerationReclaim`.
  The caller sends `next_after` back as `after` until it comes back `null`, which it also does when
  `outcome` is `aborted`. `removed` counts the matched shapes that left the store during the call,
  whether this call or the expiry manager took them out.
  """
  use Plug.Builder, copy_opts_to_assign: :config

  alias Electric.ShapeCache.GenerationReclaim
  alias Electric.StatusMonitor

  require Logger

  @max_budget_ms 120_000
  @max_cursor_bytes 256
  @max_prefix_bytes 64
  @valid_prefix_re ~r/\A[A-Za-z0-9:._\/-]+\z/

  plug(Plug.Parsers, parsers: [:json], json_decoder: Jason, length: 4_096)
  plug(:put_resp_content_type, "application/json")
  plug(:reclaim)

  defp reclaim(%Plug.Conn{assigns: %{config: config}} = conn, _opts) do
    stack_id = Access.fetch!(config, :stack_id)

    with {:ok, request} <- parse(conn.body_params),
         :ok <- require_active(stack_id),
         {:ok, result} <- read_and_remove(stack_id, request) do
      respond(conn, 200, result)
    else
      {:error, {:invalid, message}} -> respond(conn, 400, %{message: message})
      {:error, {:live_shapes, handles}} -> respond(conn, 409, live_shapes(handles))
      {:error, {:unavailable, message}} -> respond(conn, 503, %{message: message})
      {:error, reason} -> respond_unavailable(conn, reason)
    end
  end

  defp parse(
         %{
           "current_generation" => current,
           "newest_collectable_generation" => newest_collectable,
           "budget_ms" => budget_ms
         } = params
       )
       when is_integer(current) and is_integer(newest_collectable) and is_integer(budget_ms) do
    after_handle = Map.get(params, "after", "")
    token_prefix = Map.get(params, "token_prefix", GenerationReclaim.default_token_prefix())

    cond do
      newest_collectable < 1 or newest_collectable >= current ->
        {:error,
         {:invalid,
          "newest_collectable_generation must be at least 1 and below current_generation"}}

      budget_ms < 1 or budget_ms > @max_budget_ms ->
        {:error, {:invalid, "budget_ms must be between 1 and #{@max_budget_ms}"}}

      not is_binary(after_handle) or byte_size(after_handle) > @max_cursor_bytes ->
        {:error, {:invalid, "after must be a shape handle"}}

      not is_binary(token_prefix) or byte_size(token_prefix) == 0 or
        byte_size(token_prefix) > @max_prefix_bytes or
          not Regex.match?(@valid_prefix_re, token_prefix) ->
        {:error,
         {:invalid,
          "token_prefix must be 1 to #{@max_prefix_bytes} ASCII identifier characters (A-Z, a-z, 0-9, :, ., _, /, -)"}}

      true ->
        {:ok,
         %{
           current_generation: current,
           newest_collectable_generation: newest_collectable,
           budget_ms: budget_ms,
           after: after_handle,
           token_prefix: token_prefix
         }}
    end
  end

  defp parse(_params) do
    {:error,
     {:invalid,
      "current_generation, newest_collectable_generation and budget_ms are required integers"}}
  end

  # A stack still restoring has not read every shape back yet, and a removal racing the restore is
  # what the expiry manager waits out for the same reason.
  defp require_active(stack_id) do
    case StatusMonitor.status(stack_id) do
      %{shape: :up} -> :ok
      status -> {:error, {:unavailable, "stack is not active: #{inspect(status)}"}}
    end
  end

  # A page read that times out on the pool's checkout exits this request's process, which serves
  # nothing else, and a stack restarting under the read raises on its vanished tables: both are
  # answered as the store being busy. Nothing may escape to Bandit, whose error report carries the
  # request's headers, the bearer included.
  defp read_and_remove(stack_id, request) do
    GenerationReclaim.reclaim(stack_id, request)
  rescue
    error -> {:error, Exception.message(error)}
  catch
    :exit, reason -> {:error, reason}
  end

  defp respond_unavailable(conn, reason) do
    Logger.warning("Shape generation reclaim could not read the shape store: #{inspect(reason)}")
    respond(conn, 503, %{message: "the shape store could not be read"})
  end

  defp live_shapes(handles) do
    %{
      message: "the matcher selected shapes of the current generation or shapes others depend on",
      live_handles: handles
    }
  end

  defp respond(conn, status, body), do: send_resp(conn, status, Jason.encode!(body))
end
