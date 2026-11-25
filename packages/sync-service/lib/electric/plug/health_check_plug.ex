defmodule Electric.Plug.HealthCheckPlug do
  use Plug.Builder

  alias Plug.Conn
  alias Electric.StatusMonitor

  plug :fetch_query_params
  plug :check_service_status
  plug :put_resp_content_type, "application/json"
  plug :put_cache_headers
  plug :send_response

  # Match service status to a status code and status message,
  # keeping the message name decoupled from the internal representation
  # of the status to ensure the API is stable
  defp check_service_status(%Conn{assigns: %{config: config}} = conn, _) do
    stack_id = config[:stack_id]

    {status_code, status_text} =
      case StatusMonitor.service_status(stack_id) do
        :active -> {200, "active"}
        :waiting -> {202, "waiting"}
        :starting -> {202, "starting"}
        # when Electric is in the scaled-down mode (all database connections are closed),
        # report its status as active because for any incoming shape request it will
        # transparently restore the connection subsystem before processing the request
        :sleeping -> {200, "active"}
      end

    # `?require=read_only`, for a readiness probe: 200 as soon as existing shapes are served,
    # which with a large shape count is minutes before the stack is active. The status text is kept.
    status_code =
      if status_code == 202 and conn.query_params["require"] == "read_only" and
           StatusMonitor.serves_existing_shapes?(stack_id),
         do: 200,
         else: status_code

    conn |> assign(:status_text, status_text) |> assign(:status_code, status_code)
  end

  defp put_cache_headers(conn, _) do
    put_resp_header(conn, "cache-control", "no-cache, no-store, must-revalidate")
  end

  defp send_response(%Conn{assigns: assigns} = conn, _) do
    send_resp(conn, assigns.status_code, Jason.encode!(%{status: assigns.status_text}))
  end
end
