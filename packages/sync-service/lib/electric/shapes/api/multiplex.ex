defmodule Electric.Shapes.Api.Multiplex do
  @moduledoc """
  Protocol helpers for Electric's internal live-request multiplexer.

  The WebSocket subprotocol is `electric.shape-multiplex.v1`. A client adds and
  removes logical live requests with JSON text frames:

      {"type":"watch","id":"request-1","handle":"...","offset":"12_0","cursor":"123","shape":{"table":"items"}}
      {"type":"watch","id":"request-2","handle":"...","offset":"12_0","cursor":"123","shape":{"table":"items"},"timeout_ms":5000}
      {"type":"unwatch","id":"request-1"}

  `cursor` is the unwrapped value of the previous `electric-cursor` response
  header. It may be `null` for a request without a previous cursor.

  `shape` holds the shape-definition query parameters (`table`, `where`,
  `columns`, `replica`, ...) of the HTTP request that would serve the handle.
  A watch is armed only when the handle is the shape of that definition;
  otherwise it is answered with a non-retryable `shape_mismatch` error before
  its head is read, so a handle alone never tells whether its shape changed.

  `timeout_ms` is optional and bounds the wait below the configured long-poll
  timeout, so a proxy that re-registers a wait can keep the total wait of its
  client within one long-poll period.

  An accepted watch receives an optional `ready` frame, followed by exactly one
  terminal `wake` or `no_change` frame unless the client unwatches it. Wake
  frames never contain shape data; the caller fetches that data through the
  normal shape HTTP endpoint. A `no_change` frame contains the status, headers,
  and JSON body needed to reproduce the normal empty live HTTP response.
  """

  alias Electric.Postgres.Lsn
  alias Electric.Replication.LogOffset
  alias Electric.Shapes.Api
  alias Electric.Shapes.Api.Params
  alias Electric.Shapes.Api.Response
  alias Electric.Shapes.Shape

  @protocol "electric.shape-multiplex.v1"

  @type watch_id :: binary()
  @type wake_reason :: :changes | :rotation
  @type server_frame ::
          %{required(:type) => binary(), required(:id) => watch_id()}
          | %{
              required(:type) => binary(),
              required(:code) => binary(),
              required(:message) => binary(),
              required(:retryable) => boolean(),
              optional(:id) => watch_id()
            }

  @spec protocol() :: binary()
  def protocol, do: @protocol

  @doc false
  def available?(%Api{} = api, source, source_opts, availability_guard) do
    source.active?(api, source_opts) and guard_available?(availability_guard)
  rescue
    _ -> false
  catch
    _, _ -> false
  end

  @spec ready_frame(watch_id()) :: server_frame()
  def ready_frame(id), do: %{type: "ready", id: id}

  @spec wake_frame(watch_id(), wake_reason()) :: server_frame()
  def wake_frame(id, reason) when reason in [:changes, :rotation] do
    %{type: "wake", id: id, reason: Atom.to_string(reason)}
  end

  @spec error_frame(nil | watch_id(), binary(), binary(), boolean()) :: server_frame()
  def error_frame(id, code, message, retryable) do
    %{type: "error", code: code, message: message, retryable: retryable}
    |> maybe_put_id(id)
  end

  @doc """
  Builds the transport-neutral equivalent of an empty live shape response.

  Header generation deliberately goes through `Api.Response` so cache policy,
  ETag format, cursor advancement, and source-position headers remain identical
  to the HTTP path, which serves a shape at the source positions it was created
  with.
  """
  @spec no_change_frame(
          watch_id(),
          Api.t(),
          Electric.shape_handle(),
          LogOffset.t(),
          binary() | nil,
          Shape.t() | nil
        ) :: server_frame()
  def no_change_frame(id, %Api{} = api, handle, %LogOffset{} = offset, cursor, shape_definition) do
    global_last_seen_lsn =
      case Electric.LsnTracker.get_last_processed_lsn(api.stack_id) do
        nil -> offset.tx_offset
        lsn -> Lsn.to_integer(lsn)
      end

    body = [
      %{
        headers: %{
          control: "up-to-date",
          global_last_seen_lsn: to_string(global_last_seen_lsn)
        }
      }
    ]

    params = %Params{
      handle: handle,
      offset: offset,
      live: true,
      source_positions: source_positions(shape_definition)
    }

    response = %Response{
      api: api,
      handle: handle,
      offset: offset,
      params: params,
      shape_definition: shape_definition,
      status: 200,
      up_to_date: true,
      no_changes: true,
      body: body,
      finalized?: true
    }

    headers = Response.client_headers(response, %{"cursor" => cursor})

    %{
      type: "no_change",
      id: id,
      response: %{status: 200, headers: headers, body: body}
    }
  end

  defp source_positions(%Shape{source_positions: source_positions}), do: source_positions
  defp source_positions(nil), do: :legacy

  defp maybe_put_id(frame, nil), do: frame
  defp maybe_put_id(frame, id), do: Map.put(frame, :id, id)

  defp guard_available?(nil), do: true
  defp guard_available?(guard) when is_function(guard, 0), do: guard.() == :ok
end
