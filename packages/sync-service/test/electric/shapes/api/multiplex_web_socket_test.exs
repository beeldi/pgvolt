defmodule Electric.Shapes.Api.Multiplex.WebSocketTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Electric.Plug.Router
  alias Electric.Plug.ShapeMultiplexPlug
  alias Electric.Replication.LogOffset
  alias Electric.Shapes.Api
  alias Electric.Shapes.Api.Multiplex
  alias Electric.Shapes.Api.Multiplex.WebSocket
  alias Electric.Shapes.Shape

  @items_shape %{"table" => "items", "where" => "value = 'a'"}

  defmodule Source do
    @behaviour Electric.Shapes.Api.Multiplex.Source

    @impl true
    def active?(_api, agent), do: Agent.get(agent, & &1.active?)

    @impl true
    def validate_shape(_api, handle, _offset, shape, agent) do
      {validation, definition} =
        Agent.get(agent, &{&1.validation, Map.get(&1.definitions, handle)})

      cond do
        validation == :raise -> raise "shape check failed"
        validation != :ok -> validation
        definition == shape -> :ok
        true -> :mismatch
      end
    end

    @impl true
    def lookup(_api, handle, agent) do
      Agent.get_and_update(agent, fn state ->
        result =
          case Map.fetch(state.heads, handle) do
            {:ok, offset} -> {:ok, offset}
            :error -> :not_found
          end

        {result, %{state | lookups: [handle | state.lookups]}}
      end)
    end

    @impl true
    def fetch_shape(_api, handle, agent) do
      case Agent.get(agent, &Map.fetch(&1.shapes, handle)) do
        {:ok, shape} -> {:ok, shape}
        :error -> :not_found
      end
    end

    @impl true
    def subscribe(_api, handle, ref, agent) do
      Agent.update(agent, fn state ->
        {new_head, advances} = Map.pop(state.advance_on_subscribe, handle)
        heads = if new_head, do: Map.put(state.heads, handle, new_head), else: state.heads

        %{
          state
          | advance_on_subscribe: advances,
            heads: heads,
            subscriptions: [{handle, ref} | state.subscriptions]
        }
      end)

      :ok
    end

    @impl true
    def unsubscribe(_api, handle, agent) do
      Agent.update(agent, fn state ->
        %{state | unsubscriptions: [handle | state.unsubscriptions]}
      end)

      :ok
    end
  end

  setup do
    stack_id = "multiplex-websocket-test"
    :ok = Electric.LsnTracker.initialize(stack_id)
    :ok = Electric.LsnTracker.set_last_processed_lsn(stack_id, 123)

    api = %Api{
      configured: true,
      stack_id: stack_id,
      long_poll_timeout: 20_000,
      send_cache_headers?: true
    }

    {:ok, source} =
      start_supervised(
        {Agent,
         fn ->
           %{
             active?: true,
             advance_on_subscribe: %{},
             definitions: %{"shape-1" => @items_shape},
             heads: %{"shape-1" => LogOffset.new(10, 0)},
             lookups: [],
             shapes: %{},
             subscriptions: [],
             unsubscriptions: [],
             validation: :ok
           }
         end}
      )

    %{api: api, source: source}
  end

  test "refuses a watch beyond the connection's limit with a retryable error", ctx do
    state = init_socket(ctx, %{multiplex_max_watches: 1})

    assert {:push, {:text, ready}, state} =
             WebSocket.handle_in(watch_frame("request-1", "10_0", "40"), state)

    assert %{"type" => "ready", "id" => "request-1"} = Jason.decode!(ready)

    assert {:push, {:text, refused}, state} =
             WebSocket.handle_in(watch_frame("request-2", "10_0", "40"), state)

    assert %{
             "type" => "error",
             "id" => "request-2",
             "code" => "too_many_watches",
             "retryable" => true
           } = Jason.decode!(refused)

    assert ["request-1"] == Map.keys(state.watches)
  end

  test "subscribe-then-head recheck wakes a watch that raced with registration", ctx do
    Agent.update(ctx.source, fn state ->
      %{state | advance_on_subscribe: %{"shape-1" => LogOffset.new(11, 0)}}
    end)

    state = init_socket(ctx)

    assert {:push, {:text, payload}, state} =
             WebSocket.handle_in(watch_frame("request-1", "10_0", "40"), state)

    assert %{"type" => "wake", "id" => "request-1", "reason" => "changes"} =
             Jason.decode!(payload)

    assert state.watches == %{}
    assert state.handles == %{}

    source_state = Agent.get(ctx.source, & &1)
    assert length(source_state.subscriptions) == 1
    assert length(source_state.lookups) == 2
    assert source_state.unsubscriptions == ["shape-1"]
  end

  test "coalesces shape subscriptions and wakes every changed watch once", ctx do
    state = init_socket(ctx)

    assert {:push, {:text, ready_1}, state} =
             WebSocket.handle_in(watch_frame("request-1", "10_0", nil), state)

    assert %{"type" => "ready", "id" => "request-1"} = Jason.decode!(ready_1)

    assert {:push, {:text, ready_2}, state} =
             WebSocket.handle_in(watch_frame("request-2", "10_0", "20"), state)

    assert %{"type" => "ready", "id" => "request-2"} = Jason.decode!(ready_2)

    source_state = Agent.get(ctx.source, & &1)
    assert length(source_state.subscriptions) == 1
    assert length(source_state.lookups) == 3

    [subscription] = Map.values(state.handles)

    assert {:push, messages, state} =
             WebSocket.handle_info(
               {subscription.ref, :new_changes, LogOffset.new(11, 0)},
               state
             )

    frames = decode_messages(messages)

    assert MapSet.new(frames) ==
             MapSet.new([
               %{"type" => "wake", "id" => "request-1", "reason" => "changes"},
               %{"type" => "wake", "id" => "request-2", "reason" => "changes"}
             ])

    assert state.watches == %{}
    assert state.handles == %{}
    assert Agent.get(ctx.source, & &1.unsubscriptions) == ["shape-1"]
  end

  test "deadline returns a normal no-change HTTP envelope and advances the cursor", ctx do
    state = init_socket(ctx)

    assert {:push, {:text, _ready}, state} =
             WebSocket.handle_in(watch_frame("request-1", "10_0", "999999999999"), state)

    cancel_timer(state.deadline_timer_ref)
    expired_at = System.monotonic_time(:millisecond) - 1
    watch = %{Map.fetch!(state.watches, "request-1") | deadline: expired_at}
    token = make_ref()

    state = %{
      state
      | watches: %{"request-1" => watch},
        deadlines: :gb_trees.insert({expired_at, "request-1"}, true, :gb_trees.empty()),
        deadline_timer_at: expired_at,
        deadline_timer_ref: nil,
        deadline_timer_token: token
    }

    assert {:push, messages, state} =
             WebSocket.handle_info({:multiplex_deadline, token}, state)

    assert [%{"type" => "no_change"} = frame] = decode_messages(messages)

    assert %{
             "type" => "no_change",
             "id" => "request-1",
             "response" => %{
               "status" => 200,
               "headers" => headers,
               "body" => [
                 %{
                   "headers" => %{
                     "control" => "up-to-date",
                     "global_last_seen_lsn" => "123"
                   }
                 }
               ]
             }
           } = frame

    assert headers["electric-handle"] == "shape-1"
    assert headers["electric-offset"] == "10_0"
    assert headers["electric-has-data"] == "false"
    assert headers["electric-up-to-date"] == ""
    assert headers["cache-control"] == "public, max-age=5, stale-while-revalidate=5"
    assert headers["content-type"] == "application/json; charset=utf-8"
    assert is_binary(headers["etag"])
    assert String.to_integer(headers["electric-cursor"]) > 999_999_999_999
    assert state.watches == %{}
    assert state.handles == %{}
    assert length(Agent.get(ctx.source, & &1.lookups)) == 2
  end

  test "deadline no-change carries the full-xid source headers of the watched shape", ctx do
    Agent.update(ctx.source, fn state ->
      shape = %Shape{
        root_table: {"public", "items"},
        root_table_id: 1,
        source_positions: :full_xid,
        source_generation: "generation-a"
      }

      %{state | shapes: %{"shape-1" => shape}}
    end)

    {state, token} = armed_socket_past_deadline(ctx)

    assert {:push, messages, _state} = WebSocket.handle_info({:multiplex_deadline, token}, state)

    assert [%{"type" => "no_change", "response" => %{"headers" => headers}}] =
             decode_messages(messages)

    assert headers["electric-source-positions"] == "mvcc-full-xid-v1"
    assert headers["electric-source-generation"] == "generation-a"
  end

  test "deadline no-change for a legacy or vanished shape carries no source headers", ctx do
    {state, token} = armed_socket_past_deadline(ctx)

    assert {:push, messages, _state} = WebSocket.handle_info({:multiplex_deadline, token}, state)

    assert [%{"type" => "no_change", "response" => %{"headers" => headers}}] =
             decode_messages(messages)

    refute Map.has_key?(headers, "electric-source-positions")
    refute Map.has_key?(headers, "electric-source-generation")
  end

  test "timeout_ms shortens the wait but never extends it past the long-poll timeout", ctx do
    state = init_socket(ctx)
    before_ms = System.monotonic_time(:millisecond)

    assert {:push, {:text, _ready}, state} =
             WebSocket.handle_in(timed_watch_frame("request-1", 5_000), state)

    assert {:push, {:text, _ready}, state} =
             WebSocket.handle_in(timed_watch_frame("request-2", 60_000), state)

    after_ms = System.monotonic_time(:millisecond)
    short = Map.fetch!(state.watches, "request-1")
    capped = Map.fetch!(state.watches, "request-2")

    assert short.deadline in (before_ms + 5_000)..(after_ms + 5_000)

    assert capped.deadline in (before_ms + ctx.api.long_poll_timeout)..(after_ms +
                                                                          ctx.api.long_poll_timeout)

    assert state.deadline_timer_at == short.deadline
  end

  test "rejects a timeout_ms that is not a non-negative integer", ctx do
    state = init_socket(ctx)

    for timeout <- [-1, 1.5, "5000"] do
      assert {:push, {:text, payload}, _state} =
               WebSocket.handle_in(timed_watch_frame("request-1", timeout), state)

      assert %{"type" => "error", "id" => "request-1", "code" => "invalid_frame"} =
               Jason.decode!(payload)
    end
  end

  test "shape rotation wakes all watches without returning data", ctx do
    state = init_socket(ctx)

    assert {:push, {:text, _ready}, state} =
             WebSocket.handle_in(watch_frame("request-1", "10_0", nil), state)

    [subscription] = Map.values(state.handles)

    assert {:push, messages, state} =
             WebSocket.handle_info({subscription.ref, :shape_rotation}, state)

    assert [%{"type" => "wake", "id" => "request-1", "reason" => "rotation"}] =
             decode_messages(messages)

    assert state.watches == %{}
    assert state.handles == %{}
  end

  test "a regressed head during registration wakes as a rotation", ctx do
    Agent.update(ctx.source, fn state ->
      %{state | advance_on_subscribe: %{"shape-1" => LogOffset.new(9, 0)}}
    end)

    state = init_socket(ctx)

    assert {:push, {:text, payload}, state} =
             WebSocket.handle_in(watch_frame("request-1", "10_0", nil), state)

    assert %{"type" => "wake", "id" => "request-1", "reason" => "rotation"} =
             Jason.decode!(payload)

    assert state.watches == %{}
    assert state.handles == %{}
  end

  test "a requested offset ahead of the observed head wakes as a rotation", ctx do
    state = init_socket(ctx)

    assert {:push, {:text, payload}, state} =
             WebSocket.handle_in(watch_frame("request-1", "11_0", nil), state)

    assert %{"type" => "wake", "id" => "request-1", "reason" => "rotation"} =
             Jason.decode!(payload)

    assert state.watches == %{}
    assert state.handles == %{}
    assert Agent.get(ctx.source, & &1.subscriptions) == []
  end

  test "unwatch is idempotent and removes the underlying subscription", ctx do
    state = init_socket(ctx)

    assert {:push, {:text, _ready}, state} =
             WebSocket.handle_in(watch_frame("request-1", "10_0", nil), state)

    assert {:ok, state} =
             WebSocket.handle_in(
               {Jason.encode!(%{type: "unwatch", id: "request-1"}), opcode: :text},
               state
             )

    assert {:ok, state} =
             WebSocket.handle_in(
               {Jason.encode!(%{type: "unwatch", id: "request-1"}), opcode: :text},
               state
             )

    assert state.watches == %{}
    assert Agent.get(ctx.source, & &1.unsubscriptions) == ["shape-1"]
  end

  test "arms the canonical 0_inf quiet-shape offset", ctx do
    Agent.update(ctx.source, fn state ->
      %{state | heads: %{"shape-1" => LogOffset.last_before_real_offsets()}}
    end)

    state = init_socket(ctx)

    assert {:push, {:text, payload}, state} =
             WebSocket.handle_in(watch_frame("request-1", "0_inf", nil), state)

    assert %{"type" => "ready", "id" => "request-1"} = Jason.decode!(payload)
    assert Map.has_key?(state.watches, "request-1")
  end

  test "rejects other special and missing cursor watch values", ctx do
    state = init_socket(ctx)

    assert {:push, {:text, payload}, state} =
             WebSocket.handle_in(watch_frame("request-1", "1_inf", nil), state)

    assert %{"type" => "error", "code" => "invalid_offset", "retryable" => false} =
             Jason.decode!(payload)

    frame = %{
      type: "watch",
      id: "request-2",
      handle: "shape-1",
      offset: "10_0",
      shape: @items_shape
    }

    assert {:push, {:text, payload}, _state} =
             WebSocket.handle_in({Jason.encode!(frame), opcode: :text}, state)

    assert %{"type" => "error", "code" => "invalid_frame", "retryable" => false} =
             Jason.decode!(payload)
  end

  test "refuses a watch whose handle is not the shape of its definition before reading its head",
       ctx do
    state = init_socket(ctx)
    other_shape = %{"table" => "items", "where" => "value = 'b'"}

    for {id, offset} <- [{"behind", "9_0"}, {"at-head", "10_0"}, {"ahead", "11_0"}] do
      frame = %{
        type: "watch",
        id: id,
        handle: "shape-1",
        offset: offset,
        cursor: nil,
        shape: other_shape
      }

      assert {:push, {:text, payload}, state} =
               WebSocket.handle_in({Jason.encode!(frame), opcode: :text}, state)

      assert %{"type" => "error", "id" => ^id, "code" => "shape_mismatch", "retryable" => false} =
               Jason.decode!(payload)

      assert state.watches == %{}
    end

    source_state = Agent.get(ctx.source, & &1)
    assert source_state.lookups == []
    assert source_state.subscriptions == []
  end

  test "refuses a watch that carries no shape definition", ctx do
    state = init_socket(ctx)

    for shape <- [nil, "items"] do
      frame = %{type: "watch", id: "request-1", handle: "shape-1", offset: "9_0", cursor: nil}
      frame = if shape, do: Map.put(frame, :shape, shape), else: frame

      assert {:push, {:text, payload}, _state} =
               WebSocket.handle_in({Jason.encode!(frame), opcode: :text}, state)

      assert %{
               "type" => "error",
               "id" => "request-1",
               "code" => "invalid_frame",
               "retryable" => false
             } = Jason.decode!(payload)
    end

    assert Agent.get(ctx.source, & &1.lookups) == []
  end

  test "refuses a definition it cannot check, and asks for a retry only when the check failed",
       ctx do
    state = init_socket(ctx)

    for {validation, code, retryable} <- [
          {:invalid, "invalid_shape", false},
          {:unavailable, "shape_unavailable", true},
          {:raise, "shape_unavailable", true}
        ] do
      Agent.update(ctx.source, &%{&1 | validation: validation})

      assert {:push, {:text, payload}, state} =
               WebSocket.handle_in(watch_frame("request-1", "9_0", nil), state)

      assert %{"type" => "error", "id" => "request-1", "code" => ^code, "retryable" => ^retryable} =
               Jason.decode!(payload)

      assert state.watches == %{}
    end

    assert Agent.get(ctx.source, & &1.lookups) == []
  end

  test "closes an established socket when active ownership is lost", ctx do
    state = init_socket(ctx)
    Agent.update(ctx.source, &%{&1 | active?: false})

    assert {:stop, {:shutdown, :restart}, {1012, _reason}, [{:text, payload}], _state} =
             WebSocket.handle_info(:multiplex_check_availability, state)

    assert %{"type" => "error", "code" => "inactive_instance", "retryable" => true} =
             Jason.decode!(payload)
  end

  describe "ShapeMultiplexPlug" do
    test "upgrades with the selected subprotocol", ctx do
      conn = websocket_conn() |> ShapeMultiplexPlug.call(plug_opts(ctx))

      assert conn.state == :upgraded
      assert get_resp_header(conn, "sec-websocket-protocol") == [Multiplex.protocol()]
    end

    test "checks the embedding availability guard before upgrade", ctx do
      opts = Keyword.put(plug_opts(ctx), :availability_guard, fn -> {:error, :not_owner} end)
      conn = websocket_conn() |> ShapeMultiplexPlug.call(opts)

      assert conn.status == 503

      assert %{"code" => "inactive_instance", "retryable" => true} =
               Jason.decode!(conn.resp_body)
    end

    test "requires the versioned subprotocol", ctx do
      conn =
        conn(:get, "/v1/shape/multiplex")
        |> with_host_header()
        |> put_req_header("connection", "upgrade")
        |> put_req_header("upgrade", "websocket")
        |> put_req_header("sec-websocket-key", Base.encode64(:crypto.strong_rand_bytes(16)))
        |> put_req_header("sec-websocket-version", "13")
        |> ShapeMultiplexPlug.call(plug_opts(ctx))

      assert conn.status == 400
      assert %{"code" => "unsupported_subprotocol"} = Jason.decode!(conn.resp_body)
    end
  end

  test "standalone router authenticates the multiplex endpoint" do
    opts = Router.init(secret: "source-secret", stack_id: "router-auth-test")

    conn =
      conn(:get, "/v1/shape/multiplex")
      |> Router.call(opts)

    assert conn.status == 401
  end

  defp init_socket(ctx, opts \\ %{}) do
    assert {:ok, state} =
             WebSocket.init(
               Map.merge(
                 %{
                   api: ctx.api,
                   multiplex_source: Source,
                   multiplex_source_opts: ctx.source,
                   multiplex_status_check_interval: 60_000
                 },
                 opts
               )
             )

    state
  end

  defp armed_socket_past_deadline(ctx) do
    state = init_socket(ctx)

    assert {:push, {:text, _ready}, state} =
             WebSocket.handle_in(watch_frame("request-1", "10_0", "40"), state)

    cancel_timer(state.deadline_timer_ref)
    expired_at = System.monotonic_time(:millisecond) - 1
    watch = %{Map.fetch!(state.watches, "request-1") | deadline: expired_at}
    token = make_ref()

    state = %{
      state
      | watches: %{"request-1" => watch},
        deadlines: :gb_trees.insert({expired_at, "request-1"}, true, :gb_trees.empty()),
        deadline_timer_at: expired_at,
        deadline_timer_ref: nil,
        deadline_timer_token: token
    }

    {state, token}
  end

  defp timed_watch_frame(id, timeout_ms) do
    {Jason.encode!(%{
       type: "watch",
       id: id,
       handle: "shape-1",
       offset: "10_0",
       cursor: nil,
       shape: @items_shape,
       timeout_ms: timeout_ms
     }), opcode: :text}
  end

  defp watch_frame(id, offset, cursor) do
    {Jason.encode!(%{
       type: "watch",
       id: id,
       handle: "shape-1",
       offset: offset,
       cursor: cursor,
       shape: @items_shape
     }), opcode: :text}
  end

  defp websocket_conn do
    conn(:get, "/v1/shape/multiplex")
    |> with_host_header()
    |> put_req_header("connection", "upgrade")
    |> put_req_header("upgrade", "websocket")
    |> put_req_header("sec-websocket-key", Base.encode64(:crypto.strong_rand_bytes(16)))
    |> put_req_header("sec-websocket-version", "13")
    |> put_req_header("sec-websocket-protocol", Multiplex.protocol())
  end

  defp plug_opts(ctx) do
    [
      api: ctx.api,
      availability_guard: fn -> :ok end,
      multiplex_source: Source,
      multiplex_source_opts: ctx.source,
      subprotocol: Multiplex.protocol()
    ]
  end

  defp decode_messages({:text, payload}), do: [Jason.decode!(payload)]

  defp decode_messages(messages),
    do: Enum.map(messages, fn {:text, payload} -> Jason.decode!(payload) end)

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: Process.cancel_timer(ref)

  defp with_host_header(conn) do
    %{conn | host: "example.test", req_headers: [{"host", "example.test"} | conn.req_headers]}
  end
end
