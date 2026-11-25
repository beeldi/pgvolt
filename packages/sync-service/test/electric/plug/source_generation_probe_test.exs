defmodule Electric.Plug.SourceGenerationProbeTest do
  use ExUnit.Case, async: true

  import Plug.Test
  import Support.ComponentSetup

  alias Electric.Plug.Router
  alias Electric.Shapes.Api
  alias Electric.StackConfig

  @inspector Support.StubInspector.new(
               tables: ["items"],
               columns: [%{name: "id", type: "int8", pk_position: 0, type_id: {20, 1}}]
             )
  @query "table=items&offset=-1&source_positions=full_xid&secret=probe-secret"

  setup [:with_stack_id_from_test, :with_status_monitor]

  setup %{stack_id: stack_id} do
    Electric.StatusMonitor.mark_shape_metadata_ready(stack_id, self())
    Electric.StatusMonitor.wait_for_messages_to_be_processed(stack_id)
    StackConfig.put(stack_id, :full_xid_source_positions_ready, true)
    StackConfig.put(stack_id, :full_xid_source_generation, "generation-a")

    opts =
      Api.plug_opts(
        stack_id: stack_id,
        inspector: @inspector,
        feature_flags: ["full_xid_source_positions"],
        max_concurrent_requests: %{initial: 10, existing: 10},
        secret: "probe-secret"
      )
      |> Router.init()

    [opts: opts]
  end

  test "returns uncached provenance without starting a shape or snapshot", %{opts: opts} do
    response = Router.call(conn(:head, "/v1/shape?#{@query}"), opts)

    assert %{status: 200, resp_body: ""} = response

    assert Plug.Conn.get_resp_header(response, "electric-source-positions") == [
             "mvcc-full-xid-v1"
           ]

    assert Plug.Conn.get_resp_header(response, "electric-source-generation") == ["generation-a"]
    assert Plug.Conn.get_resp_header(response, "cache-control") == ["no-store"]
    assert Plug.Conn.get_resp_header(response, "surrogate-control") == ["no-store"]
    assert Plug.Conn.get_resp_header(response, "electric-handle") == []
    assert Plug.Conn.get_resp_header(response, "electric-offset") == []
  end

  test "reads the current generation on every request", %{opts: opts, stack_id: stack_id} do
    assert %{status: 200} = Router.call(conn(:head, "/v1/shape?#{@query}"), opts)
    StackConfig.put(stack_id, :full_xid_source_generation, "generation-b")
    response = Router.call(conn(:head, "/v1/shape?#{@query}"), opts)
    assert Plug.Conn.get_resp_header(response, "electric-source-generation") == ["generation-b"]
  end

  test "rejects missing authentication for every routed spelling", %{opts: opts} do
    for path <- ["/v1/shape", "/v1/shape/", "/v1/%73hape", "/v1//shape"] do
      response =
        Router.call(conn(:head, "#{path}?table=items&offset=-1&source_positions=full_xid"), opts)

      assert response.status == 401
      assert Plug.Conn.get_resp_header(response, "electric-source-generation") == []
    end
  end

  test "requires decoder readiness", %{opts: opts, stack_id: stack_id} do
    StackConfig.put(stack_id, :full_xid_source_positions_ready, false)
    response = Router.call(conn(:head, "/v1/shape?#{@query}"), opts)
    assert response.status == 503
    assert Plug.Conn.get_resp_header(response, "electric-source-generation") == []
  end

  test "retains source-mode and shape validation", %{opts: opts} do
    for suffix <- ["&experimental_compaction=true", "&where=missing_column=1"] do
      response = Router.call(conn(:head, "/v1/shape?#{@query}#{suffix}"), opts)
      assert response.status == 400
      assert Plug.Conn.get_resp_header(response, "electric-source-generation") == []
    end
  end

  test "requires the producer feature flag", %{opts: opts} do
    opts = Keyword.update!(opts, :api, &%{&1 | feature_flags: []})
    response = Router.call(conn(:head, "/v1/shape?#{@query}"), opts)
    assert response.status == 426
    assert Plug.Conn.get_resp_header(response, "electric-source-generation") == []
  end

  test "does not enumerate a response body" do
    body = Stream.map([:unexpected], fn _ -> flunk("probe enumerated a snapshot body") end)

    response =
      Api.Response.send_source_generation(conn(:head, "/v1/shape"), %Api.Response{body: body})

    assert %{status: 200, resp_body: ""} = response
  end
end
