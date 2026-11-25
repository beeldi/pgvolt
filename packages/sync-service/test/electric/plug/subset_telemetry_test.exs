defmodule Electric.Plug.SubsetTelemetryTest do
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Plug.Test
  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup

  alias Electric.Plug.Router
  alias Electric.Shapes.Consumer.State
  alias Electric.Shapes.Shape
  alias Electric.Telemetry.OpenTelemetry

  @moduletag :tmp_dir

  setup [:with_unique_db, :with_basic_tables, :with_sql_execute]
  setup :with_complete_stack

  setup(ctx) do
    :ok =
      Electric.StatusMonitor.wait_until_active(ctx.stack_id,
        timeout: ExUnit.configuration()[:assert_receive_timeout]
      )

    %{opts: Router.init(build_router_opts(ctx))}
  end

  @tag with_sql: [
         "INSERT INTO items VALUES (gen_random_uuid(), 'test value 1')",
         "INSERT INTO items VALUES (gen_random_uuid(), 'test value 2')"
       ]
  test "adds POST body attrs and subset query attrs for documented POST subset requests", ctx do
    test_pid = self()

    Repatch.patch(OpenTelemetry, :add_span_attributes, [mode: :shared], fn attrs ->
      if is_map(attrs) and
           (Map.has_key?(attrs, "subset.rows") or
              Map.has_key?(attrs, "http.body_param.subset.where")) do
        send(test_pid, {:subset_span_attrs, attrs})
      end

      true
    end)

    conn =
      conn(
        "POST",
        "/v1/shape?table=items&offset=-1&log=changes_only",
        Jason.encode!(%{
          "where" => "value ILIKE $1",
          "params" => %{"1" => "%2"},
          "limit" => 1,
          "offset" => 0,
          "order_by" => "value ASC"
        })
      )
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Router.call(ctx.opts)

    assert conn.status == 200

    assert %{
             "data" => [
               %{
                 "value" => %{"value" => "test value 2"}
               }
             ]
           } = Jason.decode!(conn.resp_body)

    assert_receive {:subset_span_attrs, attrs1}
    assert_receive {:subset_span_attrs, attrs2}
    refute_receive {:subset_span_attrs, _other}

    {request_attrs, query_attrs} =
      case {Map.has_key?(attrs1, "http.body_param.subset.where"),
            Map.has_key?(attrs2, "http.body_param.subset.where")} do
        {true, false} -> {attrs1, attrs2}
        {false, true} -> {attrs2, attrs1}
      end

    assert request_attrs["http.body_param.subset.limit"] == 1
    assert request_attrs["http.body_param.subset.offset"] == 0
    assert request_attrs["http.body_param.subset.where"] == "value ILIKE $1"
    assert request_attrs["http.body_param.subset.params"] == ~s(%{"1" => "%2"})

    assert query_attrs["subset.rows"] == 1
    assert query_attrs["subset.result_bytes"] > 0
  end

  test "truncates large POST body strings to 2000 graphemes in telemetry attrs", ctx do
    test_pid = self()
    long_where = "value ILIKE $1" <> String.duplicate(" ", 3_000)

    Repatch.patch(OpenTelemetry, :add_span_attributes, [mode: :shared], fn attrs ->
      if is_map(attrs) and Map.has_key?(attrs, "http.body_param.subset.where") do
        send(test_pid, {:body_attrs, attrs})
      end

      true
    end)

    conn =
      conn(
        "POST",
        "/v1/shape?table=items&offset=-1&log=changes_only",
        Jason.encode!(%{
          "where" => long_where,
          "params" => %{"1" => "%2"}
        })
      )
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Router.call(ctx.opts)

    assert conn.status == 200
    assert_receive {:body_attrs, attrs}
    assert String.length(attrs["http.body_param.subset.where"]) == 2_000
    assert attrs["http.body_param.subset.where"] == String.slice(long_where, 0, 2_000)
  end

  @tag :request_value_redaction
  test "shape creation logs keep the handle without the predicate", ctx do
    Application.put_env(:electric, :telemetry_redact_request_values?, true)
    previous_level = Logger.level()

    on_exit(fn ->
      Application.delete_env(:electric, :telemetry_redact_request_values?)
      Logger.configure(level: previous_level)
    end)

    Logger.configure(level: :debug)

    query =
      URI.encode_query(%{
        "table" => "items",
        "offset" => "-1",
        "where" => "value != 'shape-log-marker'"
      })

    {response, log} =
      ExUnit.CaptureLog.with_log(fn ->
        Router.call(conn("GET", "/v1/shape?" <> query), ctx.opts)
      end)

    assert response.status == 200
    assert [handle] = Plug.Conn.get_resp_header(response, "electric-handle")
    assert log =~ "Creating new shape"
    assert log =~ "Returning shape id #{handle}"
    refute log =~ "shape-log-marker"
  end

  @tag :request_value_redaction
  test "snapshot and consumer telemetry summarize the shape predicate" do
    Application.put_env(:electric, :telemetry_redact_request_values?, true)
    on_exit(fn -> Application.delete_env(:electric, :telemetry_redact_request_values?) end)
    where = "value = 'shape-marker'"

    shape = %Shape{
      root_table: {"public", "items"},
      root_table_id: 1,
      where: %{query: where}
    }

    state = %State{stack_id: "test-stack", shape_handle: "shape-handle", shape: shape}

    for attrs <- [Shape.otel_attrs("shape-handle", shape), State.telemetry_attrs(state)] do
      refute Keyword.has_key?(attrs, :"shape.where")
      assert attrs[:"shape.where.present"]
      assert attrs[:"shape.where.bytes"] == byte_size(where)
    end
  end

  @tag :request_value_redaction
  test "span errors keep the exception type without its PostgreSQL text", ctx do
    Application.put_env(:electric, :telemetry_redact_request_values?, true)
    on_exit(fn -> Application.delete_env(:electric, :telemetry_redact_request_values?) end)

    test_pid = self()

    Repatch.patch(:otel_span, :add_event, fn _, "exception", attrs ->
      send(test_pid, {:exception_event, attrs})
      true
    end)

    Repatch.patch(OpenTelemetry, :add_span_attributes, fn attrs ->
      if is_map(attrs) and Map.has_key?(attrs, "error.type") do
        send(test_pid, {:error_attrs, attrs})
      end

      true
    end)

    Repatch.patch(Electric.Shapes.Api, :validate_params, fn _, _ ->
      raise %Postgrex.Error{message: "invalid input syntax: error-marker"}
    end)

    assert_raise Plug.Conn.WrapperError, fn ->
      conn("GET", "/v1/shape?table=items&offset=-1")
      |> Router.call(ctx.opts)
    end

    assert_receive {:error_attrs, %{"error.type" => error_type}}
    assert_receive {:exception_event, attrs}

    assert {error_type, attrs[:"exception.message"]} ==
             {"Elixir.Postgrex.Error", "Elixir.Postgrex.Error"}
  end

  @tag :request_value_redaction
  test "spans summarize query predicates and flat and nested subset parameters", ctx do
    Application.put_env(:electric, :telemetry_redact_request_values?, true)
    on_exit(fn -> Application.delete_env(:electric, :telemetry_redact_request_values?) end)

    test_pid = self()

    Repatch.patch(OpenTelemetry, :add_span_attributes, [mode: :shared], fn attrs ->
      if is_map(attrs) and attrs["shape_req.is_error"] == false do
        send(test_pid, {:request_attrs, attrs})
      end

      true
    end)

    base_query =
      URI.encode_query(%{
        "table" => "items",
        "offset" => "-1",
        "log" => "changes_only",
        "where" => "value != $1",
        "params[1]" => "base-marker",
        "secret" => "query-credential"
      })

    subset = %{
      "where" => "value ILIKE $1",
      "params" => %{"1" => "subset-marker"},
      "limit" => 11,
      "offset" => 2,
      "order_by" => "value DESC"
    }

    for {method, query, body, prefix, params_key} <- [
          {"POST", base_query, subset, "http.body_param.subset.", "params"},
          {"POST", base_query, %{"subset" => subset}, "http.body_param.subset.", "params"},
          {"GET",
           base_query <>
             "&" <>
             URI.encode_query(%{
               "subset__where" => subset["where"],
               "subset__params[1]" => subset["params"]["1"],
               "subset__order_by" => subset["order_by"],
               "subset__limit" => subset["limit"],
               "subset__offset" => subset["offset"]
             }), nil, "http.query_param.", "subset__params"}
        ] do
      request =
        if body do
          conn(method, "/v1/shape?" <> query, Jason.encode!(body))
          |> Plug.Conn.put_req_header("content-type", "application/json")
        else
          conn(method, "/v1/shape?" <> query)
        end

      request = Plug.Conn.put_req_header(request, "referer", "https://example.com/?" <> query)
      assert Router.call(request, ctx.opts).status == 200
      assert_receive {:request_attrs, attrs}

      assert %{
               "http.query_string.present" => true,
               "http.query_string.bytes" => query_bytes,
               "http.query_param.where.present" => true,
               "http.query_param.params.present" => true,
               "http.query_param.secret" => "[REDACTED]",
               "shape_req.where.bytes" => 11,
               :"url.full" => "http://www.example.com/v1/shape"
             } = attrs

      assert query_bytes == byte_size(query)

      for key <- [
            params_key
            | Enum.map(~w(where limit offset order_by), fn key ->
                if method == "GET", do: "subset__" <> key, else: key
              end)
          ] do
        refute Map.has_key?(attrs, prefix <> key)
        assert attrs[prefix <> key <> ".present"]
        assert attrs[prefix <> key <> ".bytes"] > 0
      end

      refute Map.has_key?(attrs, "http.query_string")
      refute Map.has_key?(attrs, "http.request.header.referer")
      refute Map.has_key?(attrs, "shape.where")
      refute inspect(attrs) =~ "base-marker"
      refute inspect(attrs) =~ "subset-marker"
      refute inspect(attrs) =~ "query-credential"
      refute inspect(attrs) =~ "value DESC"
    end
  end
end
