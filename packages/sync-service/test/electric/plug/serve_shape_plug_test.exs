defmodule Electric.Plug.ServeShapePlugTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit, assert_expectations: true

  import Plug.Conn

  alias Electric.Postgres.Lsn
  alias Electric.Replication.LogOffset
  alias Electric.Plug.ServeShapePlug
  alias Electric.Shapes.Api
  alias Electric.Shapes.Shape

  import Support.ComponentSetup

  import Support.TestUtils,
    only: [
      set_status_to_active: 1,
      patch_shape_cache: 1,
      expect_shape_cache: 1,
      patch_storage: 1,
      patch_storage: 2,
      expect_storage: 1
    ]

  @inspector Support.StubInspector.new(
               tables: ["users"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0, type_id: {20, 1}},
                 %{name: "value", type: "text", pk_position: nil, type_id: {28, 1}}
               ]
             )
  @subquery_inspector Support.StubInspector.new(%{
                        {"public", "parent"} => [
                          %{name: "id", type: "int8", pk_position: 0, type_id: {20, 1}},
                          %{name: "value", type: "int8", pk_position: nil, type_id: {20, 1}}
                        ],
                        {"public", "child"} => [
                          %{name: "id", type: "int8", pk_position: 0, type_id: {20, 1}},
                          %{name: "parent_id", type: "int8", pk_position: nil, type_id: {20, 1}},
                          %{name: "value", type: "int8", pk_position: nil, type_id: {20, 1}}
                        ]
                      })
  @subquery_where "parent_id in (SELECT id FROM parent WHERE value = 1)"

  @test_shape %Shape{
    root_table: {"public", "users"},
    root_table_id: :erlang.phash2({"public", "users"}),
    root_column_count: 2,
    root_pk: ["id"],
    selected_columns: ["id", "value"],
    explicitly_selected_columns: ["id", "value"],
    flags: %{selects_all_columns: true}
  }
  @test_shape_handle "test-shape-handle"
  @test_opts %{foo: "bar"}
  @before_all_offset LogOffset.before_all()
  @first_offset LogOffset.first()
  @test_offset LogOffset.new(Lsn.from_integer(100), 0)
  @start_offset_50 LogOffset.new(Lsn.from_integer(50), 0)

  # Higher timeout is needed for some tests that tend to run slower on CI.
  @receive_timeout 2000

  defmodule RstStreamAdapter do
    @moduledoc false
    # Stands in for Bandit's adapter when the client has already reset the
    # HTTP/2 stream: any attempt to send the response raises, exactly like
    # Bandit.HTTP2.Stream.do_recv_rst_stream!/2 does.
    def send_resp(_state, _status, _headers, _body) do
      raise Bandit.TransportError, message: "Client reset stream normally", error: :closed
    end
  end

  @moduletag :tmp_dir

  setup [
    :with_stack_id_from_test,
    :with_registry,
    :with_persistent_kv,
    :with_pure_file_storage,
    :with_status_monitor,
    :with_shape_cleaner
  ]

  def conn(_ctx, method, params, "?" <> _ = query_string) do
    Plug.Test.conn(method, "/" <> query_string, params)
  end

  defp build_plug_opts(ctx) do
    Api.plug_opts(
      stack_id: ctx.stack_id,
      inspector: Access.get(ctx, :inspector, @inspector),
      feature_flags: Access.get(ctx, :feature_flags, []),
      stack_ready_timeout: Access.get(ctx, :stack_ready_timeout, 100),
      long_poll_timeout: long_poll_timeout(ctx),
      sse_timeout: sse_timeout(ctx),
      max_age: max_age(ctx),
      stale_age: stale_age(ctx),
      max_concurrent_requests:
        Access.get(ctx, :max_concurrent_requests, %{initial: 300, existing: 10_000})
    )
  end

  def call_serve_shape_plug(conn, ctx) do
    ServeShapePlug.call(conn, build_plug_opts(ctx))
  end

  describe "serving shape" do
    setup :with_lsn_tracker

    setup ctx do
      {:via, _, {registry_name, registry_key}} =
        Electric.Shapes.Supervisor.name(ctx.stack_id)

      {:ok, _} = Registry.register(registry_name, registry_key, nil)
      set_status_to_active(ctx)

      patch_storage(for_shape: fn @test_shape_handle, _opts -> @test_opts end)

      :ok
    end

    test "returns 400 for invalid table", ctx do
      conn =
        ctx
        |> conn(:get, %{"table" => ".invalid_shape"}, "?offset=-1")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400

      assert Jason.decode!(conn.resp_body) == %{
               "message" => "Invalid request",
               "errors" => %{
                 "table" => [
                   "Invalid zero-length delimited identifier"
                 ]
               }
             }

      assert get_resp_header(conn, "electric-has-data") == []
    end

    test "returns 400 for invalid offset", ctx do
      conn =
        ctx
        |> conn(:get, %{"table" => "foo"}, "?offset=invalid")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400

      assert Jason.decode!(conn.resp_body) == %{
               "message" => "Invalid request",
               "errors" => %{
                 "offset" => ["has invalid format"]
               }
             }
    end

    test "returns 400 for offset with negative parts", ctx do
      conn =
        ctx
        |> conn(:get, %{"table" => "foo"}, "?offset=0_-1")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400

      assert Jason.decode!(conn.resp_body) == %{
               "message" => "Invalid request",
               "errors" => %{
                 "offset" => ["has invalid format"]
               }
             }
    end

    test "returns 400 when table param is missing", ctx do
      conn =
        ctx
        |> conn(:get, %{}, "?offset=-1")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400

      assert %{
               "message" => "Invalid request",
               "errors" => %{
                 "table" => ["can't be blank"]
               }
             } = Jason.decode!(conn.resp_body)
    end

    test "returns 400 when table does not exist", ctx do
      # this will pass table name validation
      # but will fail to find the table
      conn =
        ctx
        |> conn(:get, %{"table" => "_val1d_schëmaΦ$.Φtàble"}, "?offset=-1")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400

      assert Jason.decode!(conn.resp_body) == %{
               "message" => "Invalid request",
               "errors" => %{
                 "table" => [
                   "Table \"_val1d_schëmaΦ$\".\"Φtàble\" does not exist. If the table name contains capitals or special characters you must quote it."
                 ]
               }
             }
    end

    test "returns 400 for missing shape_handle when offset != -1", ctx do
      conn =
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=#{LogOffset.first()}")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400

      assert Jason.decode!(conn.resp_body) == %{
               "message" => "Invalid request",
               "errors" => %{
                 "handle" => ["can't be blank when offset != -1"]
               }
             }
    end

    @tag long_poll_timeout: 100
    test "tells the client to refetch when offset is out of bounds after a timeout", ctx do
      out_of_bounds_offset = LogOffset.increment(@test_offset)

      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end
      )

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?handle=#{@test_shape_handle}&offset=#{out_of_bounds_offset}"
        )
        |> call_serve_shape_plug(ctx)

      assert conn.status == 409

      assert Jason.decode!(conn.resp_body) == [%{"headers" => %{"control" => "must-refetch"}}]
    end

    test "returns 400 for live request when offset == -1", ctx do
      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{LogOffset.before_all()}&live=true"
        )
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400

      assert Jason.decode!(conn.resp_body) == %{
               "message" => "Invalid request",
               "errors" => %{
                 "live" => ["can't be true when offset == -1"]
               }
             }
    end

    test "returns 400 when creating a subquery shape with compaction enabled", ctx do
      ctx =
        ctx
        |> Map.put(:inspector, @subquery_inspector)

      Repatch.patch(Electric.Shapes, :fetch_handle_by_shape, fn _, _ ->
        flunk("should reject before checking whether the shape already exists")
      end)

      conn =
        ctx
        |> conn(
          :get,
          %{
            "table" => "public.child",
            "where" => @subquery_where,
            "experimental_compaction" => "true"
          },
          "?offset=now"
        )
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400

      assert Jason.decode!(conn.resp_body) == %{
               "message" => "Invalid request",
               "errors" => %{
                 "experimental_compaction" => ["can't be enabled for shapes with subqueries"]
               }
             }
    end

    test "returns snapshot when offset is -1", ctx do
      expect_shape_cache(
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      patch_shape_cache(has_shape?: fn @test_shape_handle, _opts -> true end)

      next_offset = LogOffset.increment(@first_offset)

      expect_storage(
        get_chunk_end_log_offset: fn @before_all_offset, _ ->
          @first_offset
        end,
        get_log_stream: fn @before_all_offset, @first_offset, @test_opts ->
          [Jason.encode!(%{key: "log", value: "foo", headers: %{}, offset: next_offset})]
        end
      )

      conn =
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 200

      assert Jason.decode!(conn.resp_body) == [
               %{
                 "key" => "log",
                 "value" => "foo",
                 "headers" => %{},
                 "offset" => "#{next_offset}"
               }
             ]

      assert get_resp_header(conn, "etag") == [
               ~s|"#{@test_shape_handle}:-1:#{@first_offset}"|
             ]

      assert get_resp_header(conn, "electric-handle") == [@test_shape_handle]
    end

    test "returns 503 when await_snapshot_start fails with SnapshotError", ctx do
      snapshot_error = Electric.SnapshotError.slow_snapshot_start()

      expect_shape_cache(
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        await_snapshot_start: fn @test_shape_handle, _ -> {:error, snapshot_error} end
      )

      patch_shape_cache(has_shape?: fn @test_shape_handle, _opts -> true end)

      patch_storage(get_chunk_end_log_offset: fn @before_all_offset, _ -> @first_offset end)

      conn =
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 503
      assert %{"message" => message} = Jason.decode!(conn.resp_body)
      assert message == snapshot_error.message
    end

    test "snapshot has correct cache control headers", ctx do
      expect_shape_cache(
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      patch_shape_cache(has_shape?: fn @test_shape_handle, _opts -> true end)

      next_offset = LogOffset.increment(@first_offset)

      expect_storage(
        get_chunk_end_log_offset: fn @before_all_offset, _ ->
          next_offset
        end,
        get_log_stream: fn @before_all_offset, _, @test_opts ->
          [Jason.encode!(%{key: "log", value: "foo", headers: %{}, offset: next_offset})]
        end
      )

      max_age = 62
      stale_age = 312

      ctx =
        ctx
        |> Map.put(:max_age, max_age)
        |> Map.put(:stale_age, stale_age)

      conn =
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 200

      assert get_resp_header(conn, "cache-control") == [
               "public, max-age=604800, s-maxage=3600, stale-while-revalidate=2629746"
             ]
    end

    test "sets correct CORS headers for Access-Control-Expose-Headers", ctx do
      expect_shape_cache(
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      patch_shape_cache(has_shape?: fn @test_shape_handle, _opts -> true end)

      expect_storage(
        get_chunk_end_log_offset: fn @before_all_offset, _ ->
          @first_offset
        end,
        get_log_stream: fn @before_all_offset, _, @test_opts ->
          []
        end
      )

      conn =
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
        # Apply the CORS headers manually since we're not going through the router
        |> Electric.Plug.Router.put_cors_headers([])
        |> call_serve_shape_plug(ctx)

      # Verify that all Electric headers are included in the response
      assert [expose_header] = get_resp_header(conn, "access-control-expose-headers")
      exposed_headers_in_response = String.split(expose_header, ",")

      assert Enum.sort(exposed_headers_in_response) ==
               Enum.sort(Electric.Shapes.Api.Response.electric_headers())
    end

    test "invalid response specifies it should not be cached", ctx do
      conn =
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=bababa")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400
      assert get_resp_header(conn, "cache-control") == ["no-store"]
    end

    test "response has correct schema header", ctx do
      expect_shape_cache(
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      patch_shape_cache(has_shape?: fn @test_shape_handle, _opts -> true end)

      next_offset = LogOffset.increment(@first_offset)

      expect_storage(
        get_chunk_end_log_offset: fn @before_all_offset, _ ->
          next_offset
        end,
        get_log_stream: fn @before_all_offset, _, @test_opts ->
          [Jason.encode!(%{key: "log", value: "foo", headers: %{}, offset: next_offset})]
        end
      )

      conn =
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
        |> call_serve_shape_plug(ctx)

      assert get_resp_header(conn, "electric-schema") == [
               ~s|{"id":{"type":"int8","pk_index":0},"value":{"type":"text"}}|
             ]
    end

    test "returns log when offset is >= 0", ctx do
      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      next_offset = LogOffset.increment(@start_offset_50)
      next_next_offset = LogOffset.increment(next_offset)

      expect_storage(
        get_chunk_end_log_offset: fn @start_offset_50, _ ->
          next_next_offset
        end,
        get_log_stream: fn @start_offset_50, _, @test_opts ->
          [
            Jason.encode!(%{key: "log1", value: "foo", headers: %{}, offset: next_offset}),
            Jason.encode!(%{key: "log2", value: "bar", headers: %{}, offset: next_next_offset})
          ]
        end
      )

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{@start_offset_50}&handle=#{@test_shape_handle}"
        )
        |> call_serve_shape_plug(ctx)

      assert conn.status == 200

      assert Jason.decode!(conn.resp_body) == [
               %{
                 "key" => "log1",
                 "value" => "foo",
                 "headers" => %{},
                 "offset" => "#{next_offset}"
               },
               %{
                 "key" => "log2",
                 "value" => "bar",
                 "headers" => %{},
                 "offset" => "#{next_next_offset}"
               }
             ]

      assert get_resp_header(conn, "etag") == [
               ~s|"#{@test_shape_handle}:#{@start_offset_50}:#{next_next_offset}"|
             ]

      assert get_resp_header(conn, "electric-handle") == [@test_shape_handle]

      assert get_resp_header(conn, "electric-offset") == [
               "#{next_next_offset}"
             ]

      assert get_resp_header(conn, "electric-up-to-date") == []
      assert get_resp_header(conn, "electric-has-data") == ["true"]
    end

    test "returns 304 Not Modified when If-None-Match matches ETag",
         ctx do
      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end
      )

      expect_storage(
        get_chunk_end_log_offset: fn @start_offset_50, _ ->
          @test_offset
        end
      )

      conn = get_shape_with_etag(ctx, @start_offset_50)
      assert conn.status == 304
      assert conn.resp_body == ""
    end

    test "the 304 response includes caching headers that are appropriate for the offset", ctx do
      patch_shape_cache(
        has_shape?: fn @test_shape_handle, _opts -> true end,
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end
      )

      expect_storage(
        get_chunk_end_log_offset: fn @before_all_offset, _ -> @test_offset end,
        get_chunk_end_log_offset: {fn @start_offset_50, _ -> @test_offset end, exactly: 2}
      )

      conn = get_shape_with_etag(ctx, @before_all_offset)
      assert conn.status == 304
      cache_control = "public, max-age=604800, s-maxage=3600, stale-while-revalidate=2629746"
      assert {"cache-control", cache_control} in conn.resp_headers

      conn = get_shape_with_etag(ctx, @start_offset_50)
      assert conn.status == 304
      cache_control = "public, max-age=#{max_age(ctx)}, stale-while-revalidate=#{stale_age(ctx)}"
      assert {"cache-control", cache_control} in conn.resp_headers

      conn = get_shape_with_etag(ctx, @start_offset_50, live: true)
      assert conn.status == 304

      cache_control = "public, max-age=5, stale-while-revalidate=5"
      assert {"cache-control", cache_control} in conn.resp_headers

      expected_cursor =
        Electric.Plug.Utils.get_next_interval_timestamp(long_poll_timeout(ctx), nil)
        |> to_string()

      assert {"electric-cursor", expected_cursor} in conn.resp_headers
    end

    test "handles live updates", ctx do
      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      test_pid = self()
      next_offset = LogOffset.increment(@test_offset)
      next_offset_str = "#{next_offset}"

      expect_storage(
        get_chunk_end_log_offset: fn @test_offset, _ -> nil end,
        get_log_stream: fn @test_offset, @test_offset, @test_opts ->
          send(test_pid, :got_log_stream)
          []
        end,
        get_chunk_end_log_offset: fn @test_offset, _ -> nil end,
        get_log_stream: fn @test_offset, ^next_offset, @test_opts ->
          [Jason.encode!("test result")]
        end
      )

      task =
        Task.async(fn ->
          ctx
          |> conn(
            :get,
            %{"table" => "public.users"},
            "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true"
          )
          |> call_serve_shape_plug(ctx)
        end)

      assert_receive :got_log_stream, @receive_timeout

      # Simulate new changes arriving
      Registry.dispatch(ctx.registry, @test_shape_handle, fn [{pid, ref}] ->
        send(pid, {ref, :new_changes, next_offset})
      end)

      # The conn process should exit after sending the response
      conn = Task.await(task)

      assert conn.status == 200

      assert Jason.decode!(conn.resp_body) == [
               "test result",
               %{
                 "headers" => %{
                   "control" => "up-to-date",
                   "global_last_seen_lsn" => to_string(next_offset.tx_offset)
                 }
               }
             ]

      assert get_resp_header(conn, "cache-control") == [
               "public, max-age=5, stale-while-revalidate=5"
             ]

      assert get_resp_header(conn, "electric-offset") == [next_offset_str]
      assert get_resp_header(conn, "electric-up-to-date") == [""]
      assert get_resp_header(conn, "electric-has-data") == ["true"]
      assert get_resp_header(conn, "electric-schema") == []

      expected_cursor =
        Electric.Plug.Utils.get_next_interval_timestamp(long_poll_timeout(ctx), nil)
        |> to_string()

      assert {"electric-cursor", expected_cursor} in conn.resp_headers
    end

    test "handles shape rotation", ctx do
      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      test_pid = self()

      expect_storage(
        get_chunk_end_log_offset: fn @test_offset, _ ->
          nil
        end,
        get_log_stream: fn @test_offset, _, @test_opts ->
          send(test_pid, :got_log_stream)
          []
        end
      )

      task =
        Task.async(fn ->
          ctx
          |> conn(
            :get,
            %{"table" => "public.users"},
            "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true"
          )
          |> call_serve_shape_plug(ctx)
        end)

      assert_receive :got_log_stream, @receive_timeout

      # Simulate shape rotation
      Registry.dispatch(ctx.registry, @test_shape_handle, fn [{pid, ref}] ->
        send(pid, {ref, :shape_rotation})
      end)

      conn = Task.await(task)

      assert conn.status == 409
      assert [%{"headers" => %{"control" => "must-refetch"}}] = Jason.decode!(conn.resp_body)
    end

    test "sends an up-to-date response after a timeout if no changes are observed",
         ctx do
      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      expect_storage(
        get_chunk_end_log_offset: fn @test_offset, _ -> nil end,
        get_log_stream: fn @test_offset, _, @test_opts -> [] end
      )

      ctx = Map.put(ctx, :long_poll_timeout, 100)

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true"
        )
        |> call_serve_shape_plug(ctx)

      assert conn.status == 200

      assert [%{"headers" => %{"control" => "up-to-date"}}] = Jason.decode!(conn.resp_body)

      assert get_resp_header(conn, "cache-control") == [
               "public, max-age=5, stale-while-revalidate=5"
             ]

      expected_etag_part = "\"#{@test_shape_handle}:#{@test_offset}:#{@test_offset}:"
      assert [^expected_etag_part <> _rest] = get_resp_header(conn, "etag")

      assert get_resp_header(conn, "electric-up-to-date") == [""]
      assert get_resp_header(conn, "electric-has-data") == ["false"]
    end

    test "aborts a live long-poll as soon as the client resets the HTTP/2 stream", ctx do
      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      test_pid = self()

      expect_storage(
        get_chunk_end_log_offset: fn @test_offset, _ -> nil end,
        get_log_stream: fn @test_offset, _, @test_opts ->
          send(test_pid, :got_log_stream)
          []
        end
      )

      task =
        Task.async(fn ->
          ctx
          |> conn(
            :get,
            %{"table" => "public.users"},
            "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true"
          )
          |> call_serve_shape_plug(ctx)
        end)

      assert_receive :got_log_stream, @receive_timeout

      # Simulate Bandit's HTTP/2 connection process delivering the client's
      # RST_STREAM (error code 8 = CANCEL) to the request-handler process.
      Registry.dispatch(ctx.registry, @test_shape_handle, fn [{pid, _ref}] ->
        send(pid, {:bandit, {:rst_stream, 8}})
      end)

      # The default long_poll_timeout is 20s here, so completing well within
      # a second proves the poll was aborted by the reset, not the timeout.
      conn = Task.await(task)

      assert conn.status == 499
      assert conn.resp_body == ""
      assert get_resp_header(conn, "cache-control") == ["no-store"]
    end

    test "records a client disconnect when the response send hits a reset stream", ctx do
      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      expect_storage(
        get_chunk_end_log_offset: fn @test_offset, _ -> nil end,
        get_log_stream: fn @test_offset, _, @test_opts -> [] end
      )

      ctx = Map.put(ctx, :long_poll_timeout, 100)

      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [[:electric, :plug, :serve_shape]])

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true"
        )
        |> then(fn conn -> %{conn | adapter: {RstStreamAdapter, elem(conn.adapter, 1)}} end)

      error =
        assert_raise Plug.Conn.WrapperError, fn ->
          call_serve_shape_plug(conn, ctx)
        end

      # The transport error is reraised for Bandit to handle natively (it
      # closes client-reset streams quietly), but the request is accounted
      # for as a client disconnect, not a 500.
      assert %Bandit.TransportError{error: :closed} = error.reason
      assert error.conn.status == 499

      # Note: `live` is false here because the exception path only sees the
      # pre-pipeline conn without the request assign (see the moduledoc note
      # in ServeShapePlug) — the root span's `shape_req.is_live` attr is
      # unaffected as it falls back to query params.
      assert_received {[:electric, :plug, :serve_shape], ^telemetry_ref, %{count: 1},
                       %{status: 499}}
    end

    test "returns electric-has-data: false for offset=now requests", ctx do
      patch_shape_cache(
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end
      )

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=now&handle=#{@test_shape_handle}"
        )
        |> call_serve_shape_plug(ctx)

      assert conn.status == 200
      assert get_resp_header(conn, "electric-has-data") == ["false"]
      assert get_resp_header(conn, "electric-up-to-date") == [""]
    end

    test "sends 409 with a redirect to existing shape when requested shape handle does not exist",
         ctx do
      patch_shape_cache(
        resolve_shape_handle: fn "foo", @test_shape, _stack_id, _opts -> nil end,
        fetch_handle_by_shape: fn @test_shape, _opts -> {:ok, @test_shape_handle} end,
        has_shape?: fn "foo", _opts -> false end
      )

      patch_storage(fetch_latest_offset: fn _ -> {:ok, LogOffset.last_before_real_offsets()} end)

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{"50_12"}&handle=foo"
        )
        |> call_serve_shape_plug(ctx)

      assert conn.status == 409

      assert Jason.decode!(conn.resp_body) == [%{"headers" => %{"control" => "must-refetch"}}]
      assert get_resp_header(conn, "electric-handle") == [@test_shape_handle]
      assert get_resp_header(conn, "cache-control") == ["public, max-age=60, must-revalidate"]
    end

    test "creates a new shape when shape handle does not exist and sends a 409 redirecting to the newly created shape",
         ctx do
      new_shape_handle = "new-shape-handle"

      patch_shape_cache(has_shape?: fn @test_shape_handle, _opts -> false end)

      expect_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          nil
        end,
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {new_shape_handle, @test_offset}
        end
      )

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{"50_12"}&handle=#{@test_shape_handle}"
        )
        |> call_serve_shape_plug(ctx)

      assert conn.status == 409

      assert Jason.decode!(conn.resp_body) == [%{"headers" => %{"control" => "must-refetch"}}]
      assert get_resp_header(conn, "electric-handle") == [new_shape_handle]
    end

    test "sends 409 when shape handle does not match shape definition",
         ctx do
      new_shape_handle = "new-shape-handle"

      expect_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          nil
        end,
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {new_shape_handle, @test_offset}
        end
      )

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{"50_12"}&handle=#{@test_shape_handle}"
        )
        |> call_serve_shape_plug(ctx)

      assert conn.status == 409
      assert get_resp_header(conn, "cache-control") == ["public, max-age=60, must-revalidate"]

      assert Jason.decode!(conn.resp_body) == [%{"headers" => %{"control" => "must-refetch"}}]
      assert get_resp_header(conn, "electric-handle") == [new_shape_handle]
    end

    test "sends 400 when omitting primary key columns in selection", ctx do
      conn =
        ctx
        |> conn(:get, %{"table" => "public.users", "columns" => "value"}, "?offset=-1")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400

      assert Jason.decode!(conn.resp_body) == %{
               "message" => "Invalid request",
               "errors" => %{
                 "columns" => [
                   "The list of columns must include all primary key columns, missing: id"
                 ]
               }
             }
    end

    test "sends 400 when selecting invalid columns", ctx do
      conn =
        ctx
        |> conn(:get, %{"table" => "public.users", "columns" => "id,invalid"}, "?offset=-1")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400

      assert Jason.decode!(conn.resp_body) == %{
               "message" => "Invalid request",
               "errors" => %{
                 "columns" => ["The following columns are not found on the table: invalid"]
               }
             }
    end

    test "honours replica query param for shape", ctx do
      test_shape_handle = "test-shape-without-deltas"
      next_offset = LogOffset.increment(@first_offset)

      patch_shape_cache(has_shape?: fn ^test_shape_handle, _opts -> true end)

      expect_shape_cache(
        get_or_create_shape_handle: fn %{root_table: {"public", "users"}, replica: :full},
                                       _stack_id,
                                       _opts ->
          {test_shape_handle, @test_offset}
        end,
        await_snapshot_start: fn ^test_shape_handle, _ -> :started end
      )

      patch_storage([force: true],
        for_shape: fn ^test_shape_handle, _opts -> @test_opts end
      )

      expect_storage(
        get_chunk_end_log_offset: fn @before_all_offset, _ ->
          next_offset
        end,
        get_log_stream: fn @before_all_offset, _, @test_opts ->
          [Jason.encode!(%{key: "log", value: "foo", headers: %{}, offset: next_offset})]
        end
      )

      conn =
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=-1&replica=full")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 200

      assert Jason.decode!(conn.resp_body) == [
               %{
                 "key" => "log",
                 "value" => "foo",
                 "headers" => %{},
                 "offset" => "#{next_offset}"
               }
             ]

      assert get_resp_header(conn, "etag") == [
               ~s|"#{test_shape_handle}:-1:#{next_offset}"|
             ]

      assert get_resp_header(conn, "electric-handle") == [test_shape_handle]
    end

    test "emits [:electric, :shape, :response_size] telemetry event", ctx do
      expect_shape_cache(
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      patch_shape_cache(has_shape?: fn @test_shape_handle, _opts -> true end)

      next_offset = LogOffset.increment(@first_offset)

      expect_storage(
        get_chunk_end_log_offset: fn @before_all_offset, _ ->
          @first_offset
        end,
        get_log_stream: fn @before_all_offset, @first_offset, @test_opts ->
          [Jason.encode!(%{key: "log", value: "foo", headers: %{}, offset: next_offset})]
        end
      )

      test_pid = self()
      ref = make_ref()
      handler_id = "test-response-size-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:electric, :shape, :response_size],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_response_size, event, measurements, metadata})
        end,
        nil
      )

      stack_id = ctx.stack_id

      try do
        conn =
          ctx
          |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
          |> call_serve_shape_plug(ctx)

        assert conn.status == 200

        assert_receive {:telemetry_response_size, [:electric, :shape, :response_size],
                        measurements, %{stack_id: ^stack_id} = metadata}

        assert is_integer(measurements.bytes) and measurements.bytes > 0
        assert metadata.root_table == "public.users"
        assert metadata.is_live == false
      after
        :telemetry.detach(handler_id)
      end
    end

    test "[:electric, :shape, :response_size] uses nil label when request was not validated",
         ctx do
      test_pid = self()
      ref = make_ref()
      handler_id = "test-response-size-nil-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:electric, :shape, :response_size],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_response_size, event, measurements, metadata})
        end,
        nil
      )

      stack_id = ctx.stack_id

      try do
        # Missing `table` param fails validation, so the request never gets a
        # parsed root_table. The fallback label must be `nil`, not anything
        # attacker-controlled from the query string.
        conn =
          ctx
          |> conn(:get, %{}, "?offset=-1")
          |> call_serve_shape_plug(ctx)

        assert conn.status == 400

        assert_receive {:telemetry_response_size, [:electric, :shape, :response_size],
                        _measurements, %{stack_id: ^stack_id} = metadata}

        assert metadata.root_table == nil
      after
        :telemetry.detach(handler_id)
      end
    end

    test "[:electric, :shape, :response_size] labels a predefined shape by its own table, not the `table` parameter",
         ctx do
      patch_shape_cache(
        get_or_create_shape_handle: fn %Shape{root_table: {"public", "users"}},
                                       _stack_id,
                                       _opts ->
          {@test_shape_handle, @test_offset}
        end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end,
        has_shape?: fn @test_shape_handle, _opts -> true end
      )

      next_offset = LogOffset.increment(@first_offset)

      patch_storage(
        get_chunk_end_log_offset: fn @before_all_offset, _ -> @first_offset end,
        get_log_stream: fn @before_all_offset, @first_offset, @test_opts ->
          [Jason.encode!(%{key: "log", value: "foo", headers: %{}, offset: next_offset})]
        end
      )

      plug_opts = build_plug_opts(ctx)
      {:ok, api} = Api.predefined_shape(plug_opts[:api], table: "users")
      plug_opts = Keyword.put(plug_opts, :api, api)

      test_pid = self()
      stack_id = ctx.stack_id
      handler_id = "test-response-size-predefined-#{inspect(make_ref())}"

      :telemetry.attach(
        handler_id,
        [:electric, :shape, :response_size],
        fn _event, _measurements, metadata, _config ->
          if metadata.stack_id == stack_id,
            do: send(test_pid, {:telemetry_response_size, metadata})
        end,
        nil
      )

      try do
        conn =
          ctx
          |> conn(:get, %{"table" => "caller_chosen_label"}, "?offset=-1")
          |> ServeShapePlug.call(plug_opts)

        assert conn.status == 200

        assert_receive {:telemetry_response_size, %{root_table: root_table}}
        assert root_table == "public.users"
      after
        :telemetry.detach(handler_id)
      end
    end

    test "[:electric, :plug, :serve_shape] tags a successful response with known_error: false",
         ctx do
      expect_shape_cache(
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      patch_shape_cache(has_shape?: fn @test_shape_handle, _opts -> true end)

      next_offset = LogOffset.increment(@first_offset)

      expect_storage(
        get_chunk_end_log_offset: fn @before_all_offset, _ ->
          @first_offset
        end,
        get_log_stream: fn @before_all_offset, @first_offset, @test_opts ->
          [Jason.encode!(%{key: "log", value: "foo", headers: %{}, offset: next_offset})]
        end
      )

      test_pid = self()
      ref = make_ref()
      handler_id = "test-serve-shape-known-error-false-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:electric, :plug, :serve_shape],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_serve_shape, event, measurements, metadata})
        end,
        nil
      )

      stack_id = ctx.stack_id

      try do
        conn =
          ctx
          |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
          |> call_serve_shape_plug(ctx)

        assert conn.status == 200

        assert_receive {:telemetry_serve_shape, [:electric, :plug, :serve_shape], _measurements,
                        %{stack_id: ^stack_id} = metadata}

        assert metadata.status == 200
        assert metadata.known_error == false
      after
        :telemetry.detach(handler_id)
      end
    end

    test "[:electric, :plug, :serve_shape] tags an admission-rejected 503 with known_error: true",
         ctx do
      # Force the load-shedding path: with a zero concurrency limit, check_admission
      # rejects immediately with a 503 carrying the `electric-internal-known-error`
      # header.
      ctx = Map.put(ctx, :max_concurrent_requests, %{initial: 0, existing: 0})

      test_pid = self()
      ref = make_ref()
      handler_id = "test-serve-shape-known-error-true-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:electric, :plug, :serve_shape],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_serve_shape, event, measurements, metadata})
        end,
        nil
      )

      stack_id = ctx.stack_id

      try do
        conn =
          ctx
          |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
          |> call_serve_shape_plug(ctx)

        assert conn.status == 503

        assert_receive {:telemetry_serve_shape, [:electric, :plug, :serve_shape], _measurements,
                        %{stack_id: ^stack_id} = metadata}

        assert metadata.status == 503
        assert metadata.known_error == true
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  describe "serving shapes with sse mode" do
    setup :with_lsn_tracker

    setup ctx do
      {:via, _, {registry_name, registry_key}} =
        Electric.Shapes.Supervisor.name(ctx.stack_id)

      {:ok, _} = Registry.register(registry_name, registry_key, nil)
      set_status_to_active(ctx)

      patch_storage(for_shape: fn @test_shape_handle, _opts -> @test_opts end)

      :ok
    end

    test "returns proper SSE format response when live_sse=true and live=true", ctx do
      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      expect_storage(
        get_chunk_end_log_offset: fn @test_offset, _ -> nil end,
        get_log_stream: fn @test_offset, @test_offset, @test_opts -> [] end
      )

      # Use a short SSE timeout for the test
      ctx = Map.put(ctx, :sse_timeout, 100)

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true&live_sse=true"
        )
        |> call_serve_shape_plug(ctx)

      assert {"content-type", "text/event-stream"} in conn.resp_headers
      assert {"connection", "keep-alive"} in conn.resp_headers

      cache_control =
        Enum.find_value(conn.resp_headers, fn
          {"cache-control", value} -> value
          _ -> nil
        end)

      assert cache_control =~ "public"
      assert cache_control =~ "max-age="

      assert conn.state == :chunked

      assert conn.status == 200
      assert conn.state == :chunked
    end

    test "ends an SSE stream at once when its connection is shut down", ctx do
      test_pid = self()

      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      expect_storage(
        get_chunk_end_log_offset: fn @test_offset, _ -> nil end,
        get_log_stream: fn @test_offset, @test_offset, @test_opts ->
          send(test_pid, :got_log_stream)
          []
        end
      )

      # Far longer than the Task.await below, so only the shutdown can end the stream in time.
      ctx = Map.put(ctx, :sse_timeout, 60_000)

      # The request runs as in a ThousandIsland connection: a process that traps exits, which
      # its supervisor stops with an exit signal of reason :shutdown.
      task =
        Task.async(fn ->
          Process.flag(:trap_exit, true)

          conn =
            ctx
            |> conn(
              :get,
              %{"table" => "public.users"},
              "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true&live_sse=true"
            )
            |> call_serve_shape_plug(ctx)

          signal_kept? =
            receive do
              {:EXIT, ^test_pid, :shutdown} -> true
            after
              0 -> false
            end

          {conn, signal_kept?}
        end)

      assert_receive :got_log_stream, @receive_timeout
      Process.exit(task.pid, :shutdown)

      assert {conn, signal_kept?} = Task.await(task, @receive_timeout)

      assert conn.status == 200
      assert conn.state == :chunked

      assert signal_kept?,
             "the shutdown signal must stay in the mailbox for the connection's own loop"
    end

    test "omits the connection header from an SSE response on HTTP/2", ctx do
      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      expect_storage(
        get_chunk_end_log_offset: fn @test_offset, _ -> nil end,
        get_log_stream: fn @test_offset, @test_offset, @test_opts -> [] end
      )

      ctx = Map.put(ctx, :sse_timeout, 100)

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true&live_sse=true"
        )
        |> Plug.Test.put_http_protocol(:"HTTP/2")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 200
      assert conn.state == :chunked
      assert {"content-type", "text/event-stream"} in conn.resp_headers
      refute List.keymember?(conn.resp_headers, "connection", 0)
    end

    test "returns 400 when live_sse=true but live=false", ctx do
      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live_sse=true"
        )
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400

      assert Jason.decode!(conn.resp_body) == %{
               "message" => "Invalid request",
               "errors" => %{
                 "live_sse" => ["can't be true unless live is also true"]
               }
             }
    end

    test "sends properly formatted SSE events", ctx do
      next_offset = LogOffset.increment(@test_offset)
      test_content = %{key: "test-key", value: "test-value", headers: %{}, offset: next_offset}

      patch_shape_cache(
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      expect_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end
      )

      expect_storage(
        get_chunk_end_log_offset: fn @test_offset, _ -> next_offset end,
        get_log_stream: fn @test_offset, _, @test_opts -> [test_content] end
      )

      %{resp_body: body} =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true&live_sse=true"
        )
        |> call_serve_shape_plug(ctx)

      assert body =~ "data:"
      assert body =~ "test-key"
      assert body =~ "test-value"
    end

    test "streams changes past a chunk boundary in one SSE response, then up-to-date", ctx do
      test_pid = self()
      chunk_boundary = LogOffset.new(Lsn.from_integer(200), 0)
      latest_offset = LogOffset.new(Lsn.from_integer(300), 0)

      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      expect_storage(
        # The request is at the log end, so the response opens as a live stream.
        get_chunk_end_log_offset: fn @test_offset, _ -> nil end,
        get_log_stream: fn @test_offset, @test_offset, @test_opts ->
          send(test_pid, :sse_open)
          []
        end,
        # The first change carries the log past a chunk boundary.
        get_chunk_end_log_offset: fn @test_offset, _ -> chunk_boundary end,
        get_log_stream: fn @test_offset, ^chunk_boundary, @test_opts ->
          [Jason.encode!(%{key: "before-boundary"})]
        end,
        # The rest of the log, read from the boundary.
        get_chunk_end_log_offset: fn ^chunk_boundary, _ -> nil end,
        get_log_stream: fn ^chunk_boundary, ^latest_offset, @test_opts ->
          [Jason.encode!(%{key: "after-boundary"})]
        end
      )

      ctx = Map.put(ctx, :sse_timeout, 500)

      task =
        Task.async(fn ->
          ctx
          |> conn(
            :get,
            %{"table" => "public.users"},
            "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true&live_sse=true"
          )
          |> call_serve_shape_plug(ctx)
        end)

      assert_receive :sse_open, @receive_timeout

      Registry.dispatch(ctx.registry, @test_shape_handle, fn [{pid, ref}] ->
        send(pid, {ref, :new_changes, latest_offset})
      end)

      %{resp_body: body} = Task.await(task)

      assert body =~ "before-boundary"
      assert body =~ "after-boundary"
      assert body =~ ~s("control":"up-to-date")
    end

    test "ends an SSE catch-up at a chunk boundary when the timeout came during it", ctx do
      body = sse_catch_up_body(ctx, fn _pid, ref -> {:sse_timeout, ref} end)

      assert body =~ "before-boundary"
      refute body =~ "after-boundary"
    end

    test "ends an SSE catch-up at a chunk boundary when the server is stopping", ctx do
      body = sse_catch_up_body(ctx, fn pid, _ref -> {:EXIT, pid, :shutdown} end)

      assert body =~ "before-boundary"
      refute body =~ "after-boundary"
    end

    test "sends each change once when a notification is behind the SSE response", ctx do
      test_pid = self()
      latest_offset = LogOffset.new(Lsn.from_integer(300), 0)

      log =
        for lsn <- [200, 250, 300] do
          {LogOffset.new(Lsn.from_integer(lsn), 0), Jason.encode!(%{key: "change-#{lsn}"})}
        end

      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      patch_storage(
        get_chunk_end_log_offset: fn _offset, _ -> nil end,
        get_log_stream: fn since, up_to, @test_opts ->
          if since == @test_offset and up_to == @test_offset, do: send(test_pid, :sse_open)

          for {offset, entry} <- log,
              LogOffset.compare(offset, since) == :gt,
              LogOffset.compare(offset, up_to) != :gt,
              do: entry
        end
      )

      ctx = Map.put(ctx, :sse_timeout, 500)

      task =
        Task.async(fn ->
          ctx
          |> conn(
            :get,
            %{"table" => "public.users"},
            "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true&live_sse=true"
          )
          |> call_serve_shape_plug(ctx)
        end)

      assert_receive :sse_open, @receive_timeout

      # The first notification already covers the whole log. The ones for
      # earlier changes land after it, as when the request's own recheck of
      # the log end beats the consumer's notifications to the mailbox.
      Registry.dispatch(ctx.registry, @test_shape_handle, fn [{pid, ref}] ->
        send(pid, {ref, :new_changes, latest_offset})
        for {offset, _entry} <- log, do: send(pid, {ref, :new_changes, offset})
      end)

      %{resp_body: body} = Task.await(task)

      assert Regex.scan(~r/"key":"([^"]+)"/, body, capture: :all_but_first) ==
               [["change-200"], ["change-250"], ["change-300"]]

      assert length(Regex.scan(~r/"control":"up-to-date"/, body)) == 1
    end

    test "works with deprecated experimental_live_sse=true", ctx do
      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      expect_storage(
        get_chunk_end_log_offset: fn @test_offset, _ -> nil end,
        get_log_stream: fn @test_offset, @test_offset, @test_opts -> [] end
      )

      # Use a short SSE timeout for the test
      ctx = Map.put(ctx, :sse_timeout, 100)

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true&experimental_live_sse=true"
        )
        |> call_serve_shape_plug(ctx)

      assert {"content-type", "text/event-stream"} in conn.resp_headers
      assert {"connection", "keep-alive"} in conn.resp_headers

      cache_control =
        Enum.find_value(conn.resp_headers, fn
          {"cache-control", value} -> value
          _ -> nil
        end)

      assert cache_control =~ "public"
      assert cache_control =~ "max-age="

      assert conn.state == :chunked

      assert conn.status == 200
      assert conn.state == :chunked
    end
  end

  describe "parse_body error handling" do
    setup :with_lsn_tracker

    setup ctx do
      {:via, _, {registry_name, registry_key}} =
        Electric.Shapes.Supervisor.name(ctx.stack_id)

      {:ok, _} = Registry.register(registry_name, registry_key, nil)
      set_status_to_active(ctx)

      patch_storage(for_shape: fn @test_shape_handle, _opts -> @test_opts end)

      :ok
    end

    test "returns 400 for invalid JSON body without crashing", ctx do
      conn =
        Plug.Test.conn(:post, "/?offset=-1", "not valid json")
        |> put_req_header("content-type", "application/json")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400
      assert %{"error" => "Invalid JSON in request body"} = Jason.decode!(conn.resp_body)
    end

    test "returns 400 for non-object JSON body without crashing", ctx do
      conn =
        Plug.Test.conn(:post, "/?offset=-1", Jason.encode!(["an", "array"]))
        |> put_req_header("content-type", "application/json")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400
      assert %{"error" => "Request body must be a JSON object"} = Jason.decode!(conn.resp_body)
    end

    test "returns 413 for oversized body without crashing", ctx do
      Repatch.patch(Plug.Conn, :read_body, fn conn, _opts -> {:more, "partial", conn} end)

      conn =
        Plug.Test.conn(:post, "/?offset=-1", "body")
        |> put_req_header("content-type", "application/json")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 413
      assert %{"error" => "Request body too large"} = Jason.decode!(conn.resp_body)
    end

    test "returns 400 for body read failure without crashing", ctx do
      Repatch.patch(Plug.Conn, :read_body, fn _conn, _opts -> {:error, :timeout} end)

      conn =
        Plug.Test.conn(:post, "/?offset=-1", "body")
        |> put_req_header("content-type", "application/json")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400
      assert %{"error" => "Failed to read request body"} = Jason.decode!(conn.resp_body)
    end
  end

  describe "POST body cannot redefine the shape pinned in the URL" do
    setup :with_lsn_tracker

    setup ctx do
      {:via, _, {registry_name, registry_key}} =
        Electric.Shapes.Supervisor.name(ctx.stack_id)

      {:ok, _} = Registry.register(registry_name, registry_key, nil)
      set_status_to_active(ctx)

      patch_storage(for_shape: fn @test_shape_handle, _opts -> @test_opts end)

      :ok
    end

    # An authorizing proxy pins the shape definition in the query string. Each case
    # pins a definition that cannot validate and offers a valid replacement in the
    # body: the request must still be rejected on the pinned value, proving the body
    # never displaced it.

    test "a body `where` does not replace the where pinned in the URL", ctx do
      conn =
        Plug.Test.conn(
          :post,
          "/?table=public.users&offset=-1&where=" <> URI.encode_www_form("no_such_column = 1"),
          Jason.encode!(%{"subset" => %{}, "where" => "id = 1"})
        )
        |> put_req_header("content-type", "application/json")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400
      assert %{"errors" => %{"where" => [message]}} = Jason.decode!(conn.resp_body)
      assert message =~ "no_such_column"
    end

    test "a body `table` does not replace the table pinned in the URL", ctx do
      conn =
        Plug.Test.conn(
          :post,
          "/?table=public.no_such_table&offset=-1",
          Jason.encode!(%{"where" => "id = 1", "table" => "public.users"})
        )
        |> put_req_header("content-type", "application/json")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400
      assert %{"errors" => %{"table" => [_ | _]}} = Jason.decode!(conn.resp_body)
    end

    test "a body `columns` does not replace the columns pinned in the URL", ctx do
      conn =
        Plug.Test.conn(
          :post,
          "/?table=public.users&offset=-1&columns=" <> URI.encode_www_form("id,no_such_column"),
          Jason.encode!(%{"where" => "id = 1", "columns" => "id,value"})
        )
        |> put_req_header("content-type", "application/json")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400
      assert %{"errors" => %{"columns" => [_ | _]}} = Jason.decode!(conn.resp_body)
    end

    # The body must still be read for the subset parameters it is meant to carry:
    # a subset `where` naming an unknown column is rejected on that column, which it
    # can only be if the body reached subset validation.
    test "a documented subset body still reaches the subset params", ctx do
      conn =
        Plug.Test.conn(
          :post,
          "/?table=public.users&offset=-1&subset__order_by=id&where=" <>
            URI.encode_www_form("id = 1"),
          Jason.encode!(%{"where" => "no_such_column = 1", "subset" => %{"limit" => 10}})
        )
        |> put_req_header("content-type", "application/json")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400
      assert conn.resp_body =~ "no_such_column"
    end

    # The schema ignores a key the subset does not read, so naming it twice is no conflict: the
    # body reaches subset validation and is rejected on its unknown column.
    test "a key the subset does not read may be in both the URL and the body", ctx do
      conn =
        Plug.Test.conn(
          :post,
          "/?table=public.users&offset=-1&subset__order_by=id&subset__note=a",
          Jason.encode!(%{"subset" => %{"where" => "no_such_column = 1", "note" => "b"}})
        )
        |> put_req_header("content-type", "application/json")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 400
      assert conn.resp_body =~ "no_such_column"
      refute conn.resp_body =~ "more than one place"
    end

    # A params object given as a JSON string is decoded after the body, where a repeated key
    # would keep its last value while a proxy that checked the params read the first.
    for {request, method, query, body} <- [
          {"a GET query", :get,
           "&subset__where=" <>
             URI.encode_www_form("id = $1") <>
             "&subset__params=" <> URI.encode_www_form(~s({"1":"1","1":"2"})), nil},
          {"a POST body", :post, "",
           Jason.encode!(%{
             "subset" => %{"where" => "id = $1", "params" => ~s({"1":"1","1":"2"})}
           })}
        ] do
      test "refuses a key repeated in JSON-encoded subset params in #{request}", ctx do
        conn =
          Plug.Test.conn(
            unquote(method),
            "/?table=public.users&offset=-1" <> unquote(query),
            unquote(body)
          )
          |> put_req_header("content-type", "application/json")
          |> call_serve_shape_plug(ctx)

        assert {400,
                %{"errors" => %{"subset" => %{"params" => ["params must not repeat a key"]}}}} =
                 {conn.status, Jason.decode!(conn.resp_body)}
      end
    end
  end

  describe "POST subset parameter sources" do
    setup :with_lsn_tracker

    setup ctx do
      set_status_to_active(ctx)
      :ok
    end

    for {sources, query, body, field} <- [
          {"the URL and flat body", "&subset__limit=1", ~s({"limit":1}), "limit"},
          {"the URL and nested body", "&subset__where=id+%3D+1",
           ~s({"subset":{"where":"id = 2"}}), "where"},
          {"the flat and nested body", "", ~s({"order_by":"id","subset":{"order_by":"id"}}),
           "order_by"},
          {"the URL and nested body for a map field", "&subset__params[1]=1",
           ~s({"subset":{"params":{"2":"2"}}}), "params"}
        ] do
      test "refuses a subset field in #{sources}", ctx do
        conn =
          Plug.Test.conn(:post, "/?offset=-1" <> unquote(query), unquote(body))
          |> call_serve_shape_plug(ctx)

        errors = %{unquote(field) => ["can't be specified in more than one place"]}

        assert {400, %{"errors" => %{"subset" => ^errors}}} =
                 {conn.status, Jason.decode!(conn.resp_body)}
      end
    end

    # Plug reads `subset=a` as a string and `subset[]=a` as a list, neither of which a body
    # subset can be merged into.
    for query <- ["&subset=a", "&subset[]=a"] do
      test "refuses a URL subset that is not an object next to a body subset: #{query}", ctx do
        conn =
          Plug.Test.conn(:post, "/?offset=-1" <> unquote(query), ~s({"limit":1}))
          |> call_serve_shape_plug(ctx)

        assert {400, %{"errors" => %{"subset" => ["is invalid"]}}} =
                 {conn.status, Jason.decode!(conn.resp_body)}
      end
    end

    for {location, body} <- [
          {"top-level object", ~s({"limit":1,"limit":1})},
          {"subset object", ~S({"subset":{"where":"id = 1","wher\u0065":"id = 2"}})},
          {"object in an array", ~s({"ignored":[{"value":1,"value":2}]})}
        ] do
      test "refuses duplicate JSON members in the #{location}", ctx do
        conn =
          Plug.Test.conn(:post, "/?offset=-1", unquote(body))
          |> call_serve_shape_plug(ctx)

        assert {400, %{"error" => "Duplicate JSON member in request body"}} =
                 {conn.status, Jason.decode!(conn.resp_body)}
      end
    end
  end

  describe "a shape parameter given more than once or spelled another way" do
    setup :with_lsn_tracker

    setup ctx do
      {:via, _, {registry_name, registry_key}} =
        Electric.Shapes.Supervisor.name(ctx.stack_id)

      {:ok, _} = Registry.register(registry_name, registry_key, nil)
      set_status_to_active(ctx)

      patch_storage(for_shape: fn @test_shape_handle, _opts -> @test_opts end)

      :ok
    end

    # Plug keeps the last value of a repeated parameter and matches names exactly. A proxy
    # in front may read the first value, match undecoded names regardless of case (nginx) or
    # read names as PHP does. The request is refused rather than read either way.
    for {param, query} <- [
          {"table", "table=public.users&table=public.no_such_table"},
          {"where", "table=public.users&where=id+%3D+1&where=true"},
          {"columns", "table=public.users&columns=id&columns=id%2Cvalue"},
          {"params", "table=public.users&where=id+%3D+%241&params[1]=1&params[1]=2"},
          {"table", "table=public.users&t%61ble=public.no_such_table"},
          {"table", "TABLE=public.no_such_table&table=public.users"},
          {"where", "table=public.users&WHERE=id+%3D+1"},
          {"queryable_columns", "table=public.users&queryable.columns=id"},
          {"subset", "table=public.users&subset__where=id+%3D+1&subset__where=true"},
          {"subset", "table=public.users&subset__where=id+%3D+1&subset[where]=true"},
          {"subset", "table=public.users&subset__where=id+%3D+1&subset[order_by]=id"},
          {"params",
           "table=public.users&where=id+%3D+%241&params=%7B%221%22%3A%221%22%7D&params[1]=2"},
          {"where", "table=public.users&w%68ere=id+%3D+1"},
          {"where", "table=public.users&where=true&%20where=id+%3D+1"},
          {"where", "table=public.users&where=true&where%00=id+%3D+1"},
          {"queryable_columns", "table=public.users&queryable[columns=id"},
          {"params", "table=public.users&where=id+%3D+%241&params[1]=1&params[1]x=2"}
        ] do
      test "is refused: #{query}", ctx do
        conn =
          conn(ctx, :get, nil, "?offset=-1&" <> unquote(query))
          |> call_serve_shape_plug(ctx)

        expected = %{unquote(param) => ["can't be repeated or spelled another way"]}
        assert {400, %{"errors" => ^expected}} = {conn.status, Jason.decode!(conn.resp_body)}
      end
    end

    # The request below reaches where-clause validation, which it can only do if neither
    # the distinct `params[n]` and `subset__*` keys, the escaped brackets clients send, nor
    # the repeated `offset` were refused.
    test "distinct keys and a repeated non-shape parameter are not refused", ctx do
      where = URI.encode_www_form("no_such_column = $1 AND value = $2")
      subset = "subset__where=id+%3D+1&subset__order_by=id&subset__limit=1&subset__params=%7B%7D"

      conn =
        conn(
          ctx,
          :get,
          nil,
          "?table=public.users&offset=-1&offset=-1&where=#{where}&params%5B1%5D=1&params[2]=2&" <>
            subset
        )
        |> call_serve_shape_plug(ctx)

      assert %{"errors" => %{"where" => [message]} = errors} = Jason.decode!(conn.resp_body)
      assert message =~ "no_such_column"
      refute Enum.any?(Map.values(errors), &("can't be repeated or spelled another way" in &1))
    end
  end

  describe "admission of a request carrying a known handle" do
    setup :with_lsn_tracker

    setup ctx do
      {:via, _, {registry_name, registry_key}} =
        Electric.Shapes.Supervisor.name(ctx.stack_id)

      {:ok, _} = Registry.register(registry_name, registry_key, nil)
      set_status_to_active(ctx)

      Repatch.patch(Electric.ShapeCache.ShapeStatus, :has_shape_handle?, fn _stack_id, _handle ->
        true
      end)

      :ok
    end

    defp assert_refused_as_initial(conn) do
      assert conn.status == 503

      assert %{
               "code" => "concurrent_request_limit_exceeded",
               "message" => "Concurrent initial request limit exceeded" <> _
             } = Jason.decode!(conn.resp_body)
    end

    defp refute_shape_loaded do
      Repatch.patch(Electric.Shapes.Api, :load_shape_info, fn _request ->
        flunk("the shape must not be loaded for a refused request")
      end)
    end

    for offset <- ["-1", "now"] do
      @tag max_concurrent_requests: %{initial: 0, existing: 10}
      test "counts against the initial limit at offset #{offset}", ctx do
        refute_shape_loaded()

        conn =
          ctx
          |> conn(
            :get,
            %{"table" => "public.users"},
            "?offset=#{unquote(offset)}&handle=#{@test_shape_handle}"
          )
          |> call_serve_shape_plug(ctx)

        assert_refused_as_initial(conn)
        assert %{initial: 0, existing: 0} == Electric.AdmissionControl.get_current(ctx.stack_id)
      end
    end

    @tag max_concurrent_requests: %{initial: 0, existing: 10}
    test "counts against the initial limit before its body is read", ctx do
      conn =
        Plug.Test.conn(:post, "/?offset=-1&handle=#{@test_shape_handle}", "not valid json")
        |> put_req_header("content-type", "application/json")
        |> call_serve_shape_plug(ctx)

      assert_refused_as_initial(conn)
    end

    @tag max_concurrent_requests: %{initial: 0, existing: 10}
    test "counts against the initial limit when the handle is of another definition", ctx do
      Repatch.patch(Electric.ShapeCache.ShapeStatus, :validate_shape_handle, fn _, _, _ ->
        :error
      end)

      refute_shape_loaded()

      conn =
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{@test_offset}&handle=#{@test_shape_handle}"
        )
        |> call_serve_shape_plug(ctx)

      assert_refused_as_initial(conn)
      assert %{initial: 0, existing: 0} == Electric.AdmissionControl.get_current(ctx.stack_id)
    end

    @tag max_concurrent_requests: %{initial: 0, existing: 10}
    test "keeps its existing permit when the handle is of the requested definition", ctx do
      Repatch.patch(Electric.ShapeCache.ShapeStatus, :validate_shape_handle, fn
        _stack_id, @test_shape_handle, @test_shape -> :ok
      end)

      assert %{initial: 0, existing: 1} ==
               permits_held_at_shape_load(
                 ctx,
                 "?offset=#{@test_offset}&handle=#{@test_shape_handle}"
               )

      assert %{initial: 0, existing: 0} == Electric.AdmissionControl.get_current(ctx.stack_id)
    end

    @tag max_concurrent_requests: %{initial: 1, existing: 10}
    test "moves to an initial permit, released at the end, when the handle is of another definition",
         ctx do
      Repatch.patch(Electric.ShapeCache.ShapeStatus, :validate_shape_handle, fn _, _, _ ->
        :error
      end)

      assert %{initial: 1, existing: 0} ==
               permits_held_at_shape_load(
                 ctx,
                 "?offset=#{@test_offset}&handle=#{@test_shape_handle}"
               )

      assert %{initial: 0, existing: 0} == Electric.AdmissionControl.get_current(ctx.stack_id)
    end

    defp permits_held_at_shape_load(ctx, query_string) do
      test_pid = self()

      Repatch.patch(Electric.Shapes.Api, :load_shape_info, fn _request ->
        send(test_pid, {:permits, Electric.AdmissionControl.get_current(ctx.stack_id)})
        raise RuntimeError, "simulated crash"
      end)

      try do
        ctx
        |> conn(:get, %{"table" => "public.users"}, query_string)
        |> call_serve_shape_plug(ctx)
      catch
        _kind, _reason -> :ok
      end

      assert_received {:permits, permits}
      permits
    end
  end

  describe "admission control release on error" do
    setup :with_lsn_tracker

    setup ctx do
      {:via, _, {registry_name, registry_key}} =
        Electric.Shapes.Supervisor.name(ctx.stack_id)

      {:ok, _} = Registry.register(registry_name, registry_key, nil)
      set_status_to_active(ctx)

      %{plug_opts: build_plug_opts(ctx)}
    end

    test "releases permit when load_shape raises RuntimeError", ctx do
      Repatch.patch(Electric.Shapes.Api, :load_shape_info, fn _request ->
        raise RuntimeError, "simulated crash"
      end)

      call_plug_expecting_crash(ctx)

      assert %{initial: 0, existing: 0} == Electric.AdmissionControl.get_current(ctx.stack_id)
    end

    test "releases permit when load_shape raises DBConnection.ConnectionError", ctx do
      Repatch.patch(Electric.Shapes.Api, :load_shape_info, fn _request ->
        raise DBConnection.ConnectionError, "connection refused"
      end)

      call_plug_expecting_crash(ctx)

      assert %{initial: 0, existing: 0} == Electric.AdmissionControl.get_current(ctx.stack_id)
    end

    test "permit is acquired and properly released when exception occurs during validate_request",
         ctx do
      :ok = Electric.AdmissionControl.try_acquire(ctx.stack_id, :initial, max_concurrent: 1000)
      :ok = Electric.AdmissionControl.try_acquire(ctx.stack_id, :initial, max_concurrent: 1000)
      :ok = Electric.AdmissionControl.try_acquire(ctx.stack_id, :existing, max_concurrent: 1000)

      # check_admission runs first and acquires a permit, then validate_request raises.
      # The after-block in call/2 releases the permit, so counters return to pre-request values.
      Repatch.patch(Electric.Shapes.Api, :validate_params, fn _api, _params ->
        raise RuntimeError, "crash during validation"
      end)

      call_plug_expecting_crash(ctx)

      assert %{initial: 2, existing: 1} == Electric.AdmissionControl.get_current(ctx.stack_id)
    end

    test "releases correct :existing permit when known handle and load_shape raises", ctx do
      # When a request carries a handle that is present in the ETS shape-meta
      # table, check_admission classifies it as :existing. If load_shape then
      # raises, the error handler must release :existing — not :initial.
      Repatch.patch(Electric.ShapeCache.ShapeStatus, :has_shape_handle?, fn _stack_id, _handle ->
        true
      end)

      Repatch.patch(Electric.ShapeCache.ShapeStatus, :validate_shape_handle, fn _, _, _ -> :ok end)

      Repatch.patch(Electric.Shapes.Api, :load_shape_info, fn _request ->
        raise RuntimeError, "simulated crash"
      end)

      try do
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=0_0&handle=#{@test_shape_handle}")
        |> ServeShapePlug.call(ctx.plug_opts)
      catch
        _kind, _reason -> :ok
      end

      assert %{initial: 0, existing: 0} == Electric.AdmissionControl.get_current(ctx.stack_id)
    end

    test "releases correct :initial permit when no handle provided and load_shape raises", ctx do
      # Without a handle, check_admission classifies the request as :initial.
      # Verify the counter returns to zero on exception.
      Repatch.patch(Electric.Shapes.Api, :load_shape_info, fn _request ->
        raise RuntimeError, "simulated crash"
      end)

      call_plug_expecting_crash(ctx)

      assert %{initial: 0, existing: 0} == Electric.AdmissionControl.get_current(ctx.stack_id)
    end

    test "releases the permit of a live request whose process is killed while it waits", ctx do
      # As when an HTTP/2 connection closes: its stream processes are killed
      # with it and never reach the `after` clause in call/2.
      patch_storage(for_shape: fn @test_shape_handle, _opts -> @test_opts end)

      patch_shape_cache(
        resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        has_shape?: fn @test_shape_handle, _opts -> true end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      test_pid = self()

      patch_storage(
        get_chunk_end_log_offset: fn _offset, _ -> nil end,
        get_log_stream: fn _since, _up_to, @test_opts ->
          send(test_pid, :waiting_for_changes)
          []
        end
      )

      task =
        Task.async(fn ->
          ctx
          |> conn(
            :get,
            %{"table" => "public.users"},
            "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true"
          )
          |> ServeShapePlug.call(ctx.plug_opts)
        end)

      Process.unlink(task.pid)
      assert_receive :waiting_for_changes, @receive_timeout
      assert permits_held(ctx.stack_id) == 1

      Process.exit(task.pid, :kill)
      assert {:exit, :killed} = Task.yield(task, @receive_timeout)

      assert_eventually_no_permits(ctx.stack_id)
    end

    defp permits_held(stack_id) do
      %{initial: initial, existing: existing} = Electric.AdmissionControl.get_current(stack_id)
      initial + existing
    end

    defp assert_eventually_no_permits(stack_id, attempts \\ 100) do
      cond do
        permits_held(stack_id) == 0 ->
          :ok

        attempts > 0 ->
          Process.sleep(10)
          assert_eventually_no_permits(stack_id, attempts - 1)

        true ->
          flunk("permits still held: #{inspect(Electric.AdmissionControl.get_current(stack_id))}")
      end
    end

    test "500 response body redacts internal exception details", ctx do
      # The raised message includes content that resembles internal detail
      # (module paths, query text) that must never reach the client.
      Repatch.patch(Electric.Shapes.Api, :load_shape_info, fn _request ->
        raise RuntimeError,
              "(Postgrex.Error) ERROR 42601 at query: SELECT * FROM secret_table"
      end)

      conn =
        try do
          ctx
          |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
          |> ServeShapePlug.call(ctx.plug_opts)
        catch
          :error, %Plug.Conn.WrapperError{conn: conn} -> conn
        end

      assert conn.status == 500
      assert Jason.decode!(conn.resp_body) == %{"error" => "Internal server error"}
      # The leaked detail must not appear anywhere in the response body.
      refute conn.resp_body =~ "Postgrex"
      refute conn.resp_body =~ "secret_table"
      refute conn.resp_body =~ "load_shape_info"
    end

    # The `catch` only fires on the re-raise path (`handle_caught` sees
    # `{:plug_conn, :sent}` in the mailbox and re-raises). For the tests
    # in this describe block the exception is raised before any response is
    # sent, so `handle_errors` sends a 500 and `call/2` returns normally —
    # the `catch` is defensive for generality.
    defp call_plug_expecting_crash(ctx) do
      try do
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
        |> ServeShapePlug.call(ctx.plug_opts)
      catch
        _kind, _reason -> :ok
      end
    end

    test "successful snapshot response releases permit via after-block", ctx do
      # Covers the chunked streaming success path — permit release happens only after
      # Api.Response.send_stream finishes streaming the whole response to the client.
      patch_storage(for_shape: fn @test_shape_handle, _opts -> @test_opts end)

      expect_shape_cache(
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      patch_shape_cache(has_shape?: fn @test_shape_handle, _opts -> true end)

      expect_storage(
        get_chunk_end_log_offset: fn @before_all_offset, _ ->
          @first_offset
        end,
        get_log_stream: fn @before_all_offset, @first_offset, @test_opts ->
          [Jason.encode!(%{key: "log", value: "foo", headers: %{}, offset: @first_offset})]
        end
      )

      conn =
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
        |> ServeShapePlug.call(ctx.plug_opts)

      assert conn.status == 200

      assert %{initial: 0, existing: 0} == Electric.AdmissionControl.get_current(ctx.stack_id)
    end
  end

  # The old register_before_send approach emitted the telemetry span when
  # send_chunked started, not when it finished, losing streaming_bytes_sent
  # and duration. The current try/after in call/2 emits telemetry only after
  # super/2 returns (i.e. after the chunk reduction has drained the body),
  # so the event reflects the full request lifecycle.
  describe "telemetry span covers full chunked response" do
    setup :with_lsn_tracker

    setup ctx do
      {:via, _, {registry_name, registry_key}} =
        Electric.Shapes.Supervisor.name(ctx.stack_id)

      {:ok, _} = Registry.register(registry_name, registry_key, nil)
      set_status_to_active(ctx)

      %{plug_opts: build_plug_opts(ctx)}
    end

    test "emits [:electric, :plug, :serve_shape] after chunked body fully drained", ctx do
      patch_storage(for_shape: fn @test_shape_handle, _opts -> @test_opts end)

      expect_shape_cache(
        get_or_create_shape_handle: fn @test_shape, _stack_id, _opts ->
          {@test_shape_handle, @test_offset}
        end,
        await_snapshot_start: fn @test_shape_handle, _ -> :started end
      )

      patch_shape_cache(has_shape?: fn @test_shape_handle, _opts -> true end)

      expect_storage(
        get_chunk_end_log_offset: fn @before_all_offset, _ ->
          @first_offset
        end,
        get_log_stream: fn @before_all_offset, @first_offset, @test_opts ->
          [Jason.encode!(%{key: "log", value: "foo", headers: %{}, offset: @first_offset})]
        end
      )

      test_pid = self()
      ref = make_ref()
      handler_id = "test-serve-shape-telemetry-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:electric, :plug, :serve_shape],
        fn event, measurements, metadata, _ ->
          send(test_pid, {:serve_shape_telemetry, event, measurements, metadata})
        end,
        nil
      )

      try do
        conn =
          ctx
          |> conn(:get, %{"table" => "public.users"}, "?offset=-1")
          |> ServeShapePlug.call(ctx.plug_opts)

        assert conn.status == 200

        assert_receive {:serve_shape_telemetry, [:electric, :plug, :serve_shape], measurements,
                        metadata},
                       @receive_timeout

        # Request was fully drained before telemetry fired — bytes must reflect
        # the chunk actually streamed, duration must be non-zero, and status
        # must be the final 200 (not pre-response state).
        assert measurements.count == 1
        assert measurements.bytes > 0
        assert measurements.duration > 0
        assert metadata.status == 200
        assert metadata.stack_id == ctx.stack_id
      after
        :telemetry.detach(handler_id)
      end
    end

    test "emits telemetry on error path with final status from handle_errors", ctx do
      Repatch.patch(Electric.Shapes.Api, :load_shape_info, fn _request ->
        raise RuntimeError, "simulated crash"
      end)

      test_pid = self()
      ref = make_ref()
      handler_id = "test-serve-shape-error-telemetry-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:electric, :plug, :serve_shape],
        fn event, measurements, metadata, _ ->
          send(test_pid, {:serve_shape_telemetry, event, measurements, metadata})
        end,
        nil
      )

      try do
        call_plug_expecting_crash(ctx)

        assert_receive {:serve_shape_telemetry, [:electric, :plug, :serve_shape], measurements,
                        metadata},
                       @receive_timeout

        # Error path: handle_errors sends 500, try body returns the sent conn,
        # emit_shape_telemetry sees the final status before after pops the span.
        assert measurements.count == 1
        assert measurements.duration > 0
        assert metadata.status == 500
        assert metadata.stack_id == ctx.stack_id
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  describe "stack not ready" do
    test "returns 503", ctx do
      conn =
        ctx
        |> conn(:get, %{"table" => "public.users"}, "?offset=-1&replica=full")
        |> call_serve_shape_plug(ctx)

      assert conn.status == 503

      assert Jason.decode!(conn.resp_body) == %{
               "message" => "Timeout waiting for Postgres lock acquisition"
             }
    end

    @tag stack_ready_timeout: 5000
    test "waits until stack ready and proceeds", ctx do
      conn_task =
        Task.async(fn ->
          ctx
          |> conn(:get, %{"table" => "public.users", "columns" => "id,invalid"}, "?offset=-1")
          |> call_serve_shape_plug(ctx)
        end)

      set_status_to_active(ctx)

      conn = Task.await(conn_task)

      assert conn.status == 400
      assert get_resp_header(conn, "cache-control") == ["no-store"]
    end
  end

  # An SSE response whose first change carries the log past a chunk boundary. `stop` builds a
  # message that reaches the request right behind the change notice, so while it sends the
  # segment up to the boundary.
  defp sse_catch_up_body(ctx, stop) do
    test_pid = self()
    chunk_boundary = LogOffset.new(Lsn.from_integer(200), 0)
    latest_offset = LogOffset.new(Lsn.from_integer(300), 0)

    patch_shape_cache(
      resolve_shape_handle: fn @test_shape_handle, @test_shape, _stack_id, _opts ->
        {@test_shape_handle, @test_offset}
      end,
      has_shape?: fn @test_shape_handle, _opts -> true end,
      await_snapshot_start: fn @test_shape_handle, _ -> :started end
    )

    patch_storage(
      get_chunk_end_log_offset: fn
        # At the log end when the response opens, past a boundary once the change is in.
        @test_offset, _ -> if Process.put(:sse_open?, true), do: chunk_boundary
        ^chunk_boundary, _ -> nil
      end,
      get_log_stream: fn
        @test_offset, @test_offset, @test_opts ->
          send(test_pid, :sse_open)
          []

        @test_offset, ^chunk_boundary, @test_opts ->
          [Jason.encode!(%{key: "before-boundary"})]

        ^chunk_boundary, ^latest_offset, @test_opts ->
          [Jason.encode!(%{key: "after-boundary"})]
      end
    )

    ctx = Map.put(ctx, :sse_timeout, 5_000)

    task =
      Task.async(fn ->
        ctx
        |> conn(
          :get,
          %{"table" => "public.users"},
          "?offset=#{@test_offset}&handle=#{@test_shape_handle}&live=true&live_sse=true"
        )
        |> call_serve_shape_plug(ctx)
      end)

    assert_receive :sse_open, @receive_timeout

    Registry.dispatch(ctx.registry, @test_shape_handle, fn [{pid, ref}] ->
      send(pid, {ref, :new_changes, latest_offset})
      send(pid, stop.(pid, ref))
    end)

    %{resp_body: body} = Task.await(task)
    body
  end

  defp get_shape_with_etag(ctx, offset, extra_query_params \\ []) do
    query_str =
      URI.encode_query([offset: offset, handle: @test_shape_handle] ++ extra_query_params)

    ctx
    |> conn(:get, %{"table" => "public.users"}, "?" <> query_str)
    |> put_req_header(
      "if-none-match",
      ~s("#{@test_shape_handle}:#{offset}:#{@test_offset}")
    )
    |> call_serve_shape_plug(ctx)
  end

  defp max_age(ctx), do: Access.get(ctx, :max_age, 60)
  defp stale_age(ctx), do: Access.get(ctx, :stale_age, 300)
  defp long_poll_timeout(ctx), do: Access.get(ctx, :long_poll_timeout, 20_000)
  defp sse_timeout(ctx), do: Access.get(ctx, :sse_timeout, 60_000)
end
