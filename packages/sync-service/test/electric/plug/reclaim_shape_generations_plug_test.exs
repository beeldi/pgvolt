defmodule Electric.Plug.ReclaimShapeGenerationsPlugTest do
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import ExUnit.CaptureLog
  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Plug.Test

  alias Electric.Plug.Router
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.WriteBuffer
  alias Electric.StatusMonitor

  @moduletag :tmp_dir
  @moduletag secret: "reclaim-test-secret"

  setup [:with_unique_db, :with_basic_tables, :with_sql_execute]
  setup :with_complete_stack

  setup ctx do
    :ok = StatusMonitor.wait_until_active(ctx.stack_id, timeout: 5_000)
    %{opts: Router.init(build_router_opts(ctx))}
  end

  test "removes the shapes of collectable generations and keeps the newer ones", ctx do
    old = create_shape(ctx, "a", [1])
    kept_previous = create_shape(ctx, "b", [2])
    live = create_shape(ctx, "c", [3])
    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    assert {200, first} = reclaim(ctx, current: 3, newest_collectable: 1)

    assert %{"scanned" => 3, "matched" => 1, "removed" => 1, "outcome" => "ok"} = first
    refute ShapeStatus.has_shape_handle?(ctx.stack_id, old)
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, kept_previous)
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, live)

    assert {200, %{"scanned" => 0, "next_after" => nil}} =
             reclaim(ctx, current: 3, newest_collectable: 1, after: first["next_after"])
  end

  test "removes nothing when the matcher selects a shape of the current generation", ctx do
    old = create_shape(ctx, "a", [1])
    straddling = create_shape(ctx, "b", [3, 1])
    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    assert {409, %{"live_handles" => [^straddling]}} =
             reclaim(ctx, current: 3, newest_collectable: 1)

    assert ShapeStatus.has_shape_handle?(ctx.stack_id, old)
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, straddling)
  end

  test "a call out of budget stops after a chunk and the next one resumes after it", ctx do
    handles = for i <- 1..30, do: create_shape(ctx, "v#{i}", [1])
    :ok = WriteBuffer.flush_sync(ctx.stack_id)
    [first_chunk, rest] = handles |> Enum.sort() |> Enum.chunk_every(25)

    assert {200, first} = reclaim(ctx, current: 2, newest_collectable: 1, budget_ms: 1)

    assert %{"matched" => 30, "removed" => 25, "next_after" => next_after} = first
    assert next_after == List.last(first_chunk)
    assert Enum.all?(rest, &ShapeStatus.has_shape_handle?(ctx.stack_id, &1))

    assert {200, %{"matched" => 5, "removed" => 5}} =
             reclaim(ctx, current: 2, newest_collectable: 1, after: next_after)

    refute Enum.any?(handles, &ShapeStatus.has_shape_handle?(ctx.stack_id, &1))
  end

  test "selects a shape only by the token its filter ends with", ctx do
    quoted = create_shape_where(ctx, "'prefix ''shape-rotation:db1' = value")

    inner =
      create_shape_where(
        ctx,
        "value = 'a' AND 'shape-rotation:db1' = 'shape-rotation:db1' AND value <> 'b'"
      )

    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    assert {200, %{"matched" => 0, "removed" => 0}} =
             reclaim(ctx, current: 3, newest_collectable: 1)

    assert ShapeStatus.has_shape_handle?(ctx.stack_id, quoted)
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, inner)
  end

  @tag with_sql: [
         "CREATE TABLE parent (id INT PRIMARY KEY, value TEXT NOT NULL)",
         "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT NOT NULL REFERENCES parent(id))",
         "INSERT INTO parent (id, value) VALUES (1, 'p')",
         "INSERT INTO child (id, parent_id) VALUES (1, 1)"
       ]
  test "keeps a dependency another shape still reads, whatever token it ends with", ctx do
    live =
      create_shape_where(
        ctx,
        "parent_id IN (SELECT id FROM parent WHERE value = 'p'" <>
          " AND 'shape-rotation:db1' = 'shape-rotation:db1')" <>
          " AND 'shape-rotation:db3' = 'shape-rotation:db3'",
        "child"
      )

    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    assert {409, %{"live_handles" => [dependency]}} =
             reclaim(ctx, current: 3, newest_collectable: 1)

    assert dependency != live
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, dependency)
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, live)
  end

  test "answers 503 without logging the bearer when the store vanishes under the read", ctx do
    create_shape(ctx, "a", [1])
    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    Repatch.patch(WriteBuffer, :tombstoned_handles, fn _stack_id ->
      raise ArgumentError, "the table identifier does not refer to an existing ETS table"
    end)

    log = capture_log(fn -> assert {503, _} = reclaim(ctx, current: 3, newest_collectable: 1) end)

    refute log =~ ctx.secret
  end

  test "keeps every removal whole when one exits, and hands back no cursor", ctx do
    [first, second] =
      ["a", "b"] |> Enum.map(&create_shape(ctx, &1, [1])) |> Enum.sort()

    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    Repatch.patch(ShapeCleaner, :remove_shapes, fn stack_id, handles, reason ->
      if handles == [second],
        do: exit(:checkout_timeout),
        else:
          Repatch.real(Electric.ShapeCache.ShapeCleaner.remove_shapes(stack_id, handles, reason))
    end)

    assert {200, %{"removed" => 1, "outcome" => "aborted", "next_after" => nil}} =
             reclaim(ctx, current: 3, newest_collectable: 1)

    refute ShapeStatus.has_shape_handle?(ctx.stack_id, first)
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, second)
  end

  test "refuses a collectable generation that is not below the current one", ctx do
    live = create_shape(ctx, "a", [3])
    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    assert {400, _} = reclaim(ctx, current: 3, newest_collectable: 3)
    assert {400, _} = reclaim(ctx, current: 3, newest_collectable: 0)
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, live)
  end

  test "refuses a budget above two minutes", ctx do
    assert {400, _} = reclaim(ctx, current: 3, newest_collectable: 1, budget_ms: 120_001)
  end

  test "accepts a custom token_prefix and rejects invalid ones", ctx do
    custom = create_shape_where(ctx, "value = 'a' AND 'gen:1' = 'gen:1'")
    default = create_shape(ctx, "b", [1])
    :ok = WriteBuffer.flush_sync(ctx.stack_id)

    assert {400, _} = reclaim(ctx, current: 3, newest_collectable: 1, token_prefix: "")
    assert {400, _} = reclaim(ctx, current: 3, newest_collectable: 1, token_prefix: "bad'quote")

    assert {200, %{"matched" => 1, "removed" => 1}} =
             reclaim(ctx, current: 3, newest_collectable: 1, token_prefix: "gen:")

    refute ShapeStatus.has_shape_handle?(ctx.stack_id, custom)
    assert ShapeStatus.has_shape_handle?(ctx.stack_id, default)
  end

  defp create_shape(ctx, value, generations) do
    tokens =
      Enum.map_join(generations, fn generation ->
        " AND 'shape-rotation:db#{generation}' = 'shape-rotation:db#{generation}'"
      end)

    create_shape_where(ctx, "value = '#{value}'" <> tokens)
  end

  defp create_shape_where(ctx, where, table \\ "items") do
    query =
      URI.encode_query(%{
        "table" => table,
        "offset" => "-1",
        "where" => where,
        "secret" => ctx.secret
      })

    conn = conn("GET", "/v1/shape?" <> query) |> Router.call(ctx.opts)
    assert %{status: 200} = conn
    [handle] = Plug.Conn.get_resp_header(conn, "electric-handle")
    handle
  end

  defp reclaim(ctx, opts) do
    base = %{
      "current_generation" => Keyword.fetch!(opts, :current),
      "newest_collectable_generation" => Keyword.fetch!(opts, :newest_collectable),
      "budget_ms" => Keyword.get(opts, :budget_ms, 30_000),
      "after" => Keyword.get(opts, :after, "")
    }

    body =
      case Keyword.fetch(opts, :token_prefix) do
        {:ok, prefix} -> Map.put(base, "token_prefix", prefix)
        :error -> base
      end
      |> Jason.encode!()

    conn =
      conn("POST", "/v1/admin/shape-generations/reclaim", body)
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Plug.Conn.put_req_header("authorization", "Bearer #{ctx.secret}")
      |> Router.call(ctx.opts)

    {conn.status, Jason.decode!(conn.resp_body)}
  end
end
