defmodule Electric.Shapes.RouterWorkerTest do
  @moduledoc """
  The shard worker is exercised through the collector everywhere else, which covers what the
  collector can produce. This covers what it must survive: a routing decision naming a shape the
  worker has no struct for.

  That matters out of proportion to how likely it is. Workers are `start_link`ed from the
  collector, whose own supervisor is `max_restarts: 0`, so anything fatal here restarts the entire
  shape subsystem — while the correct answer, converting the change in the consumer instead, is
  already the module's fallback for a preparation that fails any other way.
  """
  use ExUnit.Case, async: false

  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes.Commit
  alias Electric.Replication.Changes.PreparedChange
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.LogOffset
  alias Electric.Shapes.ConsumerRegistry
  alias Electric.Shapes.RouterWorker
  alias Electric.Telemetry.OpenTelemetry

  import ExUnit.CaptureLog, only: [with_log: 1]
  import Support.ComponentSetup, only: [with_stack_id_from_test: 1]

  import Support.EmployeesFixtures,
    only: [inspector: 1, insert: 1, relation: 0, shape!: 2]

  @inspector inspector(~w[id firmId divisionId note])
  @registered "registered-shape"
  @unregistered "unregistered-shape"

  defmodule Consumer do
    @moduledoc false
    use GenServer

    def init({parent, handle}), do: {:ok, {parent, handle}}

    def handle_call(message, _from, {parent, handle} = state) do
      send(parent, {:published, handle, message})
      {:reply, :ok, state}
    end
  end

  setup :with_stack_id_from_test

  setup %{stack_id: stack_id} do
    {:ok, registry_state} = ConsumerRegistry.new(stack_id)
    parent = self()

    # Patched before the worker starts: publishing happens in the worker's own process, and its
    # `init` is what carries the test process's mocks over.
    Repatch.patch(
      Electric.ShapeCache,
      :start_consumer_for_handle,
      [mode: :shared],
      fn handle, ^stack_id, _opts ->
        GenServer.start_link(Consumer, {parent, handle},
          name: ConsumerRegistry.name(stack_id, handle)
        )
      end
    )

    # One shard, so `phash2(handle, 1) == 0` owns every handle and ownership never silently
    # excludes the shape under test.
    # Started from the test process rather than supervised: the worker's `init` reaches the test
    # process's mocks by walking `$ancestors` to its root, and under `start_supervised!` that root
    # is ExUnit's supervisor instead.
    {:ok, pid} = RouterWorker.start_link(stack_id, 0, 1, registry_state, [])
    Repatch.allow(self(), pid)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

    %{worker: pid, table: :ets.new(:scatter, [:public])}
  end

  defp shape, do: shape!(@inspector, where: ~s|"firmId" = 'F0'|)

  defp change,
    do: insert(%{"id" => "eq-1", "firmId" => "F0", "divisionId" => "D0", "note" => "n"})

  defp fragment(changes) do
    %TransactionFragment{
      xid: 100,
      has_begin?: true,
      commit: %Commit{commit_timestamp: ~U[2024-01-01 00:00:00Z]},
      changes: changes,
      affected_relations: MapSet.new([relation()]),
      change_count: length(changes),
      last_log_offset: LogOffset.new(Lsn.from_integer(1000), 1)
    }
  end

  defp route(%{worker: worker, table: table}, handles, full_xid \\ nil) do
    change = change()
    decisions = [{change, [{:prepare, handles}], []}]
    key = :erlang.unique_integer()
    transaction = %{fragment([change]) | full_xid: full_xid}
    true = :ets.insert(table, {key, {:fragment, transaction, decisions}})

    ref = RouterWorker.scatter(worker, {table, key}, OpenTelemetry.get_current_context())
    RouterWorker.gather([ref], 5_000)
  end

  test "a decision naming a shape the worker never registered routes it unprepared", ctx do
    {affected, _undeliverable, _delivered} = route(ctx, [@unregistered])

    assert Process.alive?(ctx.worker)
    assert MapSet.member?(affected, @unregistered)
  end

  test "a decision naming a registered shape is prepared on the worker", ctx do
    RouterWorker.add_shape(ctx.worker, @registered, shape())

    {affected, _undeliverable, _delivered} = route(ctx, [@registered])

    assert Process.alive?(ctx.worker)
    assert MapSet.member?(affected, @registered)
  end

  test "a registered shape whose parsed where tree was dropped is still prepared", ctx do
    # worker ever sees the change, so `Shape.convert_routed_change/2` reads `where` only as the
    # nil/not-nil distinction that picks a clause and the parsed tree is dead weight in the copy.
    shape = shape()
    RouterWorker.add_shape(ctx.worker, @registered, %{shape | where: %{shape.where | eval: nil}})

    {{affected, _undeliverable, _delivered}, log} = with_log(fn -> route(ctx, [@registered]) end)

    # deliver the same change and hide a new dependency on the tree behind a warning.
    assert MapSet.member?(affected, @registered)
    refute log =~ "Shared conversion failed"
  end

  test "deferred encoding preserves full transaction positions", ctx do
    shape = %{shape() | source_positions: :full_xid, source_generation: "generation-a"}
    RouterWorker.add_shape(ctx.worker, @registered, shape)

    route(ctx, [@registered], 4_294_967_396)

    assert_receive {:published, @registered,
                    {:handle_event, %TransactionFragment{} = transaction, _context}}

    assert transaction.full_xid == 4_294_967_396
    assert [%PreparedChange{log_items: [{_, _, _, json}]}] = transaction.changes
    assert %{"headers" => %{"txids8" => ["4294967396"]}} = Jason.decode!(json)
  end

  test "a removed shape stops being routed", ctx do
    RouterWorker.add_shape(ctx.worker, @registered, shape())
    :ok = RouterWorker.remove_shape(ctx.worker, @registered)

    {affected, _undeliverable, _delivered} = route(ctx, [@registered])

    assert Process.alive?(ctx.worker)
    assert MapSet.member?(affected, @registered)
  end

  test "an unexpected message is logged instead of crashing the worker", ctx do
    # catch-all, and the worker is linked to a max_restarts: 0 collector.
    send(ctx.worker, {:unexpected, make_ref()})

    # message and stayed up.
    :ok = RouterWorker.remove_shape(ctx.worker, @unregistered)
    assert Process.alive?(ctx.worker)
  end
end
