defmodule Electric.ShapeCache.ShapeStatus.ShapeDb.MigratorOptimizeTest do
  # not async: saturates the single-worker write pool for the whole checkout deadline
  use ExUnit.Case, async: false

  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Connection

  import Support.ComponentSetup

  @moduletag :tmp_dir

  setup [:with_stack_id_from_test, :with_shape_db]

  # The write pool is a single serialised worker, so this starves every other writer for `duration`.
  # That is precisely what a large batch of shape removals does to the scheduled optimize.
  defp occupy_write_connection(stack_id, duration) do
    test = self()

    task =
      Task.async(fn ->
        Connection.checkout_write!(stack_id, :test_occupy, fn _conn ->
          send(test, :occupied)
          Process.sleep(duration)
          :ok
        end)
      end)

    assert_receive :occupied, 5_000
    task
  end

  defp migrator_pid(stack_id) do
    stack_id
    |> ShapeDb.Supervisor.name()
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {ShapeDb.Migrator, pid, _type, _modules} when is_pid(pid) -> pid
      _child -> false
    end)
  end

  @tag timeout: 60_000
  test "a busy write pool does not make the scheduled optimize fatal", ctx do
    # Arrange
    supervisor = GenServer.whereis(ShapeDb.Supervisor.name(ctx.stack_id))
    migrator = migrator_pid(ctx.stack_id)
    assert is_pid(supervisor)
    assert is_pid(migrator)

    migrator_ref = Process.monitor(migrator)
    supervisor_ref = Process.monitor(supervisor)

    # Hold the single write connection well past the 10s deadline the migrator's checkout inherits.
    task = occupy_write_connection(ctx.stack_id, 14_000)

    # Act
    send(migrator, :optimize)

    # Assert -- `PRAGMA optimize` is advisory maintenance, so failing to acquire the connection must
    # be survivable. NimblePool signals the expired deadline with an `exit`, which unhandled kills the
    # migrator; the supervisor above is `:one_for_all` with `max_restarts: 0`, so that first crash
    # takes the shape db, the WriteBuffer and the ETS caches down with it.
    refute_receive {:DOWN, ^migrator_ref, :process, _pid, _reason}, 13_000
    refute_receive {:DOWN, ^supervisor_ref, :process, _pid, _reason}, 100

    assert Process.alive?(migrator)
    assert Process.alive?(supervisor)

    Task.await(task, 20_000)
  end

  @tag timeout: 60_000
  test "the optimize timer keeps running after a failed attempt", ctx do
    # Arrange
    migrator = migrator_pid(ctx.stack_id)
    migrator_ref = Process.monitor(migrator)
    task = occupy_write_connection(ctx.stack_id, 14_000)

    # Act -- fail one attempt, then let the pool free up and drive another.
    send(migrator, :optimize)
    refute_receive {:DOWN, ^migrator_ref, :process, _pid, _reason}, 13_000
    Task.await(task, 20_000)
    send(migrator, :optimize)

    # Assert -- a skipped run must leave the migrator able to optimize again, not wedged.
    refute_receive {:DOWN, ^migrator_ref, :process, _pid, _reason}, 2_000
    assert Process.alive?(migrator)
  end
end
