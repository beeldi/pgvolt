defmodule Electric.ShapeCache.ShapeStatus.ShapeDb.MigratorOwnershipTest do
  # not async: starts per-stack named processes and mutates tmp_dir files
  use ExUnit.Case, async: false

  alias Electric.ShapeCache.ShapeStatus.ShapeDb

  import Support.ComponentSetup

  @moduletag :tmp_dir

  setup [:with_stack_id_from_test]

  # A superseded catalog left by an earlier build is only this instance's to delete once it owns the
  # volume — the stack goes active, holding the Postgres advisory lock the connection subsystem takes
  # after the ShapeDb boots. A boot that has not yet won the lock (a rolling deploy's new instance)
  # must leave the previous instance's catalog alone, or it deletes a still-running instance's data.
  test "a boot that has not won the ownership lock leaves a superseded catalog untouched", ctx do
    # active, standing in for an instance that has not acquired the lock. Plant a superseded catalog
    # under the name an earlier build would have written.
    start_supervised!({Electric.StatusMonitor, stack_id: ctx.stack_id})

    {:ok, path} = ShapeDb.Connection.db_path(storage_dir: ctx.tmp_dir)
    dir = Path.dirname(path)
    File.mkdir_p!(dir)
    superseded = Path.join(dir, "v1_1@otp-1.sqlite")
    File.write!(superseded, "an earlier build's catalog")

    {:ok, _pid} =
      start_supervised(
        {Electric.ShapeCache.ShapeStatus.ShapeDb.Supervisor,
         [
           stack_id: ctx.stack_id,
           shape_db_opts: [
             storage_dir: ctx.tmp_dir,
             manual_flush_only: true,
             read_pool_size: 1,
             # bound the migrator's wait so the never-active stack skips the discard promptly
             discard_superseded_wait_timeout: 200
           ]
         ]},
        id: "shape_db"
      )

    # The migrator defers the discard to a continue that waits for the stack to go active; it never
    # does, so give the bounded wait time to expire and the discard to be skipped before asserting.
    Process.sleep(400)

    assert File.exists?(superseded),
           "a booting instance discarded a superseded catalog before it owned the volume"
  end
end
