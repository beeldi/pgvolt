defmodule Electric.Replication.ShapeLogCollectorSharedFramingTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Support.ComponentSetup

  alias Electric.Replication.ShapeLogCollector

  @moduletag :tmp_dir

  setup [
    :with_stack_id_from_test,
    :with_in_memory_storage,
    :with_shape_status,
    :with_lsn_tracker,
    :with_noop_publication_manager,
    :with_persistent_kv
  ]

  @inspector Support.StubInspector.new(
               tables: [{1234, {"public", "test_table"}}],
               columns: [%{name: "id", type: "int8", pk_position: 0}]
             )

  # Shared fragment blocks hand the storage backend a framed-block struct only pure file storage
  # can decode; on in-memory storage the feature is broken by construction, so booting with it on
  # must disable it (and say so) rather than let a framed change reach a backend with no clause.
  test "disables shared fragment blocks when the storage backend does not support them", ctx do
    Process.flag(:trap_exit, true)

    opts = [
      stack_id: ctx.stack_id,
      inspector: @inspector,
      persistent_kv: ctx.persistent_kv,
      shared_conversion: true,
      shared_fragment_blocks: true
    ]

    log =
      capture_log(fn ->
        case ShapeLogCollector.start_link(opts) do
          {:ok, pid} -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1000)
          _other -> :ok
        end
      end)

    assert log =~ "Shared fragment blocks requested but storage does not support them"
  end
end
