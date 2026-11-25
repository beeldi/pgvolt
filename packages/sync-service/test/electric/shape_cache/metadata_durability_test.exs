defmodule Electric.ShapeCache.MetadataDurabilityTest do
  use ExUnit.Case, async: true

  import Support.ComponentSetup

  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir
  setup [:with_stack_id_from_test, :with_async_deleter, :with_pure_file_storage, :for_shape]

  @shape_handle "the-shape-handle"
  @shape %Shape{
    root_table: {"public", "items"},
    root_table_id: 1,
    root_pk: ["id"],
    selected_columns: ["id"],
    explicitly_selected_columns: ["id"],
    where:
      Electric.Replication.Eval.Parser.parse_and_validate_expression!("id != '1'",
        refs: %{["id"] => :text}
      )
  }

  defp for_shape(%{storage: {_mod, base_opts}}) do
    %{opts: PureFileStorage.for_shape(@shape_handle, base_opts)}
  end

  defp initialise_shape(opts) do
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)
    writer
  end

  # SCOPE -- read before extending: nothing here proves the bytes reached the platter. `sync_dir!/1`
  # raises on failure, so this proves the real durable path is taken and works against the real
  # filesystem; proving the fsync itself needs syscall tracing or a crash-consistency fault injector,
  # neither of which runs in ExUnit. Do not relabel this as crash-durability coverage.
  test "the durable claim path runs against the real filesystem", %{opts: opts} do
    # Arrange
    writer = initialise_shape(opts)

    writer =
      PureFileStorage.append_to_log!(
        [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"txn":"a"}|}],
        writer
      )

    PureFileStorage.terminate(writer)

    first = LogOffset.new(10, 0)
    second = LogOffset.new(11, 0)

    # Act -- a changed offset takes the durable branch; sync_dir!/1 raises if the directory sync
    # is unavailable on this platform or filesystem.
    PureFileStorage.update_global_persistence_information(opts, second, second, second, first)

    # Assert
    claim_path =
      Path.join(
        PureFileStorage.shape_data_dir(opts, ["metadata"]),
        "last_persisted_txn_offset.bin"
      )

    assert File.exists?(claim_path)
    assert second == claim_path |> File.read!() |> :erlang.binary_to_term()

    refute File.exists?(claim_path <> ".tmp"),
           "the tmp file must be renamed away, not left beside the claim"
  end

  test "a recovered writer keeps every transaction the claim covers", %{opts: opts} do
    # Arrange
    writer = initialise_shape(opts)

    writer =
      PureFileStorage.append_to_log!(
        [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"txn":"a"}|}],
        writer
      )

    writer =
      PureFileStorage.append_to_log!(
        [{LogOffset.new(11, 0), "test_key", :insert, ~S|{"txn":"b"}|}],
        writer
      )

    PureFileStorage.terminate(writer)

    # Act -- recovery re-reads the claim and trims the log to it.
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.terminate(writer)

    # Assert
    assert [~S|{"txn":"a"}|, ~S|{"txn":"b"}|] ==
             PureFileStorage.get_log_stream(LogOffset.new(0, 0), LogOffset.last(), opts)
             |> Enum.to_list()
  end
end
