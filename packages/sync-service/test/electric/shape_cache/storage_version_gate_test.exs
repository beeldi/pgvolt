defmodule Electric.ShapeCache.StorageVersionGateTest do
  use ExUnit.Case, async: true

  import Support.ComponentSetup

  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.PureFileStorage.PayloadArena
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
    %{base_opts: base_opts, opts: PureFileStorage.for_shape(@shape_handle, base_opts)}
  end

  defp write_pre_arena_shape(opts) do
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)

    writer =
      PureFileStorage.append_to_log!(
        [{LogOffset.new(10, 0), "test_key", :insert, ~S|{"txn":"a"}|}],
        writer
      )

    PureFileStorage.terminate(writer)
  end

  defp reboot(opts) do
    opts |> PureFileStorage.init_writer!(@shape) |> PureFileStorage.terminate()
  end

  defp log(opts) do
    PureFileStorage.get_log_stream(LogOffset.new(0, 0), LogOffset.last(), opts) |> Enum.to_list()
  end

  defp version_on_disk(opts) do
    opts
    |> PureFileStorage.shape_data_dir(["metadata"])
    |> Path.join("version.bin")
    |> File.read!()
    |> :erlang.binary_to_term()
  end

  test "a ref-free shape log survives the stack's first arena generation", %{
    opts: opts,
    base_opts: base_opts
  } do
    # Arrange -- a shape written before the arena existed: version 1, no ref entries anywhere in it.
    write_pre_arena_shape(opts)
    assert 1 == version_on_disk(opts)

    # Act -- an unrelated shape's fan-out class opens the stack's first arena generation, then
    # this shape's writer restarts.
    PayloadArena.open_writer!(base_opts.base_path)
    reboot(opts)

    # Assert
    assert [~S|{"txn":"a"}|] == log(opts),
           "a log holding no arena refs was discarded because a different shape wrote one"
  end

  test "a shape reachable by an arena-era reader records the version its refs require", %{
    opts: opts,
    base_opts: base_opts
  } do
    # Arrange
    write_pre_arena_shape(opts)

    # Act
    PayloadArena.open_writer!(base_opts.base_path)
    reboot(opts)

    # Assert -- the marker is what makes a rollback discard this shape instead of mishandling a
    # ref, so it has to land before the writer can emit one. A pre-arena image would serve ref
    # bytes as JSON; the image that predates checksums crashes on the wider ref.
    assert 3 == version_on_disk(opts)
  end
end
