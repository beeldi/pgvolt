defmodule Electric.ShapeCache.CompactionFilesTest do
  use ExUnit.Case, async: true

  import Support.ComponentSetup
  import Support.TestUtils, only: [changes_to_log_items: 1]

  alias Electric.Replication.Changes
  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.Storage
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir
  # Tiny chunks so a handful of updates yields enough complete chunks to compact.
  @moduletag with_pure_file_storage_opts: [chunk_bytes_threshold: 10, flush_period: 1000]
  setup [:with_stack_id_from_test, :with_async_deleter, :with_pure_file_storage, :for_shape]

  @shape_handle "the-shape-handle"
  @shape %Shape{
    root_table: {"public", "test_table"},
    root_table_id: 1,
    root_pk: ["id"],
    selected_columns: ["id", "name"],
    explicitly_selected_columns: ["id", "name"],
    where: nil
  }

  defp for_shape(%{storage: {_mod, base_opts}}) do
    %{base_opts: base_opts, opts: PureFileStorage.for_shape(@shape_handle, base_opts)}
  end

  defp start_shape(opts) do
    writer = PureFileStorage.init_writer!(opts, @shape)
    PureFileStorage.set_pg_snapshot(%{xmin: 1, xmax: 10}, opts)
    PureFileStorage.mark_snapshot_as_started(opts)
    PureFileStorage.make_new_snapshot!([], opts)
    writer
  end

  defp updates(range) do
    Enum.map(range, fn i ->
      %Changes.UpdatedRecord{
        relation: {"public", "test_table"},
        old_record: %{"id" => "sameid", "name" => "Test#{i - 1}"},
        record: %{"id" => "sameid", "name" => "Test#{i}"},
        log_offset: LogOffset.new(i, 0),
        changed_columns: MapSet.new(["name"])
      }
    end)
    |> changes_to_log_items()
  end

  defp compact_once(opts, writer, range) do
    writer = PureFileStorage.append_to_log!(updates(range), writer)
    PureFileStorage.terminate(writer)

    assert :ok = PureFileStorage.compact(opts, 0)
    assert_receive {Storage, {mod, fun, args}}, 5_000

    writer = PureFileStorage.init_writer!(opts, @shape)
    apply(mod, fun, [writer | args])
  end

  defp compacted_logs(opts) do
    opts
    |> PureFileStorage.shape_data_dir(["log"])
    |> File.ls!()
    |> Enum.filter(&(&1 =~ ~r/^log\.compacted\..*\.jsonfile\.bin$/))
    |> Enum.sort()
  end

  # Cleanup runs in a supervised Task, so the count settles shortly after the compaction message is
  # applied. Polling to a deadline returns immediately once cleanup works and reports the real state
  # when it does not; sleeping a fixed span would be both slower and flakier.
  defp compacted_logs_settling_to(
         opts,
         expected,
         remaining_ms \\ ExUnit.configuration()[:assert_receive_timeout]
       )

  defp compacted_logs_settling_to(opts, _expected, remaining_ms) when remaining_ms <= 0,
    do: compacted_logs(opts)

  defp compacted_logs_settling_to(opts, expected, remaining_ms) do
    case compacted_logs(opts) do
      logs when length(logs) == expected ->
        logs

      _ ->
        Process.sleep(20)
        compacted_logs_settling_to(opts, expected, remaining_ms - 20)
    end
  end

  # A suffix equal to one still on disk makes the copy destination alias its own source, and
  # open-for-write truncates. Forcing that equality means two compactions inside one millisecond,
  # which no test can do deterministically, so this asserts the property that makes equality
  # impossible: the suffix only ever moves forward. A clock-derived name does not, and that is
  # deterministic to set up.
  test "a new latest suffix is strictly past the one it replaces", %{opts: opts} do
    # Arrange
    writer = start_shape(opts)
    writer = PureFileStorage.append_to_log!(updates(1..10), writer)
    PureFileStorage.terminate(writer)

    # The writer captures its suffix at init, so files and metadata both have to move for the next
    # writer to adopt a suffix ahead of the clock.
    current = PureFileStorage.latest_name(opts)
    future = DateTime.utc_now() |> DateTime.to_unix(:millisecond) |> Kernel.+(60_000)
    future_suffix = "latest.#{future}"

    for kind <- [:json_file, :chunk_file, :key_file] do
      from = apply(PureFileStorage, kind, [opts, current])
      to = apply(PureFileStorage, kind, [opts, future_suffix])
      if File.exists?(from), do: File.rename!(from, to)
    end

    # No cached chunks: reads take the chunk index on disk.
    PureFileStorage.set_latest_name(opts, future_suffix, {nil, []})

    assert PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.new(10, 0), opts)
           |> Enum.any?()

    # Act
    assert :ok = PureFileStorage.compact(opts, 0)
    assert_receive {Storage, {mod, fun, args}}, 5_000
    writer = PureFileStorage.init_writer!(opts, @shape)
    _writer = apply(mod, fun, [writer | args])

    # Assert
    "latest." <> new_position = PureFileStorage.latest_name(opts)
    assert {new_position, ""} = Integer.parse(new_position)

    assert new_position > future,
           "the new suffix (#{new_position}) went backwards past the one it replaces (#{future}); " <>
             "a suffix that moves backwards can land on a name still on disk"

    assert PureFileStorage.get_log_stream(LogOffset.first(), LogOffset.new(10, 0), opts)
           |> Enum.any?(),
           "the log was truncated by its own compaction"
  end

  # `handle_compaction_finished/4` bound the boundary as {old_suffix, _} where it is {offset, suffix},
  # and LogOffset implements String.Chars, so the wrong binding still produced a well-formed path for
  # a file that never existed. Every rm_rf! then succeeded against nothing.
  #
  # A generation is now retained one cycle past being superseded, because a read holds the file names
  # it will open long before it opens them. The bound is what matters: retained, not accumulating.
  test "compaction retains one superseded generation and drops the rest", %{opts: opts} do
    # Arrange
    writer = compact_once(opts, start_shape(opts), 1..10)
    after_first = compacted_logs_settling_to(opts, 1)
    assert length(after_first) == 1, "expected one compacted log after the first compaction"

    # Act
    writer = compact_once(opts, writer, 11..20)
    after_second = compacted_logs_settling_to(opts, 2)
    _writer = compact_once(opts, writer, 21..30)

    # Assert
    after_third = compacted_logs_settling_to(opts, 2)

    assert length(after_second) == 2,
           "the generation the second compaction superseded should still be readable, got #{inspect(after_second)}"

    assert length(after_third) == 2,
           "compaction left #{length(after_third)} compacted logs behind (#{inspect(after_third)}); " <>
             "retention must stay bounded or a long-lived shape grows without end"

    assert after_first -- after_third == after_first,
           "the first generation should be gone by the third compaction, got #{inspect(after_third)}"
  end
end
