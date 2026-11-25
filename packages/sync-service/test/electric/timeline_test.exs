defmodule Electric.TimelineTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit
  import ExUnit.CaptureLog

  alias Electric.Timeline

  @moduletag :tmp_dir
  @stack_id "test_stack"

  describe "load_timeline/1" do
    setup context do
      %{
        opts: [
          persistent_kv: Electric.PersistentKV.Filesystem.new!(root: context.tmp_dir),
          stack_id: @stack_id
        ]
      }
    end

    test "returns nil when no timeline is available", %{opts: opts} do
      assert Timeline.load_timeline(opts) == nil
    end

    test "distinguishes an unreadable record from an absent one", %{opts: opts} = context do
      Timeline.store_timeline({1, 2}, opts)
      truncate!(timeline_path(context), 0)

      assert capture_log(fn ->
               assert Timeline.load_timeline(opts) == :unreadable
             end) =~ "Failed to load timeline ID"
    end
  end

  describe "store_timeline/2" do
    setup context do
      %{
        opts: [
          persistent_kv: Electric.PersistentKV.Filesystem.new!(root: context.tmp_dir),
          stack_id: @stack_id
        ]
      }
    end

    test "stores the timeline", %{opts: opts} do
      timeline = {1, 2}
      Timeline.store_timeline(timeline, opts)
      assert ^timeline = Timeline.load_timeline(opts)
    end

    # The purge stores the new timeline last, and retries after a pause when a step fails. A write
    # that raised restarted the manager at once instead.
    test "returns a write that fails", %{opts: opts} = context do
      # A rename cannot replace a directory that holds a file, whoever runs the test.
      File.mkdir_p!(Path.join(timeline_path(context), "held"))

      assert {:error, _} = Timeline.store_timeline({1, 2}, opts)
    end

    # The filesystem backend syncs the directory after the rename, and raises when that fails.
    test "returns a directory sync that fails", %{opts: opts} do
      Repatch.patch(Electric.Utils, :sync_dir!, fn dir ->
        raise File.Error, reason: :eio, action: "sync directory", path: dir
      end)

      assert {:error, %File.Error{reason: :eio}} = Timeline.store_timeline({1, 2}, opts)
    end
  end

  describe "check/2" do
    setup context do
      timeline = context[:electric_timeline]
      kv = Electric.PersistentKV.Filesystem.new!(root: context.tmp_dir)
      opts = [persistent_kv: kv, shape_cache: {ShapeCache, []}, stack_id: @stack_id]

      if timeline != nil do
        Timeline.store_timeline(timeline, opts)
      end

      {:ok, [timeline: timeline, opts: opts]}
    end

    @tag electric_timeline: nil
    test "stores the timeline if Electric has no timeline yet", %{opts: opts} do
      assert Timeline.load_timeline(opts) == nil

      timeline = {2, 5}

      assert capture_log(fn ->
               assert :no_previous_timeline = Timeline.check(timeline, opts)
             end) =~ "No previous timeline"

      assert ^timeline = Timeline.load_timeline(opts)
    end

    # The caller holds shape metadata it left unloaded for a purge, and a first start would load
    # it again. A caller that read the record first and found it gone could read it once more in
    # the check, which then stored the connected timeline over the purge.
    @tag electric_timeline: nil
    test "with missing_owes_purge?, stores the purge a missing record owes", %{opts: opts} do
      opts = Keyword.put(opts, :missing_owes_purge?, true)

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check({2, 5}, opts)
             end) =~ "The stored timeline is gone"

      assert Timeline.load_timeline(opts) == {2, -1}
    end

    @tag electric_timeline: {2, 5}
    test "with missing_owes_purge?, a record that reads is checked as without it", %{
      opts: opts
    } do
      opts = Keyword.put(opts, :missing_owes_purge?, true)

      capture_log(fn -> assert :ok = Timeline.check({2, 5}, opts) end)
      assert Timeline.load_timeline(opts) == {2, 5}
    end

    @tag electric_timeline: {1, 2}
    test "proceeds without changes if Postgres' timeline matches Electric's timeline", %{
      timeline: timeline,
      opts: opts
    } do
      assert ^timeline = Timeline.load_timeline(opts)

      assert capture_log(fn ->
               assert :ok = Timeline.check(timeline, opts)
             end) =~ "Connected to Postgres"

      assert ^timeline = Timeline.load_timeline(opts)
    end

    @tag electric_timeline: {1, 3}
    test "returns :timeline_changed on Point In Time Recovery (PITR)", %{
      timeline: timeline,
      opts: opts
    } do
      assert ^timeline = Timeline.load_timeline(opts)

      pg_timeline = {1, 2}

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check(pg_timeline, opts)
             end) =~ "Detected PITR to timeline"

      assert Timeline.load_timeline(opts) == {1, -1}
    end

    @tag electric_timeline: {1, 3}
    test "returns :timeline_changed when Postgres DB changed", %{
      timeline: timeline,
      opts: opts
    } do
      assert ^timeline = Timeline.load_timeline(opts)

      pg_timeline = {2, 3}

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check(pg_timeline, opts)
             end) =~ "Detected different Postgres DB"

      assert Timeline.load_timeline(opts) == {2, -1}
    end

    # An unreadable record must not land on the first-boot answer: `:no_previous_timeline` is the
    # one result that skips the purge, so it would let a PITR through unnoticed.
    @tag electric_timeline: {1, 3}
    test "returns :timeline_changed when the stored timeline cannot be read",
         %{opts: opts} = ctx do
      truncate!(timeline_path(ctx), 0)

      pg_timeline = {1, 2}

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check(pg_timeline, opts)
             end) =~ "Could not read the stored timeline"

      assert Timeline.load_timeline(opts) == {1, -1}
    end

    # The caller purges the shapes after the check. A restart before the purge ends must not find
    # the new timeline stored: it would read it as unchanged and keep the old timeline's shapes.
    @tag electric_timeline: {1, 3}
    test "keeps returning :timeline_changed until the new timeline is stored", %{opts: opts} do
      pg_timeline = {1, 2}

      capture_log(fn ->
        assert :timeline_changed = Timeline.check(pg_timeline, opts)
        assert :timeline_changed = Timeline.check(pg_timeline, opts)

        :ok = Timeline.store_timeline(pg_timeline, opts)

        assert :ok = Timeline.check(pg_timeline, opts)
      end)

      assert ^pg_timeline = Timeline.load_timeline(opts)
    end

    # The caller purges nothing until the purge is owed on disk, and retries the check after a
    # pause. A write that raised restarted it at once, on the full disk that failed the write,
    # until its supervisors gave up.
    @tag electric_timeline: {1, 3}
    test "returns the change it found with the write of the owed purge that failed", %{
      opts: opts
    } do
      Repatch.patch(File, :rename, fn _source, _destination -> {:error, :enospc} end)

      capture_log(fn ->
        assert {:error, :timeline_changed, :enospc} = Timeline.check({1, 2}, opts)
      end)

      assert Timeline.load_timeline(opts) == {1, 3}
    end

    # A purge step that fails is retried, and the retry checks the timeline again. Writing the
    # owed purge again, on the full disk that failed the step, raised out of that retry.
    @tag electric_timeline: {1, 3}
    test "does not write again a purge it already owes", %{opts: opts} = ctx do
      pg_timeline = {1, 2}

      capture_log(fn ->
        assert :timeline_changed = Timeline.check(pg_timeline, opts)
        # Every write renames a new file over the record.
        %File.Stat{inode: inode} = File.stat!(timeline_path(ctx))

        assert :timeline_changed = Timeline.check(pg_timeline, opts)
        assert %File.Stat{inode: ^inode} = File.stat!(timeline_path(ctx))
      end)

      assert Timeline.load_timeline(opts) == {1, -1}
    end

    # The caller's write of the owed purge failed, maybe once its rename had landed, and the
    # marker it left is not durable. A check that took the marker it read as written purged on it.
    @tag electric_timeline: {1, 3}
    test "with rewrite_owed_purge?, writes again an owed purge it reads", %{opts: opts} = ctx do
      pg_timeline = {1, 2}
      capture_log(fn -> assert :timeline_changed = Timeline.check(pg_timeline, opts) end)
      %File.Stat{inode: inode} = File.stat!(timeline_path(ctx))
      opts = Keyword.put(opts, :rewrite_owed_purge?, true)

      capture_log(fn -> assert :timeline_changed = Timeline.check(pg_timeline, opts) end)
      refute File.stat!(timeline_path(ctx)).inode == inode
      assert Timeline.load_timeline(opts) == {1, -1}

      Repatch.patch(File, :rename, fn _source, _destination -> {:error, :enospc} end)

      capture_log(fn ->
        assert {:error, :timeline_changed, :enospc} = Timeline.check(pg_timeline, opts)
      end)
    end

    # The caller stops serving the old timeline's shapes before the purge is owed on disk: one that
    # died between the write and the close left them served until another caller purged them.
    @tag electric_timeline: {1, 3}
    test "runs before_owing_purge on a change, before it writes the purge", %{opts: opts} do
      parent = self()
      before = fn -> send(parent, {:before_owing_purge, Timeline.load_timeline(opts)}) end
      opts = Keyword.put(opts, :before_owing_purge, before)

      assert :ok = Timeline.check({1, 3}, opts)
      refute_received {:before_owing_purge, _}

      capture_log(fn -> assert :timeline_changed = Timeline.check({1, 2}, opts) end)
      assert_received {:before_owing_purge, {1, 3}}
      assert Timeline.load_timeline(opts) == {1, -1}

      # A purge already owed is not written again, and the caller still stops serving.
      capture_log(fn -> assert :timeline_changed = Timeline.check({1, 2}, opts) end)
      assert_received {:before_owing_purge, {1, -1}}
    end
  end

  # IDENTIFY_SYSTEM reports the system identifier and the timeline as text, and the manager
  # stores them as received.
  @pg_id "7000000000000000001"

  # Switch points of successive promotions.
  @switch_1 "0/1000000"
  @switch_2 "2/20000000"
  @switch_3 "3/1000000"
  @switch_4 "3/2000000"
  @switch_5 "4/1000000"

  describe "check/3 on a promotion" do
    setup context do
      opts = [
        persistent_kv: Electric.PersistentKV.Filesystem.new!(root: context.tmp_dir),
        stack_id: @stack_id
      ]

      Timeline.store_timeline(Map.get(context, :electric_timeline, {@pg_id, "2"}), opts)
      %{opts: opts}
    end

    test "keeps shapes when the stored timeline is an ancestor and Electric stopped before its switch point",
         %{opts: opts} do
      promotion = promotion([{1, @switch_1}, {2, @switch_2}], {:ok, lsn(@switch_2) - 1})

      assert capture_log(fn ->
               assert :timeline_continued = Timeline.check({@pg_id, "3"}, opts, promotion)
             end) =~ "Timeline 3 continues timeline 2"

      assert Timeline.load_timeline(opts) == {@pg_id, "3"}
      assert Timeline.stream_timeline_id(opts, {@pg_id, "3"}) == "2"
    end

    # A commit position names where the commit record starts, and the new timeline's first record
    # starts at the switch point: a commit there exists on the old timeline only.
    test "purges when Electric took the commit that starts at the switch point", %{opts: opts} do
      promotion = promotion([{1, @switch_1}, {2, @switch_2}], {:ok, lsn(@switch_2)})

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check({@pg_id, "3"}, opts, promotion)
             end) =~
               "Detected PITR to timeline 3: Electric took #{@switch_2} from timeline 2, " <>
                 "at or past the switch point #{@switch_2}. Purging shapes."

      assert Timeline.load_timeline(opts) == {@pg_id, -1}
      assert Timeline.stream_timeline_id(opts, {@pg_id, "3"}) == "3"
    end

    test "purges when Electric took WAL past the switch point", %{opts: opts} do
      promotion = promotion([{1, @switch_1}, {2, @switch_2}], {:ok, lsn(@switch_2) + 1})

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check({@pg_id, "3"}, opts, promotion)
             end) =~ "at or past the switch point #{@switch_2}"
    end

    test "purges when the stored timeline is not an ancestor of the current one", %{opts: opts} do
      promotion = promotion([{1, @switch_1}, {3, @switch_3}], {:ok, 0})

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check({@pg_id, "4"}, opts, promotion)
             end) =~ "Detected PITR to timeline 4: timeline 2 is not an ancestor of timeline 4"
    end

    test "purges when the history could not be read", %{opts: opts} do
      promotion = %{
        history: {:error, ~s|could not open file "pg_wal/00000003.history"|},
        processed_lsn: fn -> {:ok, 0} end
      }

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check({@pg_id, "3"}, opts, promotion)
             end) =~ "Detected PITR to timeline 3: its history is unavailable"
    end

    test "purges when nothing bounds what Electric took", %{opts: opts} do
      promotion =
        promotion(
          [{1, @switch_1}, {2, @switch_2}],
          {:error, "Electric holds no record of what it took from the old timeline"}
        )

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check({@pg_id, "3"}, opts, promotion)
             end) =~
               "Detected PITR to timeline 3: Electric holds no record of what it took from the old timeline"
    end

    test "follows several promotions back to the stored timeline", %{opts: opts} do
      promotion =
        promotion(
          [{1, @switch_1}, {2, @switch_2}, {3, @switch_3}, {4, @switch_4}, {5, @switch_5}],
          {:ok, lsn(@switch_2) - 1}
        )

      assert capture_log(fn ->
               assert :timeline_continued = Timeline.check({@pg_id, "6"}, opts, promotion)
             end) =~ "Timeline 6 continues timeline 2: it branched off at #{@switch_2}"

      assert Timeline.stream_timeline_id(opts, {@pg_id, "6"}) == "2"
    end

    # Timeline 3 ran past where timeline 2 ended, so a later switch point would wrongly cover
    # WAL that only timeline 2 ever held.
    test "measures against the switch point where the stored timeline ended, not a later one",
         %{opts: opts} do
      promotion =
        promotion(
          [{1, @switch_1}, {2, @switch_2}, {3, @switch_3}, {4, @switch_4}, {5, @switch_5}],
          {:ok, lsn(@switch_3) - 1}
        )

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check({@pg_id, "6"}, opts, promotion)
             end) =~ "at or past the switch point #{@switch_2}"
    end

    test "keeps the timeline the stream started on across successive promotions", %{opts: opts} do
      capture_log(fn ->
        assert :timeline_continued =
                 Timeline.check(
                   {@pg_id, "3"},
                   opts,
                   promotion([{1, @switch_1}, {2, @switch_2}], {:ok, lsn(@switch_2) - 1})
                 )

        assert :timeline_continued =
                 Timeline.check(
                   {@pg_id, "4"},
                   opts,
                   promotion(
                     [{1, @switch_1}, {2, @switch_2}, {3, @switch_3}],
                     {:ok, lsn(@switch_3) - 1}
                   )
                 )

        assert :ok = Timeline.check({@pg_id, "4"}, opts)
      end)

      assert Timeline.load_timeline(opts) == {@pg_id, "4"}
      assert Timeline.stream_timeline_id(opts, {@pg_id, "4"}) == "2"
    end

    test "a purge starts a new stream", %{opts: opts} do
      capture_log(fn ->
        assert :timeline_continued =
                 Timeline.check(
                   {@pg_id, "3"},
                   opts,
                   promotion([{1, @switch_1}, {2, @switch_2}], {:ok, lsn(@switch_2) - 1})
                 )

        assert :timeline_changed = Timeline.check({@pg_id, "5"}, opts)
      end)

      assert Timeline.load_timeline(opts) == {@pg_id, -1}
      assert Timeline.stream_timeline_id(opts, {@pg_id, "5"}) == "5"

      :ok = Timeline.store_timeline({@pg_id, "5"}, opts)

      assert Timeline.load_timeline(opts) == {@pg_id, "5"}
      assert Timeline.stream_timeline_id(opts, {@pg_id, "5"}) == "5"
    end

    test "never reads a recreated slot as a promotion", %{opts: opts} do
      Timeline.store_irrecoverable_timeline(@pg_id, opts)
      promotion = promotion([{1, @switch_1}, {2, @switch_2}], {:ok, 0})

      capture_log(fn ->
        assert :timeline_changed = Timeline.check({@pg_id, "3"}, opts, promotion)
      end)
    end

    @tag electric_timeline: {"6000000000000000001", "2"}
    test "never reads another database as a promotion", %{opts: opts} do
      promotion = promotion([{1, @switch_1}, {2, @switch_2}], {:ok, 0})

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check({@pg_id, "3"}, opts, promotion)
             end) =~ "Detected different Postgres DB"
    end

    test "never reads an unreadable record as a promotion", %{opts: opts} = ctx do
      truncate!(timeline_path(ctx), 0)
      promotion = promotion([{1, @switch_1}, {2, @switch_2}], {:ok, 0})

      assert capture_log(fn ->
               assert :timeline_changed = Timeline.check({@pg_id, "3"}, opts, promotion)
             end) =~ "Could not read the stored timeline"
    end

    test "reads no evidence while the timeline is unchanged", %{opts: opts} do
      promotion = %{
        history: {:ok, []},
        processed_lsn: fn -> raise "read evidence for an unchanged timeline" end
      }

      capture_log(fn ->
        assert :ok = Timeline.check({@pg_id, "2"}, opts, promotion)
      end)
    end
  end

  describe "stream_timeline_id/2" do
    setup context do
      %{
        opts: [
          persistent_kv: Electric.PersistentKV.Filesystem.new!(root: context.tmp_dir),
          stack_id: @stack_id
        ]
      }
    end

    test "is the connected timeline when nothing is stored", %{opts: opts} do
      assert Timeline.stream_timeline_id(opts, {@pg_id, "3"}) == "3"
    end

    test "is the connected timeline when no promotion was read as a continuation", %{opts: opts} do
      Timeline.store_timeline({@pg_id, "3"}, opts)

      assert Timeline.stream_timeline_id(opts, {@pg_id, "3"}) == "3"
    end

    # The replication client can reach a timeline the manager has not checked yet: the stored
    # stream then belongs to another connection.
    test "is the connected timeline when the record names another one", %{opts: opts} do
      Timeline.store_timeline({@pg_id, "2"}, opts)

      capture_log(fn ->
        Timeline.check(
          {@pg_id, "3"},
          opts,
          promotion([{1, @switch_1}, {2, @switch_2}], {:ok, 0})
        )
      end)

      assert Timeline.stream_timeline_id(opts, {@pg_id, "4"}) == "4"
    end
  end

  describe "parse_history/1" do
    test "reads PostgreSQL's history file format" do
      content =
        "1\t0/1000000\tno recovery target specified\n" <>
          "\n# written by hand\n" <>
          "2\t1/20000000\tbefore 2024-01-01 00:00:00+00\n"

      assert Timeline.parse_history(content) ==
               {:ok, [{1, lsn("0/1000000")}, {2, lsn("1/20000000")}]}
    end

    test "reads an empty history" do
      assert Timeline.parse_history("") == {:ok, []}
    end

    test "rejects a malformed line" do
      assert Timeline.parse_history("1\t0/1000000\tok\ntwo\t0/1\n") ==
               {:error, {:malformed_history_line, "two\t0/1"}}
    end
  end

  defp promotion(history, processed_lsn) do
    %{
      history:
        {:ok, Enum.map(history, fn {parent, switch_point} -> {parent, lsn(switch_point)} end)},
      processed_lsn: fn -> processed_lsn end
    }
  end

  defp lsn(string),
    do: string |> Electric.Postgres.Lsn.from_string() |> Electric.Postgres.Lsn.to_integer()

  defp timeline_path(%{tmp_dir: tmp_dir}), do: Path.join(tmp_dir, "timeline_id_#{@stack_id}")

  defp truncate!(path, size) do
    File.open!(path, [:read, :write, :raw], fn file ->
      {:ok, ^size} = :file.position(file, size)
      :ok = :file.truncate(file)
    end)
  end
end
