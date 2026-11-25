defmodule Electric.ShapeCache.PureFileStorage.PayloadArenaTest do
  use ExUnit.Case, async: true
  use Repatch.ExUnit

  import Support.ComponentSetup

  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.Storage
  alias Electric.ShapeCache.PureFileStorage
  alias Electric.ShapeCache.PureFileStorage.PayloadArena
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir

  describe "writer/reader" do
    test "appended payloads resolve back through a log stream", %{tmp_dir: tmp_dir} do
      writer = PayloadArena.open_writer!(tmp_dir)

      {writer, ref1} = PayloadArena.append!(writer, "key-1", ~S|{"a":1}|)
      {writer, ref2} = PayloadArena.append!(writer, "key-2", ~S|{"b":2}|)
      _writer = PayloadArena.sync!(writer)

      assert [~S|{"a":1}|, ~S|inline|, ~S|{"b":2}|] ==
               [{:ref, ref1}, ~S|inline|, {:ref, ref2}]
               |> PayloadArena.resolve_refs(tmp_dir)
               |> Enum.to_list()
    end

    test "reader returns the full key and json payload", %{tmp_dir: tmp_dir} do
      writer = PayloadArena.open_writer!(tmp_dir)
      {writer, ref} = PayloadArena.append!(writer, "the-key", ~S|{"x":42}|)
      PayloadArena.sync!(writer)

      reader = PayloadArena.open_reader(tmp_dir)
      assert {reader, {"the-key", ~S|{"x":42}|}} = PayloadArena.read_payload!(reader, ref)
      assert :ok = PayloadArena.close_reader!(reader)
    end

    test "a discarded suffix is overwritten, not desynced", %{tmp_dir: tmp_dir} do
      writer = PayloadArena.open_writer!(tmp_dir)
      {writer, ref_a} = PayloadArena.append!(writer, "ka", ~S|{"kept":"a"}|)

      # Simulates the router's rescue path: the appends of a failed class preparation are
      # discarded together with the writer struct that performed them.
      {_discarded, _ref_b} = PayloadArena.append!(writer, "kb", ~S|{"discarded":"b"}|)

      {writer, ref_c} = PayloadArena.append!(writer, "kc", ~S|{"kept":"c"}|)
      PayloadArena.sync!(writer)

      assert [~S|{"kept":"a"}|, ~S|{"kept":"c"}|] ==
               [{:ref, ref_a}, {:ref, ref_c}]
               |> PayloadArena.resolve_refs(tmp_dir)
               |> Enum.to_list()
    end

    test "any_generations? reflects whether a writer ever opened", %{tmp_dir: tmp_dir} do
      refute PayloadArena.any_generations?(tmp_dir)
      PayloadArena.open_writer!(tmp_dir)
      assert PayloadArena.any_generations?(tmp_dir)
    end

    test "empty generations left by previous boots are pruned", %{tmp_dir: tmp_dir} do
      Enum.each(1..3, fn discriminator -> PayloadArena.open_writer!(tmp_dir, discriminator) end)
      dir = Path.join(tmp_dir, "payload-arena")
      assert length(File.ls!(dir)) == 3

      assert :ok = PayloadArena.prune_empty_generations!(tmp_dir)

      assert File.ls!(dir) == []
    end

    test "a generation holding payloads survives pruning and still resolves", %{tmp_dir: tmp_dir} do
      PayloadArena.open_writer!(tmp_dir, 1)
      writer = PayloadArena.open_writer!(tmp_dir, 2)
      {writer, ref} = PayloadArena.append!(writer, "k", ~S|{"v":1}|)
      PayloadArena.sync!(writer)

      PayloadArena.prune_empty_generations!(tmp_dir)

      assert File.ls!(Path.join(tmp_dir, "payload-arena")) == ["#{writer.gen}.bin"]

      assert [~S|{"v":1}|] ==
               [{:ref, ref}] |> PayloadArena.resolve_refs(tmp_dir) |> Enum.to_list()
    end

    test "a nonempty generation no ref reaches is reclaimed", %{tmp_dir: tmp_dir} do
      # empties never touches either -- this is what accumulated forever before reclaim existed.
      kept = PayloadArena.open_writer!(tmp_dir, 1)
      {kept, ref} = PayloadArena.append!(kept, "k", ~S|{"v":1}|)
      PayloadArena.sync!(kept)

      orphan = PayloadArena.open_writer!(tmp_dir, 2)
      {orphan, _ref} = PayloadArena.append!(orphan, "k2", ~S|{"v":2}|)
      PayloadArena.sync!(orphan)

      dir = Path.join(tmp_dir, "payload-arena")
      assert length(File.ls!(dir)) == 2

      assert :ok = PayloadArena.reclaim_generations!(tmp_dir, MapSet.new([kept.gen]))

      assert File.ls!(dir) == ["#{kept.gen}.bin"]

      assert [~S|{"v":1}|] ==
               [{:ref, ref}] |> PayloadArena.resolve_refs(tmp_dir) |> Enum.to_list()
    end

    test "reclaim on a stack that never enabled the arena does nothing", %{tmp_dir: tmp_dir} do
      assert :ok = PayloadArena.reclaim_generations!(tmp_dir, MapSet.new())

      refute PayloadArena.any_generations?(tmp_dir)
    end

    test "pruning a stack that never enabled the arena does nothing", %{tmp_dir: tmp_dir} do
      assert :ok = PayloadArena.prune_empty_generations!(tmp_dir)

      refute PayloadArena.any_generations?(tmp_dir)
    end

    test "a writer opened after the clock steps back never reuses an id", %{tmp_dir: tmp_dir} do
      ahead = (System.system_time(:millisecond) + :timer.hours(1)) * 256 + 3
      File.mkdir_p!(Path.join(tmp_dir, "payload-arena"))
      File.write!(Path.join([tmp_dir, "payload-arena", "#{ahead}.bin"]), <<>>)

      writer = PayloadArena.open_writer!(tmp_dir, 3)

      assert writer.gen > ahead
      assert rem(writer.gen, 256) == 3
    end

    test "a dangling ref raises instead of serving garbage", %{tmp_dir: tmp_dir} do
      writer = PayloadArena.open_writer!(tmp_dir)
      {writer, _ref} = PayloadArena.append!(writer, "k", ~S|{"v":1}|)
      PayloadArena.sync!(writer)

      bogus = <<10::64, 0::32, writer.gen::64, 999_999::64>>

      error =
        assert_raise Storage.Error, fn ->
          [{:ref, bogus}] |> PayloadArena.resolve_refs(tmp_dir) |> Enum.to_list()
        end

      assert error.kind == :data_loss
      assert error.message =~ "dangling payload-arena ref"
    end

    test "a missing generation reaches the caller's data-loss callback", %{tmp_dir: tmp_dir} do
      writer = PayloadArena.open_writer!(tmp_dir)
      {writer, ref} = PayloadArena.append!(writer, "k", ~S|{"v":1}|)
      PayloadArena.sync!(writer)
      File.rm!(Path.join([tmp_dir, "payload-arena", "#{writer.gen}.bin"]))
      test_pid = self()

      report = fn message ->
        send(test_pid, {:data_loss, message})
        raise Storage.Error, kind: :data_loss, message: message
      end

      assert_raise Storage.Error, fn ->
        [{:ref, ref}] |> PayloadArena.resolve_refs(tmp_dir, report) |> Enum.to_list()
      end

      assert_received {:data_loss, message}
      assert message =~ "missing payload-arena generation"
    end

    test "a missing generation is data loss on the inlining read path too", %{tmp_dir: tmp_dir} do
      writer = PayloadArena.open_writer!(tmp_dir)
      {writer, ref} = PayloadArena.append!(writer, "k", ~S|{"v":1}|)
      PayloadArena.sync!(writer)
      File.rm!(Path.join([tmp_dir, "payload-arena", "#{writer.gen}.bin"]))
      reader = PayloadArena.open_reader(tmp_dir)

      error = assert_raise Storage.Error, fn -> PayloadArena.read_payload!(reader, ref) end

      assert error.kind == :data_loss
      assert error.message =~ "missing payload-arena generation"
    end
  end

  describe "storage integration" do
    setup [:with_stack_id_from_test, :with_async_deleter]

    @shape_handle "the-shape-handle"
    @shape %Shape{
      root_table: {"public", "items"},
      root_table_id: 1,
      root_pk: ["id"],
      selected_columns: ["id"],
      explicitly_selected_columns: ["id"]
    }

    setup ctx do
      base_opts =
        PureFileStorage.shared_opts(
          stack_id: ctx.stack_id,
          storage_dir: ctx.tmp_dir,
          chunk_bytes_threshold: ctx[:chunk_size] || 10 * 1024 * 1024,
          flush_period: ctx[:flush_period] || 1000
        )

      start_link_supervised!(Storage.stack_child_spec({PureFileStorage, base_opts}))

      opts = PureFileStorage.for_shape(@shape_handle, base_opts)
      arena = PayloadArena.open_writer!(base_opts.base_path)

      %{base_opts: base_opts, opts: opts, arena: arena}
    end

    defp start_writer(%{opts: opts}) do
      writer = PureFileStorage.init_writer!(opts, @shape)
      PureFileStorage.set_pg_snapshot(%{xmin: 100}, opts)
      PureFileStorage.mark_snapshot_as_started(opts)
      PureFileStorage.make_new_snapshot!([], opts)
      writer
    end

    defp ref_line(arena, offset, key, op, json) do
      {arena, ref} = PayloadArena.append!(arena, key, json)
      {arena, {offset, key, op, json, ref}}
    end

    test "ref lines are served from the live tail before any flush", ctx do
      writer = start_writer(ctx)

      {arena, line} = ref_line(ctx.arena, LogOffset.new(10, 0), "k1", :insert, ~S|{"n":1}|)
      PayloadArena.sync!(arena)

      _writer = PureFileStorage.append_to_log!([line], writer)

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(11, 0), ctx.opts)
             |> Enum.to_list() == [~S|{"n":1}|]
    end

    test "ref lines are served from disk after the writer terminates", ctx do
      writer = start_writer(ctx)

      {arena, line1} = ref_line(ctx.arena, LogOffset.new(10, 0), "k1", :insert, ~S|{"n":1}|)
      {arena, line2} = ref_line(arena, LogOffset.new(11, 0), "k2", :delete, ~S|{"n":2}|)
      PayloadArena.sync!(arena)

      writer =
        PureFileStorage.append_to_log!(
          [line1, {LogOffset.new(10, 2), "k-inline", :update, ~S|{"inline":true}|}, line2],
          writer
        )

      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(12, 0), ctx.opts)
             |> Enum.to_list() == [~S|{"n":1}|, ~S|{"inline":true}|, ~S|{"n":2}|]
    end

    @tag chunk_size: 5
    test "chunk boundaries are driven by real payload sizes, and reads cross them", ctx do
      writer = start_writer(ctx)

      {lines, arena} =
        Enum.map_reduce(1..6, ctx.arena, fn i, arena ->
          {arena, line} =
            ref_line(
              arena,
              LogOffset.new(10 + i, 0),
              "k#{i}",
              :insert,
              ~S|{"payload":| <> "#{i}}"
            )

          {line, arena}
        end)

      PayloadArena.sync!(arena)

      writer = PureFileStorage.append_to_log!(lines, writer)
      PureFileStorage.terminate(writer)

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(20, 0), ctx.opts)
             |> Enum.to_list() ==
               Enum.map(1..6, fn i -> ~S|{"payload":| <> "#{i}}" end)
    end

    @tag chunk_size: 5
    test "compaction inlines ref entries and merges through them", ctx do
      writer = start_writer(ctx)

      {arena, insert_line} =
        ref_line(
          ctx.arena,
          LogOffset.new(10, 0),
          ~S|"k1"|,
          :insert,
          ~S|{"value":{"id":"k1"},"key":"\"k1\"","headers":{"operation":"insert"}}|
        )

      PayloadArena.sync!(arena)

      updates =
        Enum.map(1..6, fn i ->
          {LogOffset.new(10 + i, 0), ~S|"k1"|, :update,
           ~S|{"value":{"id":"k1","n":"| <>
             "#{i}" <> ~S|"},"key":"\"k1\"","headers":{"operation":"update"}}|}
        end)

      writer = PureFileStorage.append_to_log!([insert_line | updates], writer)
      PureFileStorage.terminate(writer)

      assert :ok = PureFileStorage.compact(ctx.opts, 0)
      assert_receive {Storage, {PureFileStorage, :handle_compaction_finished, args}}, 5000

      writer = PureFileStorage.init_writer!(ctx.opts, @shape)
      _writer = apply(PureFileStorage, :handle_compaction_finished, [writer | args])

      lines =
        PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(20, 0), ctx.opts)
        |> Enum.map(&Jason.decode!(&1, keys: :atoms))

      assert %{headers: %{operation: "insert"}, value: %{id: "k1"}} = hd(lines)
      assert %{headers: %{operation: "update"}, value: %{n: "6"}} = List.last(lines)
    end

    test "reclaim keeps a generation a shape references and drops an unreferenced one", ctx do
      writer = start_writer(ctx)
      base_path = ctx.base_opts.base_path

      # A shape log holding a ref into the generation opened in setup.
      {arena, line} = ref_line(ctx.arena, LogOffset.new(10, 0), "k1", :insert, ~S|{"n":1}|)
      PayloadArena.sync!(arena)
      writer = PureFileStorage.append_to_log!([line], writer)
      PureFileStorage.terminate(writer)

      # A second nonempty generation no shape references -- a leftover from another boot's writer.
      stray = PayloadArena.open_writer!(base_path, 5)
      {stray, _ref} = PayloadArena.append!(stray, "orphan", ~S|{"n":99}|)
      PayloadArena.sync!(stray)

      referenced = PureFileStorage.referenced_arena_generations(base_path)
      assert MapSet.member?(referenced, ctx.arena.gen)
      refute MapSet.member?(referenced, stray.gen)

      assert :ok = PayloadArena.reclaim_generations!(base_path, referenced)

      dir = Path.join(base_path, "payload-arena")
      refute File.exists?(Path.join(dir, "#{stray.gen}.bin"))
      assert File.exists?(Path.join(dir, "#{ctx.arena.gen}.bin"))

      assert PureFileStorage.get_log_stream(LogOffset.new(9, 0), LogOffset.new(11, 0), ctx.opts)
             |> Enum.to_list() == [~S|{"n":1}|]
    end

    # A directory that can be searched and not read still serves the shapes under it by handle, and
    # the scan read their refs as none. A symlink to itself fails the listing as a root user too.
    test "the scan fails on a shard it cannot list", ctx do
      base_path = write_ref_line(ctx)
      File.mkdir_p!(Path.join(base_path, "un"))
      File.ln_s!("li", Path.join([base_path, "un", "li"]))

      assert_raise File.Error, ~r/list directory/, fn ->
        PureFileStorage.referenced_arena_generations(base_path)
      end
    end

    # The boot scans only a store whose arena holds payloads, so its root was there: one the listing
    # does not find, a symlinked root whose target is away for it say, would read as holding no
    # shape, and the reclaim would drop every generation once the target is back.
    test "the scan fails on a store root it cannot find", ctx do
      base_path = write_ref_line(ctx)

      assert_raise File.Error, ~r/list directory/, fn ->
        PureFileStorage.referenced_arena_generations(Path.join(base_path, "missing"))
      end
    end

    test "the scan fails on a shape log directory it cannot list", ctx do
      base_path = write_ref_line(ctx)
      log_dir = Path.join([base_path, "un", "li", "unlisted-shape", "log"])
      File.mkdir_p!(Path.dirname(log_dir))
      File.ln_s!("log", log_dir)

      assert_raise File.Error, ~r/list directory/, fn ->
        PureFileStorage.referenced_arena_generations(base_path)
      end
    end

    test "the scan leaves out an entry no shape can sit under", ctx do
      base_path = write_ref_line(ctx)
      File.ln_s!("backups", Path.join(base_path, "backups"))
      File.ln_s!("backups", Path.join([base_path, "th", "backups"]))

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen])
    end

    # The service reads a shape only at the path its handle maps to. An entry three levels down
    # anywhere else, a copy moved there by hand say, is read by no response: a symlink to itself
    # there failed every scan, and a copy there kept the generations it referred to.
    test "the scan leaves out an entry away from the path its name maps to", ctx do
      base_path = write_ref_line(ctx)
      {moved_dir, _moved_gen} = write_ref_shape(ctx, "moved-shape-handle", 1)
      misplaced_dir = Path.join([base_path, "12", "34"])
      File.mkdir_p!(misplaced_dir)
      File.ln_s!("backup", Path.join(misplaced_dir, "backup"))
      File.rename!(moved_dir, Path.join(misplaced_dir, "moved-shape-handle"))

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen])
    end

    # A response begun before a restart still reads a removed shape's log through the descriptor it
    # opened, and resolves the refs it reads next, so a log gone after its listing fails the scan.
    test "a log whose shape a cleanup removes after its listing fails the scan", ctx do
      base_path = write_ref_line(ctx)
      {moved_dir, moved_gen} = write_ref_shape(ctx, "moved-shape-handle", 1)

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen, moved_gen])

      test_pid = self()

      Repatch.patch(PureFileStorage.LogFile, :arena_generations_in_log, fn path ->
        if String.starts_with?(path, moved_dir <> "/") do
          File.rm_rf!(moved_dir)
          send(test_pid, {:removed_under, path})
        end

        Repatch.real(PureFileStorage.LogFile.arena_generations_in_log(path))
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.referenced_arena_generations(base_path) end

      assert_received {:removed_under, log_path}
      assert {error.reason, error.path} == {:enoent, log_path}
    end

    # A response begun before a restart still reads a removed shape's log through the descriptor
    # it opened, so a shape gone after the scan listed it fails the scan. Whichever shape the scan
    # reads first, a cleanup removes the other during that read; each holds a ref into its own
    # generation.
    test "a shape a cleanup removes after the scan listed it fails the scan", ctx do
      base_path = write_ref_line(ctx)
      first_dir = PureFileStorage.shape_data_dir(ctx.opts)
      {moved_dir, moved_gen} = write_ref_shape(ctx, "moved-shape-handle", 1)

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen, moved_gen])

      test_pid = self()

      Repatch.patch(PureFileStorage.LogFile, :arena_generations_in_log, fn path ->
        for dir <- [first_dir, moved_dir],
            not String.starts_with?(path, dir <> "/"),
            File.exists?(dir) do
          File.rm_rf!(dir)
          send(test_pid, {:removed, dir})
        end

        Repatch.real(PureFileStorage.LogFile.arena_generations_in_log(path))
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.referenced_arena_generations(base_path) end

      assert_received {:removed, removed_dir}
      refute_received {:removed, _dir}
      assert {error.reason, error.path} == {:enoent, Path.join(removed_dir, "log")}
    end

    # A shape directory can be a symlink, which the reads follow. A cleanup that moves its target
    # away after the scan listed the shape leaves the symlink, and the shape is gone all the same.
    # Whichever shape the scan reads first, the other's target is moved during that read.
    test "a symlinked shape whose target is moved away after the scan listed it fails the scan",
         ctx do
      base_path = write_ref_line(ctx)
      {second_dir, second_gen} = write_ref_shape(ctx, "second-shape-handle", 1)

      linked =
        for dir <- [PureFileStorage.shape_data_dir(ctx.opts), second_dir] do
          target = Path.join([base_path, "targets", Path.basename(dir)])
          File.mkdir_p!(Path.dirname(target))
          File.rename!(dir, target)
          File.ln_s!(target, dir)
          {dir, target}
        end

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen, second_gen])

      test_pid = self()

      Repatch.patch(PureFileStorage.LogFile, :arena_generations_in_log, fn path ->
        for {dir, target} <- linked,
            not String.starts_with?(path, dir <> "/"),
            File.exists?(target) do
          File.rename!(target, target <> ".removed")
          send(test_pid, {:moved, dir})
        end

        Repatch.real(PureFileStorage.LogFile.arena_generations_in_log(path))
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.referenced_arena_generations(base_path) end

      assert_received {:moved, moved_dir}
      refute_received {:moved, _dir}
      assert {error.reason, error.path} == {:enoent, Path.join(moved_dir, "log")}
    end

    # A shape's log directory can be a symlink too. A move of its target after the scan listed
    # the shape leaves the shape directory in place and the symlink in it, and the logs gone all
    # the same, while a response still reads them. Whichever shape the scan reads first, the
    # other's log target is moved during that read.
    test "a shape whose log directory's target is moved away after the scan listed it fails the scan",
         ctx do
      base_path = write_ref_line(ctx)
      {second_dir, second_gen} = write_ref_shape(ctx, "second-shape-handle", 1)

      linked =
        for dir <- [PureFileStorage.shape_data_dir(ctx.opts), second_dir] do
          log_dir = Path.join(dir, "log")
          target = Path.join([base_path, "targets", Path.basename(dir)])
          File.mkdir_p!(Path.dirname(target))
          File.rename!(log_dir, target)
          File.ln_s!(target, log_dir)
          {dir, target}
        end

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen, second_gen])

      test_pid = self()

      Repatch.patch(PureFileStorage.LogFile, :arena_generations_in_log, fn path ->
        for {dir, target} <- linked,
            not String.starts_with?(path, dir <> "/"),
            File.exists?(target) do
          File.rename!(target, target <> ".removed")
          send(test_pid, {:moved, dir})
        end

        Repatch.real(PureFileStorage.LogFile.arena_generations_in_log(path))
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.referenced_arena_generations(base_path) end

      assert_received {:moved, moved_dir}
      refute_received {:moved, _dir}
      assert File.dir?(moved_dir)
      assert {error.reason, error.path} == {:enoent, Path.join(moved_dir, "log")}
    end

    # A symlinked log directory can be retargeted at a file in one rename after the scan listed
    # the shape, while a response still reads the logs it opened through the old target. The log
    # path then lists as no directory, and the file it resolves to says nothing of those logs.
    # Whichever shape the scan reads first, the other's log is retargeted during that read.
    test "a shape whose log symlink is retargeted at a file after the scan listed it fails the scan",
         ctx do
      base_path = write_ref_line(ctx)
      {second_dir, second_gen} = write_ref_shape(ctx, "second-shape-handle", 1)

      for dir <- [PureFileStorage.shape_data_dir(ctx.opts), second_dir] do
        log_dir = Path.join(dir, "log")
        target = Path.join([base_path, "targets", Path.basename(dir)])
        File.mkdir_p!(Path.dirname(target))
        File.rename!(log_dir, target)
        File.ln_s!(target, log_dir)
      end

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen, second_gen])

      test_pid = self()

      Repatch.patch(PureFileStorage.LogFile, :arena_generations_in_log, fn path ->
        for dir <- [PureFileStorage.shape_data_dir(ctx.opts), second_dir],
            not String.starts_with?(path, dir <> "/"),
            log_dir = Path.join(dir, "log"),
            File.dir?(log_dir) do
          file = Path.join([base_path, "targets", Path.basename(dir) <> ".file"])
          File.write!(file, "")
          File.ln_s!(file, log_dir <> ".retarget")
          File.rename!(log_dir <> ".retarget", log_dir)
          send(test_pid, {:retargeted, dir})
        end

        Repatch.real(PureFileStorage.LogFile.arena_generations_in_log(path))
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.referenced_arena_generations(base_path) end

      assert_received {:retargeted, retargeted_dir}
      refute_received {:retargeted, _dir}
      assert File.regular?(Path.join(retargeted_dir, "log"))
      assert {error.reason, error.path} == {:enotdir, Path.join(retargeted_dir, "log")}
    end

    # A symlinked shape directory can be retargeted at a file in one rename after the scan listed
    # it, while a response still reads the logs it opened through the old target. The shape's path
    # then resolves to no directory, and the file says nothing of those logs.
    test "a symlinked shape retargeted at a file after the scan listed it fails the scan", ctx do
      base_path = write_ref_line(ctx)
      {second_dir, second_gen} = write_ref_shape(ctx, "second-shape-handle", 1)
      target = Path.join([base_path, "targets", "second-shape-handle"])
      File.mkdir_p!(Path.dirname(target))
      File.rename!(second_dir, target)
      File.ln_s!(target, second_dir)

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen, second_gen])

      test_pid = self()

      Repatch.patch(PureFileStorage, :at_handle_path?, fn base, path ->
        if path == second_dir and File.dir?(path) do
          file = target <> ".file"
          File.write!(file, "")
          File.ln_s!(file, path <> ".retarget")
          File.rename!(path <> ".retarget", path)
          send(test_pid, :retargeted)
        end

        Repatch.real(Repatch.private(PureFileStorage.at_handle_path?(base, path)))
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.referenced_arena_generations(base_path) end

      assert_received :retargeted
      assert File.regular?(second_dir)
      assert {error.reason, error.path} == {:enotdir, Path.join(second_dir, "log")}
    end

    # A log directory can be missing for a moment, a symlinked one whose target is moved away and
    # back say, while a response reads the logs it opened through it. A listing that finds it gone
    # saw none of those logs, and the looks after it find it back.
    test "a log directory the listing finds gone and the next look finds back fails the scan",
         ctx do
      base_path = write_ref_line(ctx)
      {second_dir, second_gen} = write_ref_shape(ctx, "second-shape-handle", 1)
      log_dir = Path.join(second_dir, "log")
      target = Path.join([base_path, "targets", "second-shape-handle"])
      File.mkdir_p!(Path.dirname(target))
      File.rename!(log_dir, target)
      File.ln_s!(target, log_dir)

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen, second_gen])

      File.rename!(target, target <> ".away")
      test_pid = self()

      Repatch.patch(PureFileStorage, :log_dir_stat, fn shape_dir, dir ->
        if File.exists?(target <> ".away") do
          File.rename!(target <> ".away", target)
          send(test_pid, {:back, dir})
        end

        Repatch.real(Repatch.private(PureFileStorage.log_dir_stat(shape_dir, dir)))
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.referenced_arena_generations(base_path) end

      assert_received {:back, ^log_dir}
      assert {error.reason, error.path} == {:enoent, log_dir}

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen, second_gen])
    end

    # A shard can be a symlink, to spread shapes over volumes say. A move of its target after the
    # listing found the shard, and back, leaves the shapes under it served by handle, and the scan
    # read none of their refs.
    test "a symlinked shard whose target is moved away after the listing found it fails the scan",
         ctx do
      base_path = write_ref_line(ctx)
      shard = ctx.opts |> PureFileStorage.shape_data_dir() |> Path.dirname() |> Path.dirname()
      target = Path.join([base_path, "targets", Path.basename(shard)])
      File.mkdir_p!(Path.dirname(target))
      File.rename!(shard, target)
      File.ln_s!(target, shard)

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen])

      test_pid = self()

      Repatch.patch(PureFileStorage, :ls_dir!, fn path, nothing_under ->
        if path == shard and not Process.get(:moved, false) do
          Process.put(:moved, true)
          File.rename!(target, target <> ".away")
          send(test_pid, :moved)
        end

        Repatch.real(Repatch.private(PureFileStorage.ls_dir!(path, nothing_under)))
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.referenced_arena_generations(base_path) end

      assert_received :moved
      assert {error.reason, error.path} == {:enoent, shard}

      File.rename!(target <> ".away", target)

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen])
    end

    # A symlinked shape whose target is away for its log listing hides its log directory and its
    # version from the looks after it too, and a target back before the last look left the scan
    # with none of the refs of a shape whose logs are back.
    test "a symlinked shape whose target is away for its log listing and back after fails the scan",
         ctx do
      base_path = write_ref_line(ctx)
      shape_dir = PureFileStorage.shape_data_dir(ctx.opts)
      log_dir = Path.join(shape_dir, "log")
      target = Path.join([base_path, "targets", Path.basename(shape_dir)])
      File.mkdir_p!(Path.dirname(target))
      File.rename!(shape_dir, target)
      File.ln_s!(target, shape_dir)

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen])

      test_pid = self()

      Repatch.patch(PureFileStorage, :ls_dir!, fn path, nothing_under ->
        if path == log_dir and not Process.get(:moved, false) do
          Process.put(:moved, true)
          File.rename!(target, target <> ".away")
          send(test_pid, :moved)
        end

        Repatch.real(Repatch.private(PureFileStorage.ls_dir!(path, nothing_under)))
      end)

      Repatch.patch(PureFileStorage, :version_stat, fn dir ->
        stat = Repatch.real(Repatch.private(PureFileStorage.version_stat(dir)))

        if File.exists?(target <> ".away") do
          File.rename!(target <> ".away", target)
          send(test_pid, :back)
        end

        stat
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.referenced_arena_generations(base_path) end

      assert_received :moved
      assert {error.reason, error.path} == {:enoent, log_dir}

      if File.exists?(target <> ".away"), do: File.rename!(target <> ".away", target)

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen])
    end

    # A shape's log directory can be moved away alone, by hand say, while a response still reads
    # the logs it held through the descriptor it opened. The shape's version is written after its
    # log directory is created, so the shape is no writer yet to create one. Whichever shape the
    # scan reads first, the other's log directory is moved during that read.
    test "a shape whose log directory is moved away after the scan listed it fails the scan",
         ctx do
      base_path = write_ref_line(ctx)
      first_dir = PureFileStorage.shape_data_dir(ctx.opts)
      {second_dir, second_gen} = write_ref_shape(ctx, "second-shape-handle", 1)

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen, second_gen])

      test_pid = self()

      Repatch.patch(PureFileStorage.LogFile, :arena_generations_in_log, fn path ->
        for dir <- [first_dir, second_dir],
            not String.starts_with?(path, dir <> "/"),
            log_dir = Path.join(dir, "log"),
            File.dir?(log_dir) do
          File.rename!(log_dir, log_dir <> ".moved")
          send(test_pid, {:moved, dir})
        end

        Repatch.real(PureFileStorage.LogFile.arena_generations_in_log(path))
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.referenced_arena_generations(base_path) end

      assert_received {:moved, moved_dir}
      refute_received {:moved, _dir}
      assert File.regular?(Path.join([moved_dir, "metadata", "version.bin"]))
      assert {error.reason, error.path} == {:enoent, Path.join(moved_dir, "log")}
    end

    # A compaction's removal of an older generation can run during the scan, while a response
    # begun before the restart still reads that generation's refs.
    test "a log gone from a shape still on disk fails the scan", ctx do
      base_path = write_ref_line(ctx)
      {compacted_dir, compacted_gen} = write_ref_shape(ctx, "compacted-shape-handle", 1)

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen, compacted_gen])

      test_pid = self()

      Repatch.patch(PureFileStorage.LogFile, :arena_generations_in_log, fn path ->
        if String.starts_with?(path, compacted_dir <> "/") do
          File.rm!(path)
          send(test_pid, {:removed, path})
        end

        Repatch.real(PureFileStorage.LogFile.arena_generations_in_log(path))
      end)

      error =
        assert_raise File.Error, fn -> PureFileStorage.referenced_arena_generations(base_path) end

      assert_received {:removed, log_path}
      assert {error.reason, error.path} == {:enoent, log_path}
    end

    # A shape directory can exist before its writer creates the log directory under it.
    test "a shape directory without a log directory holds no refs", ctx do
      base_path = write_ref_line(ctx)
      File.mkdir_p!(PureFileStorage.shape_data_dir(base_path, "bare-shape-handle"))

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen])
    end

    # A stray file can sit at the path a shape's name maps to. No shape sits under it, and the
    # scan still keeps the generations the other shapes refer to.
    test "a file at the path a shape's name maps to holds no refs", ctx do
      base_path = write_ref_line(ctx)
      stray = PureFileStorage.shape_data_dir(base_path, "stray-file-handle")
      File.mkdir_p!(Path.dirname(stray))
      File.write!(stray, "")

      assert PureFileStorage.referenced_arena_generations(base_path) ==
               MapSet.new([ctx.arena.gen])
    end

    test "a log the scan cannot read still fails it", ctx do
      base_path = write_ref_line(ctx)

      Repatch.patch(PureFileStorage.LogFile, :arena_generations_in_log, fn path ->
        raise File.Error, reason: :eio, action: "read file", path: path
      end)

      assert_raise File.Error, ~r/read file/, fn ->
        PureFileStorage.referenced_arena_generations(base_path)
      end
    end

    defp write_ref_line(ctx) do
      writer = start_writer(ctx)
      {arena, line} = ref_line(ctx.arena, LogOffset.new(10, 0), "k1", :insert, ~S|{"n":1}|)
      PayloadArena.sync!(arena)
      writer = PureFileStorage.append_to_log!([line], writer)
      PureFileStorage.terminate(writer)
      ctx.base_opts.base_path
    end

    # Another shape with a ref into a generation of its own.
    defp write_ref_shape(ctx, shape_handle, discriminator) do
      opts = PureFileStorage.for_shape(shape_handle, ctx.base_opts)
      writer = start_writer(%{opts: opts})
      arena = PayloadArena.open_writer!(ctx.base_opts.base_path, discriminator)
      {arena, line} = ref_line(arena, LogOffset.new(10, 0), "k1", :insert, ~S|{"n":1}|)
      PayloadArena.sync!(arena)
      writer = PureFileStorage.append_to_log!([line], writer)
      PureFileStorage.terminate(writer)
      {PureFileStorage.shape_data_dir(opts), arena.gen}
    end
  end

  describe "payload integrity" do
    setup %{tmp_dir: tmp_dir} do
      writer = PayloadArena.open_writer!(tmp_dir)
      {writer, ref} = PayloadArena.append!(writer, "the-key", ~S|{"amount":42}|)
      PayloadArena.sync!(writer)

      %{ref: ref, gen_path: Path.join([tmp_dir, "payload-arena", "#{writer.gen}.bin"])}
    end

    defp flip_payload_byte(gen_path, offset) do
      contents = File.read!(gen_path)
      <<head::binary-size(^offset), byte, tail::binary>> = contents
      File.write!(gen_path, <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>)
    end

    test "a flipped payload byte is reported rather than served", ctx do
      flip_payload_byte(ctx.gen_path, byte_size("the-key") + 3)

      error =
        assert_raise Storage.Error, fn ->
          [{:ref, ctx.ref}] |> PayloadArena.resolve_refs(ctx.tmp_dir) |> Enum.to_list()
        end

      assert error.kind == :data_loss
      assert error.message =~ "corrupted payload-arena payload"
    end

    test "a flipped payload byte is reported on the inlining read path too", ctx do
      flip_payload_byte(ctx.gen_path, byte_size("the-key") + 3)
      reader = PayloadArena.open_reader(ctx.tmp_dir)

      error = assert_raise Storage.Error, fn -> PayloadArena.read_payload!(reader, ctx.ref) end

      assert error.kind == :data_loss
    end

    test "a flipped key byte is reported on the inlining read path", ctx do
      # payload's, so a payload-only checksum would pass this record.
      flip_payload_byte(ctx.gen_path, 1)
      reader = PayloadArena.open_reader(ctx.tmp_dir)

      error = assert_raise Storage.Error, fn -> PayloadArena.read_payload!(reader, ctx.ref) end

      assert error.kind == :data_loss
      assert error.message =~ "corrupted payload-arena key"
    end

    test "a flipped key byte does not cost the serving path its shape", ctx do
      flip_payload_byte(ctx.gen_path, 1)

      resolved = [{:ref, ctx.ref}] |> PayloadArena.resolve_refs(ctx.tmp_dir) |> Enum.to_list()

      assert resolved == [~S|{"amount":42}|]
    end

    test "a ref written before checksums existed still resolves", ctx do
      <<legacy::binary-size(28), _checksum::32, _key_checksum::32>> = ctx.ref

      resolved = [{:ref, legacy}] |> PayloadArena.resolve_refs(ctx.tmp_dir) |> Enum.to_list()

      assert resolved == [~S|{"amount":42}|]
    end

    test "a ref written before the key checksum existed still reads back", ctx do
      <<payload_only::binary-size(32), _key_checksum::32>> = ctx.ref
      reader = PayloadArena.open_reader(ctx.tmp_dir)

      {_reader, payload} = PayloadArena.read_payload!(reader, payload_only)

      assert payload == {"the-key", ~S|{"amount":42}|}
    end

    test "an intact payload resolves unchanged", ctx do
      assert [~S|{"amount":42}|] ==
               [{:ref, ctx.ref}] |> PayloadArena.resolve_refs(ctx.tmp_dir) |> Enum.to_list()
    end
  end
end
