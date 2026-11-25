defmodule Electric.AsyncDeleterTest do
  use ExUnit.Case, async: true
  import Support.ComponentSetup

  alias Electric.AsyncDeleter
  require Repatch
  @moduletag :tmp_dir

  # Use a very short interval to keep tests fast
  @interval 25

  setup [
    :with_stack_id_from_test,
    :with_registry
  ]

  defp create_temp_dir(ctx, name) do
    dir = Path.join(ctx.tmp_dir, name)
    File.mkdir_p!(dir)
    dir
  end

  defp create_temp_file(dir, name, contents \\ "data") do
    path = Path.join(dir, name)
    File.write!(path, contents)
    path
  end

  setup ctx do
    [trash_dir: AsyncDeleter.trash_dir(ctx.tmp_dir, ctx.stack_id)]
  end

  setup ctx do
    if ctx[:add_initial_files] do
      File.mkdir_p!(ctx.trash_dir)
      # create some dummy files in trash dir
      for i <- 1..3 do
        create_temp_file(ctx.trash_dir, "file_#{i}.txt", "initial data #{i}")
      end
    end

    :ok
  end

  setup ctx do
    unless ctx[:blocked_trash_dir] do
      start_link_supervised!(
        {AsyncDeleter,
         stack_id: ctx.stack_id, storage_dir: ctx.tmp_dir, cleanup_interval_ms: @interval}
      )
    end

    :ok
  end

  test "delete moves directory into trash and later removes it", %{
    stack_id: stack_id,
    tmp_dir: base,
    trash_dir: trash_dir
  } do
    dir = create_temp_dir(%{tmp_dir: base}, "to_delete")
    file_path = create_temp_file(dir, "f.txt")

    assert File.exists?(file_path)

    assert :ok = AsyncDeleter.delete(stack_id, dir)

    # Original dir should no longer exist (moved)
    refute File.exists?(dir)

    # Something should now exist in trash dir
    assert File.dir?(trash_dir)
    # Wait a moment to allow rename to complete (should be immediate)
    assert Enum.any?(File.ls!(trash_dir), fn _ -> true end)

    # Before interval passes, the moved content still exists in trash
    moved_entries_before = File.ls!(trash_dir)
    assert moved_entries_before != []

    # After interval passes, cleanup should have run and trash dir should be empty again
    assert File.dir?(trash_dir)
    assert_dir_empty(trash_dir)
  end

  test "multiple deletes are batched into single cleanup", %{
    stack_id: stack_id,
    tmp_dir: base,
    trash_dir: trash_dir
  } do
    d1 = create_temp_dir(%{tmp_dir: base}, "d1")
    d2 = create_temp_dir(%{tmp_dir: base}, "d2")

    f1 = create_temp_file(d1, "a")
    f2 = create_temp_file(d2, "b")

    assert File.exists?(f1)
    assert File.exists?(f2)

    assert :ok = AsyncDeleter.delete(stack_id, d1)
    # quickly queue second before cleanup interval
    assert :ok = AsyncDeleter.delete(stack_id, d2)

    # Both originals gone
    refute File.exists?(d1)
    refute File.exists?(d2)

    # Should contain at least two renamed dirs before cleanup
    entries = File.ls!(trash_dir)
    assert length(entries) >= 2

    # After cleanup interval, trash dir should be empty
    assert_dir_empty(trash_dir)
  end

  test "deleting missing path returns ok and does nothing", %{
    tmp_dir: base,
    stack_id: stack_id,
    trash_dir: trash_dir
  } do
    path = Path.join(base, "nonexistent_#{System.unique_integer()}")
    refute File.exists?(path)

    assert :ok = AsyncDeleter.delete(stack_id, path)
    # nothing to assert further; just ensure no crash and no entries after interval
    assert_dir_empty(trash_dir)
  end

  test "can manage concurrent deletes while cleaning up", %{
    stack_id: stack_id,
    tmp_dir: base,
    trash_dir: trash_dir
  } do
    d1 = create_temp_dir(%{tmp_dir: base}, "d1")

    stream =
      Task.async_stream(1..1000, fn i ->
        f = create_temp_file(d1, "tmp_#{i}")
        sleep_time = round(Enum.random(1..10) * 0.1 * (2 * @interval))
        Process.sleep(sleep_time)
        assert :ok = AsyncDeleter.delete(stack_id, f)
        sleep_time
      end)

    Enum.to_list(stream)

    # ensure all files are deleted
    assert_dir_empty(d1, 0)
    assert_dir_empty(trash_dir, 5_000)
  end

  @tag add_initial_files: true
  test "performs initial cleanup", %{trash_dir: trash_dir} do
    # ensure no files exist after startup
    assert_dir_empty(trash_dir)
  end

  # A trash directory that cannot be created is the shape a full volume takes: this process is the
  # only thing that empties the trash, so crashing on it means the disk never gets reclaimed and the
  # stack supervisor goes down with us. Blocking the path with a regular file reproduces that
  # without needing a real ENOSPC, and it fails for root too, unlike a permissions-based fixture.
  @tag blocked_trash_dir: true
  test "starts idle when the trash directory cannot be created", %{
    stack_id: stack_id,
    tmp_dir: base,
    trash_dir: trash_dir
  } do
    File.mkdir_p!(Path.dirname(Path.dirname(trash_dir)))
    File.write!(Path.dirname(trash_dir), "not a directory")

    pid =
      start_link_supervised!(
        {AsyncDeleter, stack_id: stack_id, storage_dir: base, cleanup_interval_ms: @interval}
      )

    assert Process.alive?(pid)

    # And it refuses the delete out loud rather than reporting the untouched path as reclaimed.
    dir = create_temp_dir(%{tmp_dir: base}, "unreclaimable")
    assert {:error, _reason} = AsyncDeleter.delete(stack_id, dir)
    assert File.exists?(dir)
  end

  test "recreates a trash directory that went missing instead of dropping the delete", %{
    stack_id: stack_id,
    tmp_dir: base,
    trash_dir: trash_dir
  } do
    dir = create_temp_dir(%{tmp_dir: base}, "to_delete")
    create_temp_file(dir, "f.txt")
    File.rm_rf!(trash_dir)

    assert :ok = AsyncDeleter.delete(stack_id, dir)

    refute File.exists?(dir)
    assert_dir_empty(trash_dir)
  end

  describe "a path on another filesystem than the stack's trash" do
    test "goes to a trash in its root", %{stack_id: stack_id, tmp_dir: base, trash_dir: trash_dir} do
      root = create_temp_dir(%{tmp_dir: base}, "store")
      dir = create_temp_dir(%{tmp_dir: root}, "ab/cd/shape")
      create_temp_file(dir, "f.txt")
      refuse_renames_into(trash_dir, :exdev)

      assert :ok = AsyncDeleter.delete(stack_id, dir, root)

      assert File.ls!(Path.join(root, "ab/cd")) == []
      assert_dir_empty(Path.join(root, ".electric_trash"))
    end

    test "gives its entries to a trash in it when it is the root", %{
      stack_id: stack_id,
      tmp_dir: base,
      trash_dir: trash_dir
    } do
      root = create_temp_dir(%{tmp_dir: base}, "store")
      create_temp_file(root, "f.txt")
      create_temp_file(create_temp_dir(%{tmp_dir: root}, "ab/cd/shape"), "g.txt")
      refuse_renames_into(trash_dir, :ebusy)

      assert :ok = AsyncDeleter.delete(stack_id, root, root)

      assert File.ls!(root) == [".electric_trash"]
      assert_dir_empty(Path.join(root, ".electric_trash"))
    end

    test "goes to a trash in its root once a recreated trash cannot take it either", %{
      stack_id: stack_id,
      tmp_dir: base,
      trash_dir: trash_dir
    } do
      root = create_temp_dir(%{tmp_dir: base}, "store")
      dir = create_temp_dir(%{tmp_dir: root}, "shape")
      File.rm_rf!(trash_dir)
      refuse_renames_into(trash_dir, :exdev)

      assert :ok = AsyncDeleter.delete(stack_id, dir, root)

      refute File.exists?(dir)
      assert File.dir?(trash_dir)
      assert_dir_empty(Path.join(root, ".electric_trash"))
    end

    test "stays in place without a root", %{
      stack_id: stack_id,
      tmp_dir: base,
      trash_dir: trash_dir
    } do
      dir = create_temp_dir(%{tmp_dir: base}, "shape")
      refuse_renames_into(trash_dir, :exdev)

      assert {:error, :exdev} = AsyncDeleter.delete(stack_id, dir)
      assert File.dir?(dir)
    end
  end

  # A volume mounted for the shapes under the storage directory: a rename into the stack's trash
  # crosses filesystems. Into a trash that is missing it fails on the missing directory first.
  defp refuse_renames_into(trash_dir, reason) do
    Repatch.patch(AsyncDeleter, :do_rename, fn path, dir, attempts ->
      cond do
        dir != trash_dir ->
          Repatch.real(Repatch.private(AsyncDeleter.do_rename(path, dir, attempts)))

        File.dir?(dir) ->
          {:error, reason}

        true ->
          {:error, :enoent}
      end
    end)
  end

  defp assert_dir_empty(dir, timeout \\ ExUnit.configuration()[:assert_receive_timeout]) do
    assert File.ls!(dir) == []
  rescue
    e in ExUnit.AssertionError ->
      if timeout <= 0 do
        reraise e, __STACKTRACE__
      else
        Process.sleep(@interval)
        assert_dir_empty(dir, timeout - @interval)
      end
  end
end
