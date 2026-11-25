defmodule Electric.PersistentKV.FilesystemTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Electric.PersistentKV, as: KV
  alias Electric.Replication.PersistentReplicationState

  @moduletag :tmp_dir
  @key "some/thing/here.data"

  setup ctx do
    %{kv: Electric.PersistentKV.Filesystem.new!(root: ctx.tmp_dir)}
  end

  describe "get/2 on a damaged file" do
    test "a half-written value is not reported as a successful read", %{kv: kv, tmp_dir: tmp_dir} do
      :ok = KV.set(kv, @key, %{offset: 12_345})
      path = Path.join(tmp_dir, @key)
      assert {:ok, %{offset: 12_345}} = KV.get(kv, @key)

      truncate!(path, div(File.stat!(path).size, 2))

      assert {:error, :corrupted} = KV.get(kv, @key)
    end

    test "an empty file is not reported as a successful read", %{kv: kv, tmp_dir: tmp_dir} do
      :ok = KV.set(kv, @key, %{offset: 12_345})
      truncate!(Path.join(tmp_dir, @key), 0)

      assert {:error, :corrupted} = KV.get(kv, @key)
    end

    test "a value written before this module encoded still reads back", %{
      kv: kv,
      tmp_dir: tmp_dir
    } do
      path = Path.join(tmp_dir, @key)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "a plain string from before term_to_binary")

      assert {:ok, "a plain string from before term_to_binary"} = KV.get(kv, @key)
    end
  end

  # Every caller of `get/2` guards on a shape, and a damaged read used to arrive wearing one of
  # those shapes rather than as an error. These cover the recovery each caller needs now that it
  # arrives as one.
  describe "callers of get/2" do
    test "a damaged installation id is replaced, not adopted", %{kv: kv, tmp_dir: tmp_dir} do
      stored = Electric.Config.persist_installation_id(kv, "the-original-id")
      assert stored == "the-original-id"

      truncate!(Path.join(tmp_dir, "electric_installation_id"), 3)

      {replacement, log} =
        with_log(fn -> Electric.Config.persist_installation_id(kv, "a-fresh-id") end)

      assert replacement == "a-fresh-id"
      assert log =~ "Could not read the stored installation id"
      assert Electric.Config.installation_id!(kv) == "a-fresh-id"
    end

    test "damaged tracked relations fall back to empty", %{kv: kv, tmp_dir: tmp_dir} do
      opts = [persistent_kv: kv, stack_id: "filesystem_test_stack"]
      relations = %{table_to_id: %{{"public", "items"} => 1}, id_to_table_info: %{}}
      :ok = PersistentReplicationState.set_tracked_relations(relations, opts)
      assert PersistentReplicationState.get_tracked_relations(opts) == relations

      truncate!(Path.join(tmp_dir, "filesystem_test_stack:tracked_relations"), 8)

      {loaded, log} =
        with_log(fn -> PersistentReplicationState.get_tracked_relations(opts) end)

      assert loaded == %{table_to_id: %{}, id_to_table_info: %{}}
      assert log =~ "Could not read the tracked relations"
    end
  end

  describe "set/3 when the write cannot complete" do
    test "a failed rename leaves no orphan temp file behind", %{kv: kv, tmp_dir: tmp_dir} do
      # write reaches the temp file but never lands.
      blocked = Path.join(tmp_dir, "blocked.data")
      File.mkdir_p!(blocked)
      File.write!(Path.join(blocked, "child"), "x")

      result = KV.set(kv, "blocked.data", %{offset: 12_345})

      assert {:error, _} = result

      assert Enum.filter(File.ls!(tmp_dir), &String.starts_with?(&1, ".")) == [],
             "the temp dotfile must not survive a write that could not be renamed into place"
    end
  end

  defp truncate!(path, size) do
    File.open!(path, [:read, :write, :raw], fn file ->
      {:ok, ^size} = :file.position(file, size)
      :ok = :file.truncate(file)
    end)
  end
end
