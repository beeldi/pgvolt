defmodule Electric.ShapeCache.ShapeStatus.ShapeDb.ReadAheadTest do
  # The integrity check reads every page of the shape store in b-tree order. On a cold page cache
  # that is one-at-a-time 4 KB reads, minutes for a store with many shapes on a disk limited in
  # reads per second, while the same file reads front to back far faster, so the file is read
  # through sequentially first.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Support.ComponentSetup

  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Connection

  @moduletag :tmp_dir

  setup [:with_stack_id_from_test]

  describe "read_ahead/1" do
    test "reads the file to its end, whatever its size relative to the read size", ctx do
      path = Path.join(ctx.tmp_dir, "store.sqlite")
      size = 9 * 1024 * 1024 + 123
      File.write!(path, :binary.copy(<<0>>, size))

      assert Connection.read_ahead(path) == size
    end

    test "has nothing to read for an in-memory database or a file not created yet", ctx do
      assert Connection.read_ahead(":memory:") == 0
      assert Connection.read_ahead(Path.join(ctx.tmp_dir, "missing.sqlite")) == 0
    end
  end

  @tag shape_db_opts: [exclusive_mode: true]
  test "a store is read through before its integrity check", ctx do
    log = capture_log(fn -> with_shape_db(ctx) end)

    assert log =~
             ~r/Boot step shape_db\.read_ahead took \d+ms.*Boot step shape_db\.integrity_check took \d+ms/s
  end
end
