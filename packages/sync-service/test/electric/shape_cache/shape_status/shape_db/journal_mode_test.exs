defmodule Electric.ShapeCache.ShapeStatus.ShapeDb.JournalModeTest do
  # The journal mode is persisted in the database file, so it has to be applied when an existing
  # file is opened, not only when it is created.
  use ExUnit.Case, async: true

  import Support.ComponentSetup

  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Connection

  @moduletag :tmp_dir

  setup [:with_stack_id_from_test]

  defp journal_mode(conn) do
    {:ok, [mode]} = Connection.fetch_one(conn, "PRAGMA journal_mode", [])
    mode
  end

  defp create_database(ctx, opts) do
    opts = Keyword.merge([storage_dir: ctx.tmp_dir], opts)
    {:ok, conn} = Connection.open(opts)
    {:ok, _version} = Connection.migrate(conn, opts)
    mode = journal_mode(conn)
    :ok = Connection.close(conn)
    mode
  end

  test "reopening a database created in WAL mode sets journal_mode=DELETE in exclusive mode",
       ctx do
    assert "wal" = create_database(ctx, exclusive_mode: false)

    assert "delete" = create_database(ctx, exclusive_mode: true)
  end

  test "reopening a database created in DELETE mode sets journal_mode=WAL when not exclusive",
       ctx do
    assert "delete" = create_database(ctx, exclusive_mode: true)

    assert "wal" = create_database(ctx, exclusive_mode: false)
  end
end
