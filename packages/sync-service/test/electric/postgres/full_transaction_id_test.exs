defmodule Electric.Postgres.FullTransactionIdTest do
  use ExUnit.Case, async: true
  alias Electric.Postgres.FullTransactionId

  test "resolves both sides of wrap from the same retained interval" do
    anchor =
      FullTransactionId.anchor!(%{
        xmax: 4_294_967_310,
        catalog_xmin: 4_294_967_290,
        confirmed_lsn: 100,
        fence_lsn: 300
      })

    assert FullTransactionId.resolve(4_294_967_294, 200, anchor) == {:ok, 4_294_967_294}
    assert FullTransactionId.resolve(5, 200, anchor) == {:ok, 4_294_967_301}
  end

  test "fresh retained slot interval resolves several quiet epochs" do
    anchor =
      FullTransactionId.anchor!(%{
        xmax: 12_884_902_088,
        catalog_xmin: 100,
        confirmed_lsn: 100,
        fence_lsn: 300
      })

    assert FullTransactionId.resolve(150, 200, anchor) == {:ok, 12_884_902_038}
  end

  test "does not reuse an ancient anchor beyond its WAL fence even for the same raw xid" do
    anchor =
      FullTransactionId.anchor!(%{
        xmax: 200,
        catalog_xmin: 100,
        confirmed_lsn: 100,
        fence_lsn: 300
      })

    assert FullTransactionId.resolve(150, 200, anchor) == {:ok, 150}
    assert FullTransactionId.resolve(150, 301, anchor) == :refresh
    assert FullTransactionId.resolve(150, 100, anchor) == {:ok, 150}
    assert FullTransactionId.resolve(150, 99, anchor) == :refresh
  end

  test "can reuse a bounded anchor for every covered transaction without time expiry" do
    anchor =
      FullTransactionId.anchor!(%{
        xmax: 300,
        catalog_xmin: 100,
        confirmed_lsn: 100,
        fence_lsn: 500
      })

    Enum.each(101..299, fn xid ->
      assert FullTransactionId.resolve(xid, 200, anchor) == {:ok, xid}
    end)
  end

  test "waits for ProcArray completion even when commit WAL is already covered" do
    anchor =
      FullTransactionId.anchor!(%{
        xmax: 200,
        catalog_xmin: 100,
        confirmed_lsn: 100,
        fence_lsn: 300
      })

    assert FullTransactionId.resolve(200, 200, anchor) == :refresh
    assert FullTransactionId.resolve(90, 200, anchor) == :refresh
    assert FullTransactionId.resolve(150, 200, nil) == :refresh
  end

  test "rejects intervals spanning half the xid space and reserved catalog xmin" do
    assert_raise ArgumentError, fn ->
      FullTransactionId.anchor!(%{
        xmax: 2_147_483_748,
        catalog_xmin: 100,
        confirmed_lsn: 100,
        fence_lsn: 300
      })
    end

    assert_raise ArgumentError, fn ->
      FullTransactionId.anchor!(%{xmax: 200, catalog_xmin: 2, confirmed_lsn: 100, fence_lsn: 300})
    end
  end
end
