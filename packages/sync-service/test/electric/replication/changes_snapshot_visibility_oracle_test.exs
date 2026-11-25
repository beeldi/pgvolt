defmodule Electric.Replication.Changes.SnapshotVisibilityOracleTest do
  @moduledoc """
  Differential test of `Transaction.visible_in_snapshot?/2` against real Postgres.

  This predicate decides, for every transaction arriving on the replication stream while a shape's
  initial snapshot is still authoritative, whether the snapshot query already returned that
  transaction's rows. A wrong `true` drops the transaction from the shape log for good.

  The two sides speak different xid widths and that is the whole point of asking Postgres: the
  snapshot comes from `pg_current_snapshot()`, whose xmin, xmax and xip list are `xid8` values
  carrying the wraparound epoch, while the stream carries the bare 32-bit xid out of the logical
  replication `Begin` message. So each case names a transaction by its full `xid8` identity, asks
  Postgres about that, and asks Electric about the 32 bits it would actually see.
  """
  use ExUnit.Case, async: false

  import Support.DbSetup

  alias Electric.Replication.Changes.Transaction

  setup :with_shared_db

  @epoch 4_294_967_296

  @layouts [
    {"in-progress transactions at snapshot time", 100, 200, [100, 150, 199],
     [50, 100, 120, 150, 199, 200, 250]},
    {"no in-progress transactions", 100, 200, [], [50, 150, 200, 250]}
  ]

  for {epoch_name, base} <- [{"epoch 0", 1_000}, {"epoch 1", @epoch + 1_000}],
      {layout_name, xmin_offset, xmax_offset, xip_offsets, probe_offsets} <- @layouts do
    test "#{layout_name}, #{epoch_name}", %{pool: pool} do
      # Arrange
      base = unquote(base)

      snapshot =
        {base + unquote(xmin_offset), base + unquote(xmax_offset),
         Enum.map(unquote(xip_offsets), &(base + &1))}

      for offset <- unquote(probe_offsets) do
        # Act
        transaction_xid = base + offset
        postgres = visible_in_snapshot(pool, transaction_xid, snapshot)
        electric = Transaction.visible_in_snapshot?(rem(transaction_xid, @epoch), snapshot)

        # Assert
        assert electric == postgres,
               "xid #{transaction_xid} in #{snapshot_literal(snapshot)}: " <>
                 "Postgres says #{postgres}, Electric says #{electric}"
      end
    end
  end

  defp visible_in_snapshot(pool, xid, snapshot) do
    scalar(
      pool,
      "SELECT pg_visible_in_snapshot($1::text::xid8, $2::text::pg_snapshot)",
      [to_string(xid), snapshot_literal(snapshot)]
    )
  end

  defp snapshot_literal({xmin, xmax, xip_list}),
    do: "#{xmin}:#{xmax}:#{Enum.join(xip_list, ",")}"
end
