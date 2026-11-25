defmodule Electric.Postgres.FullTransactionId do
  alias Electric.Postgres.Lsn

  @modulus 0x100000000
  @half_range div(@modulus, 2)

  def fetch_anchor!(pool, source) do
    {:ok, anchor} =
      Postgrex.transaction(
        pool,
        fn connection ->
          Postgrex.query!(
            connection,
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY",
            []
          )

          %{rows: [[fence]]} =
            Postgrex.query!(connection, "SELECT pg_current_wal_lsn()::text", [])

          %{rows: rows} =
            Postgrex.query!(
              connection,
              """
              SELECT pg_current_snapshot(), catalog_xmin::text, confirmed_flush_lsn::text
              FROM pg_replication_slots
              WHERE slot_name = $1 AND slot_type = 'logical' AND active AND active_pid = $2
                AND database = current_database() AND catalog_xmin IS NOT NULL
                AND confirmed_flush_lsn IS NOT NULL AND wal_status <> 'lost'
                AND NOT pg_is_in_recovery()
                AND (pg_control_system()).system_identifier::text = $3
                AND left(pg_walfile_name(pg_current_wal_lsn()), 8) = upper(lpad(to_hex($4::text::bigint), 8, '0'))
                AND current_database() = $5
                AND datoid::text = $6
              """,
              [
                source.slot_name,
                source.backend_pid,
                source.system_identifier,
                source.timeline_id,
                source.database,
                source.database_oid
              ]
            )

          case rows do
            [[{_, xmax, _}, catalog_xmin, confirmed_lsn]] ->
              anchor!(%{
                xmax: xmax,
                catalog_xmin: String.to_integer(catalog_xmin),
                fence_lsn: parse_lsn(fence),
                confirmed_lsn: parse_lsn(confirmed_lsn)
              })

            _ ->
              raise ArgumentError,
                    "full transaction provenance requires the matching active primary slot"
          end
        end,
        timeout: 5_000
      )

    :telemetry.execute([:electric, :postgres, :full_transaction_id, :anchor], %{count: 1}, %{})
    anchor
  end

  def resolve(_xid, _transaction_lsn, nil), do: :refresh

  def resolve(xid, transaction_lsn, anchor) when xid >= 3 and xid < @modulus do
    full_xid = expand_around(xid, anchor.xmax)

    # PostgreSQL skips commit-record starts strictly before confirmed_flush_lsn.
    if transaction_lsn >= anchor.confirmed_lsn and transaction_lsn <= anchor.fence_lsn and
         full_xid >= anchor.retained_xmin and full_xid < anchor.xmax do
      {:ok, full_xid}
    else
      :refresh
    end
  end

  def anchor!(%{
        xmax: xmax,
        catalog_xmin: catalog_xmin,
        confirmed_lsn: confirmed_lsn,
        fence_lsn: fence_lsn
      }) do
    retained_xmin = expand_around(catalog_xmin, xmax)

    if catalog_xmin >= 3 and catalog_xmin < @modulus and retained_xmin >= 3 and
         retained_xmin <= xmax and xmax - retained_xmin < @half_range do
      %{
        xmax: xmax,
        retained_xmin: retained_xmin,
        confirmed_lsn: confirmed_lsn,
        fence_lsn: fence_lsn
      }
    else
      raise ArgumentError, "slot retention cannot establish a full transaction epoch"
    end
  end

  defp parse_lsn(lsn), do: lsn |> Lsn.from_string() |> Lsn.to_integer()

  defp expand_around(xid, boundary) do
    difference = Integer.mod(xid - boundary, @modulus)
    boundary + if(difference >= @half_range, do: difference - @modulus, else: difference)
  end
end
