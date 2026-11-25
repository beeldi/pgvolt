defmodule Electric.Replication.TxnJournalRestartPrepareTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.LogOffset
  alias Electric.Replication.TxnJournal

  @moduletag :tmp_dir

  defp fragment(tx_offset) do
    %TransactionFragment{
      xid: tx_offset,
      lsn: Electric.Postgres.Lsn.from_integer(tx_offset),
      last_log_offset: LogOffset.new(tx_offset, 0),
      has_begin?: true,
      commit: %Changes.Commit{},
      changes: [
        %Changes.NewRecord{
          relation: {"public", "test_table"},
          record: %{"id" => "#{tx_offset}"},
          log_offset: LogOffset.new(tx_offset, 0)
        }
      ],
      change_count: 1
    }
  end

  defp relation do
    %Changes.Relation{id: 1234, schema: "public", table: "test_table", columns: []}
  end

  defp seal_generation(base_path, events, boundary) do
    writer = TxnJournal.open_writer!(base_path)
    writer = Enum.reduce(events, writer, &TxnJournal.append_event!(&2, &1))

    case boundary do
      nil -> TxnJournal.sync!(writer)
      offset -> TxnJournal.sync!(writer, offset)
    end

    writer.gen
  end

  setup %{tmp_dir: tmp_dir} do
    g1 = seal_generation(tmp_dir, [fragment(10)], 10)
    g2 = seal_generation(tmp_dir, [{:idle_slot_boundary, 40}], 40)
    g3 = seal_generation(tmp_dir, [relation()], nil)

    %{base: tmp_dir, g1: g1, g2: g2, g3: g3}
  end

  test "prepared restart state matches the per-generation oracle", ctx do
    restart = TxnJournal.open_writer!(ctx.base)

    expected =
      ctx.base
      |> TxnJournal.list_generations()
      |> Enum.reject(&(&1 == restart.gen))
      |> Enum.map(fn gen -> {gen, TxnJournal.last_commit_tx_offset(ctx.base, [gen]) || -1} end)

    restart = TxnJournal.adopt_generations!(restart, ctx.base)

    assert restart.closed_gens == expected

    assert restart.closed_gens == [
             {ctx.g1, 10},
             {ctx.g2, 40},
             {ctx.g3, -1}
           ]
  end

  test "a generation holding no commit is adopted as reclaimable", ctx do
    restart = ctx.base |> TxnJournal.open_writer!() |> TxnJournal.adopt_generations!(ctx.base)

    assert {ctx.g3, -1} in restart.closed_gens
  end

  test "an idle-slot-boundary generation carries its boundary offset", ctx do
    restart = ctx.base |> TxnJournal.open_writer!() |> TxnJournal.adopt_generations!(ctx.base)

    assert {ctx.g2, 40} in restart.closed_gens
  end

  test "the live generation is not adopted", ctx do
    restart = TxnJournal.open_writer!(ctx.base)

    restart = TxnJournal.adopt_generations!(restart, ctx.base)

    refute Enum.any?(restart.closed_gens, fn {gen, _} -> gen == restart.gen end)
    assert length(restart.closed_gens) == 3
  end

  test "adopted generations are prepended to existing closed generations in disk order", ctx do
    restart = TxnJournal.open_writer!(ctx.base)
    restart = %{restart | closed_gens: [{999, 5}]}

    restart = TxnJournal.adopt_generations!(restart, ctx.base)

    assert restart.closed_gens == [
             {ctx.g1, 10},
             {ctx.g2, 40},
             {ctx.g3, -1},
             {999, 5}
           ]
  end
end
