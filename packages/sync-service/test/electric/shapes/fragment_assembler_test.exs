defmodule Electric.Shapes.FragmentAssemblerTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.FramedBlock
  alias Electric.Replication.Changes.PreparedChange
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.LogOffset
  alias Electric.Shapes.FragmentAssembler

  @relation {"public", "items"}

  defp change(i) do
    %Changes.NewRecord{
      relation: @relation,
      record: %{"id" => "r#{i}"},
      log_offset: LogOffset.new(100, i),
      key: ~s|"public"."items"/"r#{i}"|
    }
  end

  defp prepared(i) do
    json = ~s|{"payload":#{i}}|

    %PreparedChange{
      change: change(i),
      log_items: [{LogOffset.new(100, i), "k#{i}", :insert, json}],
      total_size: byte_size(json)
    }
  end

  defp fragment(changes, opts \\ []) do
    %TransactionFragment{
      xid: 7,
      lsn: 100,
      last_log_offset: LogOffset.new(100, 99),
      has_begin?: Keyword.get(opts, :begin?, true),
      commit: Keyword.get(opts, :commit, %Changes.Commit{}),
      changes: changes,
      affected_relations: MapSet.new([@relation]),
      change_count: length(changes)
    }
  end

  defp decision(i, shape_ids), do: {change(i), [{prepared(i), shape_ids}], []}

  defp assemble(assembler, decisions) do
    FragmentAssembler.assemble(assembler, fragment([]), decisions)
  end

  test "split transactions retain full transaction positions in data and commit-only fragments" do
    transaction = %{fragment([change(1)], commit: nil) | full_xid: 4_294_967_303}

    {data, assembler} =
      FragmentAssembler.assemble(FragmentAssembler.new(), transaction, [decision(1, ["s1"])])

    assert data["s1"].full_xid == 4_294_967_303

    commit = %{fragment([], begin?: false) | full_xid: 4_294_967_303}
    {committed, _assembler} = FragmentAssembler.assemble(assembler, commit, [])

    assert committed["s1"].full_xid == 4_294_967_303
    assert committed["s1"].changes == []
    assert %Changes.Commit{} = committed["s1"].commit
  end

  test "a run of identical-membership prepared changes flushes as one block plus the trailer" do
    assembler = FragmentAssembler.new(blocks?: true)
    ids = ["s1", "s2", "s3"]

    {fragments, _assembler} = assemble(assembler, Enum.map(1..6, &decision(&1, ids)))

    for id <- ids do
      %TransactionFragment{changes: changes, change_count: count} = fragments[id]

      assert [%FramedBlock{count: 5} = block, %PreparedChange{} = trailer] = changes
      assert count == 6
      assert block.first_offset == LogOffset.new(100, 1)
      assert block.last_offset == LogOffset.new(100, 5)
      assert trailer.log_items == [{LogOffset.new(100, 6), "k6", :insert, ~s|{"payload":6}|}]
    end
  end

  test "the identical block struct is shared across every member" do
    assembler = FragmentAssembler.new(blocks?: true)
    ids = ["s1", "s2", "s3"]

    {fragments, _} = assemble(assembler, Enum.map(1..5, &decision(&1, ids)))

    blocks = Enum.map(ids, fn id -> hd(fragments[id].changes) end)
    assert [b, b, b] = blocks
  end

  test "runs below the minimum stay per-change" do
    assembler = FragmentAssembler.new(blocks?: true)
    ids = ["s1", "s2"]

    {fragments, _} = assemble(assembler, Enum.map(1..2, &decision(&1, ids)))

    assert [%PreparedChange{}, %PreparedChange{}] = fragments["s1"].changes
    assert fragments["s1"].change_count == 2
  end

  test "a membership change breaks the run" do
    assembler = FragmentAssembler.new(blocks?: true)

    decisions =
      Enum.map(1..4, &decision(&1, ["s1", "s2"])) ++
        Enum.map(5..8, &decision(&1, ["s1", "s3"]))

    {fragments, _} = assemble(assembler, decisions)

    assert [
             %FramedBlock{count: 3, first_offset: %{op_offset: 1}},
             %PreparedChange{},
             %FramedBlock{count: 3, first_offset: %{op_offset: 5}},
             %PreparedChange{}
           ] = fragments["s1"].changes

    assert fragments["s1"].change_count == 8
    assert [%FramedBlock{count: 3}, %PreparedChange{}] = fragments["s2"].changes
    assert [%FramedBlock{count: 3}, %PreparedChange{}] = fragments["s3"].changes
  end

  test "an interleaved unprepared change breaks the run and keeps per-shape order" do
    assembler = FragmentAssembler.new(blocks?: true)
    ids = ["s1", "s2"]

    decisions =
      Enum.map(1..4, &decision(&1, ids)) ++
        [{change(5), [], ["s1"]}] ++
        Enum.map(6..9, &decision(&1, ids))

    {fragments, _} = assemble(assembler, decisions)

    assert [
             %FramedBlock{count: 3},
             %PreparedChange{},
             %Changes.NewRecord{},
             %FramedBlock{count: 3},
             %PreparedChange{}
           ] = fragments["s1"].changes

    assert fragments["s1"].change_count == 9
    assert fragments["s2"].change_count == 8
  end

  test "an unblockable member keeps the whole class per-change" do
    assembler =
      FragmentAssembler.new(blocks?: true)
      |> FragmentAssembler.mark_unblockable(["s2"])

    ids = ["s1", "s2"]
    {fragments, _} = assemble(assembler, Enum.map(1..6, &decision(&1, ids)))

    assert Enum.all?(fragments["s1"].changes, &match?(%PreparedChange{}, &1))
    assert Enum.all?(fragments["s2"].changes, &match?(%PreparedChange{}, &1))
  end

  test "blocks disabled keeps the historical per-change output" do
    assembler = FragmentAssembler.new()
    ids = ["s1", "s2", "s3"]

    {fragments, _} = assemble(assembler, Enum.map(1..6, &decision(&1, ids)))

    assert Enum.all?(fragments["s1"].changes, &match?(%PreparedChange{}, &1))
    assert fragments["s1"].change_count == 6
  end

  test "the framed bytes equal per-item make_entry output" do
    assembler = FragmentAssembler.new(blocks?: true)
    ids = ["s1", "s2"]

    {fragments, _} = assemble(assembler, Enum.map(1..5, &decision(&1, ids)))
    [%FramedBlock{} = block, _trailer] = fragments["s1"].changes

    expected =
      Enum.map(1..4, fn i ->
        [{offset, key, _op, json}] = prepared(i).log_items

        {iodata, _size} =
          Electric.ShapeCache.PureFileStorage.LogFile.make_entry(
            {offset, byte_size(key), key, ?i, 0, byte_size(json), json}
          )

        IO.iodata_to_binary(iodata)
      end)
      |> IO.iodata_to_binary()

    assert block.frame == expected
    assert op = elem(elem(block.entries_meta, 0), 0)
    assert op == LogOffset.new(100, 1)
  end
end
