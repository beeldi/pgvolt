defmodule Electric.Shapes.Consumer.TransactionConverterTest do
  use ExUnit.Case, async: true

  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.Transaction
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Consumer.TransactionConverter
  alias Electric.Shapes.Shape

  @inspector Support.StubInspector.new(
               tables: ["child"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0, type_id: {20, 1}},
                 %{name: "parent_id", type: "int8", pk_position: nil, type_id: {20, 1}},
                 %{name: "name", type: "text", pk_position: nil, type_id: {28, 1}}
               ]
             )

  test "marks only the final emitted change as last" do
    txn = %Transaction{
      xid: 7,
      lsn: lsn(7),
      last_log_offset: Electric.Replication.LogOffset.new(lsn(7), 1),
      changes: [
        child_insert("1"),
        child_insert("2")
      ]
    }

    assert {:ok,
            [
              %Effects.AppendChanges{
                xid: 7,
                changes: [
                  %Changes.NewRecord{record: %{"id" => "1"}, last?: false},
                  %Changes.NewRecord{record: %{"id" => "2"}, last?: true}
                ]
              }
            ]} =
             TransactionConverter.transaction_to_effects(
               txn,
               simple_shape(),
               stack_id: "stack-id",
               shape_handle: "shape-handle"
             )
  end

  test "converts updates through Shape.convert_change and marks the final change" do
    txn = %Transaction{
      xid: 8,
      lsn: lsn(8),
      last_log_offset: Electric.Replication.LogOffset.new(lsn(8), 1),
      changes: [child_update("1", "2")]
    }

    assert {:ok,
            [
              %Effects.AppendChanges{
                xid: 8,
                changes: [
                  %Changes.NewRecord{record: %{"id" => "2"}, last?: true}
                ]
              }
            ]} =
             TransactionConverter.transaction_to_effects(
               txn,
               filtered_shape(),
               stack_id: "stack-id",
               shape_handle: "shape-handle"
             )
  end

  test "surfaces truncate before emitting changes" do
    txn = %Transaction{
      xid: 9,
      lsn: lsn(9),
      last_log_offset: Electric.Replication.LogOffset.new(lsn(9), 1),
      changes: [%Changes.TruncatedRelation{relation: {"public", "child"}}]
    }

    assert {:error, {:truncate, 9}} =
             TransactionConverter.transaction_to_effects(txn, simple_shape())
  end

  test "returns no append effects for an empty converted transaction" do
    txn = %Transaction{
      xid: 10,
      lsn: lsn(10),
      last_log_offset: Electric.Replication.LogOffset.new(lsn(10), 1),
      changes: []
    }

    assert {:ok, []} = TransactionConverter.transaction_to_effects(txn, simple_shape())
  end

  test "converts multiple transactions into ordered append effects" do
    txns = [
      %Transaction{
        xid: 11,
        lsn: lsn(11),
        last_log_offset: Electric.Replication.LogOffset.new(lsn(11), 1),
        changes: [child_insert("1")]
      },
      %Transaction{
        xid: 12,
        lsn: lsn(12),
        last_log_offset: Electric.Replication.LogOffset.new(lsn(12), 1),
        changes: []
      },
      %Transaction{
        xid: 13,
        lsn: lsn(13),
        last_log_offset: Electric.Replication.LogOffset.new(lsn(13), 1),
        changes: [child_insert("2")]
      }
    ]

    assert {:ok,
            [
              %Effects.AppendChanges{
                xid: 11,
                changes: [%Changes.NewRecord{record: %{"id" => "1"}, last?: true}]
              },
              %Effects.AppendChanges{
                xid: 13,
                changes: [%Changes.NewRecord{record: %{"id" => "2"}, last?: true}]
              }
            ]} =
             TransactionConverter.transactions_to_effects(
               txns,
               simple_shape(),
               stack_id: "stack-id",
               shape_handle: "shape-handle"
             )
  end

  test "keeps a framed block mid-transaction and marks the trailing change last" do
    txn = %Transaction{
      xid: 14,
      lsn: lsn(14),
      last_log_offset: Electric.Replication.LogOffset.new(lsn(14), 1),
      changes: [framed_block(), child_insert("2")]
    }

    assert {:ok,
            [
              %Effects.AppendChanges{
                xid: 14,
                changes: [
                  %Changes.FramedBlock{count: 2},
                  %Changes.NewRecord{record: %{"id" => "2"}, last?: true}
                ]
              }
            ]} =
             TransactionConverter.transaction_to_effects(
               txn,
               simple_shape(),
               stack_id: "stack-id",
               shape_handle: "shape-handle"
             )
  end

  test "raises when a framed block is the transaction's final change" do
    txn = %Transaction{
      xid: 15,
      lsn: lsn(15),
      last_log_offset: Electric.Replication.LogOffset.new(lsn(15), 1),
      changes: [child_insert("1"), framed_block()]
    }

    assert_raise RuntimeError, ~r/framed block was the final change/, fn ->
      TransactionConverter.transaction_to_effects(
        txn,
        simple_shape(),
        stack_id: "stack-id",
        shape_handle: "shape-handle"
      )
    end
  end

  defp lsn(value), do: Lsn.from_integer(value)

  defp simple_shape do
    Shape.new!("child", inspector: @inspector)
  end

  defp filtered_shape do
    Shape.new!("child", where: "id = 2", inspector: @inspector)
  end

  defp child_update(old_id, new_id) do
    %Changes.UpdatedRecord{
      relation: {"public", "child"},
      old_record: %{"id" => old_id, "parent_id" => "1", "name" => "child-#{old_id}"},
      record: %{"id" => new_id, "parent_id" => "1", "name" => "child-#{new_id}"},
      changed_columns: MapSet.new(["id"])
    }
  end

  defp child_insert(id) do
    %Changes.NewRecord{
      relation: {"public", "child"},
      record: %{"id" => id, "parent_id" => "1", "name" => "child-#{id}"}
    }
  end

  defp framed_block do
    %Changes.FramedBlock{
      frame: "",
      frame_size: 0,
      count: 2,
      payload_size: 0,
      ets_lines: [],
      entries_meta: {},
      first_offset: Electric.Replication.LogOffset.new(lsn(0), 0),
      last_offset: Electric.Replication.LogOffset.new(lsn(0), 1)
    }
  end
end
