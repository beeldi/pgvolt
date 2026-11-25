defmodule Electric.LogItemsTest do
  use ExUnit.Case, async: true
  alias Electric.LogItems
  alias Electric.Replication.LogOffset
  alias Electric.Replication.Changes

  describe "encode_changes/4" do
    setup do
      shape = %Electric.Shapes.Shape{
        root_table: {"public", "test"},
        root_table_id: 1,
        root_pk: ["pk"],
        source_positions: :full_xid
      }

      change = %Changes.UpdatedRecord{
        old_key: "old_key",
        key: "new_key",
        old_record: %{"pk" => "1"},
        record: %{"pk" => "2"},
        log_offset: LogOffset.first(),
        relation: shape.root_table
      }

      [shape: shape, change: change]
    end

    test "encodes both sides of a key change with full provenance and exact size", context do
      {lines, size} = LogItems.encode_changes([context.change], 100, 4_294_967_396, context.shape)

      assert [{_, "old_key", :delete, deleted}, {_, "new_key", :insert, inserted}] = lines
      assert size == byte_size(deleted) + byte_size(inserted)

      for json <- [deleted, inserted] do
        assert %{"headers" => %{"txids" => [100], "txids8" => ["4294967396"]}} =
                 Jason.decode!(json)
      end
    end

    test "requires full provenance before encoding a full-mode shape", context do
      assert_raise ArgumentError, ~r/full transaction provenance/, fn ->
        LogItems.encode_changes([context.change], 100, nil, context.shape)
      end
    end

    test "keeps legacy rows unchanged, including shapes restored without a mode", context do
      legacy = Map.delete(context.shape, :source_positions)
      {lines, _size} = LogItems.encode_changes([context.change], 100, nil, legacy)

      for {_, _, _, json} <- lines do
        refute Map.has_key?(Jason.decode!(json)["headers"], "txids8")
      end

      assert LogItems.encode_changes([], 100, nil, legacy) == {[], 0}
    end
  end

  describe "from_change/3" do
    test "stores an entire `NewRecord` value" do
      record = %Changes.NewRecord{
        key: "my_key",
        record: %{"pk" => "10", "hello" => "world"},
        log_offset: LogOffset.first(),
        relation: {"public", "test"}
      }

      assert LogItems.from_change(record, 1, ["pk"], :default) ==
               [
                 {LogOffset.new(0, 0),
                  %{
                    value: %{"hello" => "world", "pk" => "10"},
                    key: "my_key",
                    headers: %{
                      relation: ["public", "test"],
                      operation: :insert,
                      txids: [1],
                      lsn: "0",
                      op_position: 0
                    }
                  }}
               ]

      # And with empty PK
      assert LogItems.from_change(record, 1, [], :default) ==
               [
                 {LogOffset.new(0, 0),
                  %{
                    value: %{"hello" => "world", "pk" => "10"},
                    key: "my_key",
                    headers: %{
                      relation: ["public", "test"],
                      operation: :insert,
                      txids: [1],
                      lsn: "0",
                      op_position: 0
                    }
                  }}
               ]
    end

    test "stores only PK of a `DeletedRecord` value" do
      record = %Changes.DeletedRecord{
        key: "my_key",
        old_record: %{"pk" => "10", "hello" => "world"},
        log_offset: LogOffset.first(),
        relation: {"public", "test"}
      }

      assert LogItems.from_change(record, 1, ["pk"], :default) ==
               [
                 {LogOffset.new(0, 0),
                  %{
                    value: %{"pk" => "10"},
                    key: "my_key",
                    headers: %{
                      relation: ["public", "test"],
                      operation: :delete,
                      txids: [1],
                      lsn: "0",
                      op_position: 0
                    }
                  }}
               ]
    end

    test "stores entire `DeletedRecord` value if table has no PK" do
      record = %Changes.DeletedRecord{
        key: "my_key",
        old_record: %{"value" => "10", "hello" => "world"},
        log_offset: LogOffset.first(),
        relation: {"public", "test"}
      }

      assert LogItems.from_change(record, 1, [], :default) ==
               [
                 {LogOffset.new(0, 0),
                  %{
                    value: %{"hello" => "world", "value" => "10"},
                    key: "my_key",
                    headers: %{
                      relation: ["public", "test"],
                      operation: :delete,
                      txids: [1],
                      lsn: "0",
                      op_position: 0
                    }
                  }}
               ]
    end

    test "stores any changed columns and PK of an `UpdatedRecord` value" do
      record =
        Changes.UpdatedRecord.new(%{
          key: "my_key",
          old_record: %{"pk" => "10", "hello" => "world", "test" => "me"},
          record: %{"pk" => "10", "hello" => "world", "test" => "new"},
          log_offset: LogOffset.first(),
          relation: {"public", "test"}
        })

      assert LogItems.from_change(record, 1, ["pk"], :default) ==
               [
                 {LogOffset.new(0, 0),
                  %{
                    value: %{"pk" => "10", "test" => "new"},
                    key: "my_key",
                    headers: %{
                      relation: ["public", "test"],
                      operation: :update,
                      txids: [1],
                      lsn: "0",
                      op_position: 0
                    }
                  }}
               ]
    end

    test "when replica=full sends entire row for updates" do
      record =
        Changes.UpdatedRecord.new(%{
          key: "my_key",
          old_record: %{"pk" => "10", "hello" => "world", "test" => "me"},
          record: %{"pk" => "10", "hello" => "world", "test" => "new"},
          log_offset: LogOffset.first(),
          relation: {"public", "test"}
        })

      assert LogItems.from_change(record, 1, ["pk"], :full) ==
               [
                 {LogOffset.first(),
                  %{
                    value: %{"pk" => "10", "hello" => "world", "test" => "new"},
                    old_value: %{"test" => "me"},
                    key: "my_key",
                    headers: %{
                      relation: ["public", "test"],
                      operation: :update,
                      txids: [1],
                      lsn: "0",
                      op_position: 0
                    }
                  }}
               ]
    end

    test "when replica=full sends entire row for deletes" do
      record =
        %Changes.DeletedRecord{
          key: "my_key",
          old_record: %{"pk" => "10", "hello" => "world", "test" => "me"},
          log_offset: LogOffset.first(),
          relation: {"public", "test"}
        }

      assert LogItems.from_change(record, 1, ["pk"], :full) ==
               [
                 {LogOffset.first(),
                  %{
                    value: %{"pk" => "10", "hello" => "world", "test" => "me"},
                    key: "my_key",
                    headers: %{
                      relation: ["public", "test"],
                      operation: :delete,
                      txids: [1],
                      lsn: "0",
                      op_position: 0
                    }
                  }}
               ]
    end

    test "splits up the `UpdatedRecord` if a key was changed, adding a reference to both" do
      record =
        Changes.UpdatedRecord.new(%{
          old_key: "old_key",
          key: "new_key",
          old_record: %{"pk" => "9", "hello" => "world", "test" => "me"},
          record: %{"pk" => "10", "hello" => "world", "test" => "new"},
          log_offset: LogOffset.first(),
          relation: {"public", "test"}
        })

      assert LogItems.from_change(record, 1, ["pk"], :default) ==
               [
                 {LogOffset.new(0, 0),
                  %{
                    value: %{"pk" => "9"},
                    key: "old_key",
                    headers: %{
                      relation: ["public", "test"],
                      operation: :delete,
                      txids: [1],
                      key_change_to: "new_key",
                      lsn: "0",
                      op_position: 0
                    }
                  }},
                 {LogOffset.new(0, 1),
                  %{
                    value: %{"hello" => "world", "pk" => "10", "test" => "new"},
                    key: "new_key",
                    headers: %{
                      relation: ["public", "test"],
                      operation: :insert,
                      txids: [1],
                      key_change_from: "old_key",
                      lsn: "0",
                      op_position: 1
                    }
                  }}
               ]
    end

    test "splits up the `UpdatedRecord` if a key was changed, adding a reference to both when no PK is defined" do
      record =
        Changes.UpdatedRecord.new(%{
          old_key: "old_key",
          key: "new_key",
          old_record: %{"hello" => "world", "test" => "me"},
          record: %{"hello" => "world", "test" => "new"},
          log_offset: LogOffset.first(),
          relation: {"public", "test"}
        })

      assert LogItems.from_change(record, 1, [], :default) ==
               [
                 {LogOffset.new(0, 0),
                  %{
                    value: %{"hello" => "world", "test" => "me"},
                    key: "old_key",
                    headers: %{
                      relation: ["public", "test"],
                      operation: :delete,
                      txids: [1],
                      key_change_to: "new_key",
                      lsn: "0",
                      op_position: 0
                    }
                  }},
                 {LogOffset.new(0, 1),
                  %{
                    value: %{"hello" => "world", "test" => "new"},
                    key: "new_key",
                    headers: %{
                      relation: ["public", "test"],
                      operation: :insert,
                      txids: [1],
                      key_change_from: "old_key",
                      lsn: "0",
                      op_position: 1
                    }
                  }}
               ]
    end
  end
end
