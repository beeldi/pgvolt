defmodule Electric.Shapes.PartitionsTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Replication.Changes.Relation
  alias Electric.Replication.Changes.TruncatedRelation
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Shapes.Partitions
  alias Electric.Shapes.Shape

  alias Support.StubInspector

  @inspector StubInspector.new(%{
               {"public", "partitioned"} => %{
                 relation: %{
                   children: [
                     {"public", "partition_01"},
                     {"public", "partition_02"}
                   ]
                 },
                 columns: [
                   %{name: "id", type: "int8", pk_position: 0},
                   %{name: "an_array", array_type: "int8"}
                 ]
               },
               {"public", "partition_01"} => %{
                 relation: %{
                   children: nil,
                   parent: {"public", "partitioned"}
                 },
                 columns: [
                   %{name: "id", type: "int8", pk_position: 0},
                   %{name: "an_array", array_type: "int8"}
                 ]
               },
               {"public", "partition_02"} => %{
                 relation: %{
                   children: nil,
                   parent: {"public", "partitioned"}
                 },
                 columns: [
                   %{name: "id", type: "int8", pk_position: 0},
                   %{name: "an_array", array_type: "int8"}
                 ]
               },
               {"public", "partition_03"} => %{
                 relation: %{
                   children: nil,
                   parent: {"public", "partitioned"}
                 },
                 columns: [
                   %{name: "id", type: "int8", pk_position: 0},
                   %{name: "an_array", array_type: "int8"}
                 ]
               }
             })

  test "changes to table partition are sent to root" do
    partitions = Partitions.new(inspector: @inspector)

    {:ok, partitions} =
      Partitions.add_shape(partitions, "s1", Shape.new!("partitioned", inspector: @inspector))

    {:ok, partitions} =
      Partitions.add_shape(
        partitions,
        "s2",
        Shape.new!("partitioned", where: "id = 2", inspector: @inspector)
      )

    {:ok, partitions} =
      Partitions.add_shape(
        partitions,
        "s3",
        Shape.new!("partitioned", where: "id = 3", inspector: @inspector)
      )

    new = %NewRecord{
      relation: {"public", "partition_02"},
      record: %{"id" => "2"}
    }

    root = %{new | relation: {"public", "partitioned"}}
    insert = %TransactionFragment{changes: [new]}

    assert {_, %TransactionFragment{changes: [^new, ^root]}} =
             Partitions.handle_txn_fragment(partitions, insert)
  end

  test "no change expansion is done when no partitioned shapes are active" do
    partitions =
      Partitions.new(inspector: @inspector)

    {:ok, partitions} =
      Partitions.add_shape(partitions, "s1", Shape.new!("partition_01", inspector: @inspector))

    {:ok, partitions} =
      Partitions.add_shape(
        partitions,
        "s2",
        Shape.new!("partition_01", where: "id = 2", inspector: @inspector)
      )

    {:ok, partitions} =
      Partitions.add_shape(
        partitions,
        "s3",
        Shape.new!("partition_01", where: "id = 3", inspector: @inspector)
      )

    insert = %TransactionFragment{
      changes: [
        %NewRecord{
          relation: {"public", "partition_01"},
          record: %{"id" => "2"}
        }
      ]
    }

    {_, ^insert} = Partitions.handle_txn_fragment(partitions, insert)
  end

  test "after addition of new partition, shape receives updates" do
    partitions =
      Partitions.new(inspector: @inspector)

    {:ok, partitions} =
      Partitions.add_shape(partitions, "s1", Shape.new!("partitioned", inspector: @inspector))

    {:ok, partitions} =
      Partitions.add_shape(
        partitions,
        "s2",
        Shape.new!("partitioned", where: "id = 2", inspector: @inspector)
      )

    {:ok, partitions} =
      Partitions.add_shape(
        partitions,
        "s3",
        Shape.new!("partition_01", inspector: @inspector)
      )

    new = %NewRecord{relation: {"public", "partition_03"}, record: %{"id" => "2"}}
    root = %NewRecord{relation: {"public", "partitioned"}, record: %{"id" => "2"}}
    insert = %TransactionFragment{changes: [new]}

    {_, ^insert} = Partitions.handle_txn_fragment(partitions, insert)

    relation = %Relation{
      schema: "public",
      table: "partition_03",
      id: :erlang.phash2({"public", "partition_03"})
    }

    {:ok, partitions} = Partitions.handle_relation(partitions, relation)

    {_, %TransactionFragment{changes: [^new, ^root]}} =
      Partitions.handle_txn_fragment(partitions, insert)
  end

  test "remove_shape/2 cleans up partition information" do
    empty = Partitions.new(inspector: @inspector)

    partition_03 = %Relation{
      schema: "public",
      table: "partition_03",
      id: :erlang.phash2({"public", "partition_03"})
    }

    {:ok, partitions} =
      Partitions.add_shape(
        empty,
        "s1",
        Shape.new!("partitioned", inspector: @inspector)
      )

    {:ok, partitions} =
      Partitions.add_shape(
        partitions,
        "s2",
        Shape.new!("partitioned", where: "id = 1", inspector: @inspector)
      )

    {:ok, partitions} =
      Partitions.add_shape(
        partitions,
        "s3",
        Shape.new!("partition_01", where: "id = 2", inspector: @inspector)
      )

    {:ok, partitions} =
      Partitions.add_shape(
        partitions,
        "s4",
        Shape.new!("partition_02", where: "id > 2", inspector: @inspector)
      )

    {:ok, partitions} = Partitions.handle_relation(partitions, partition_03)

    {:ok, partitions} =
      Partitions.add_shape(
        partitions,
        "s5",
        Shape.new!("partition_03", where: "id > 7", inspector: @inspector)
      )

    new = %NewRecord{relation: {"public", "partition_03"}, record: %{"id" => "2"}}
    root = %NewRecord{relation: {"public", "partitioned"}, record: %{"id" => "2"}}
    insert = %TransactionFragment{changes: [new]}

    assert {_, %TransactionFragment{changes: [^new, ^root]}} =
             Partitions.handle_txn_fragment(partitions, insert)

    clean_partitions =
      partitions
      |> Partitions.remove_shape("s2")
      |> Partitions.remove_shape("s1")
      |> Partitions.remove_shape("s4")
      |> Partitions.remove_shape("s5")
      |> Partitions.remove_shape("s3")

    assert clean_partitions == empty

    assert {_, %TransactionFragment{changes: [^new]}} =
             Partitions.handle_txn_fragment(clean_partitions, insert)
  end

  test "add_shape/3 returns error on no connection" do
    partitions = Partitions.new(inspector: StubInspector.no_conn())

    {:error, :connection_not_available} =
      Partitions.add_shape(partitions, "s1", Shape.new!("partitioned", inspector: @inspector))
  end

  describe "truncation" do
    setup do
      partitions =
        Partitions.new(inspector: @inspector)

      {:ok, partitions} =
        Partitions.add_shape(partitions, "s1", Shape.new!("partitioned", inspector: @inspector))

      {:ok, partitions} =
        Partitions.add_shape(
          partitions,
          "s2",
          Shape.new!("partitioned", where: "id = 2", inspector: @inspector)
        )

      {:ok, partitions} =
        Partitions.add_shape(
          partitions,
          "s3",
          Shape.new!("partition_01", where: "id = 2", inspector: @inspector)
        )

      {:ok, partitions} =
        Partitions.add_shape(
          partitions,
          "s4",
          Shape.new!("partition_02", where: "id > 2", inspector: @inspector)
        )

      [partitions: partitions]
    end

    test "truncation of root partition truncates root and all partitions", ctx do
      truncate_partitioned = %TruncatedRelation{relation: {"public", "partitioned"}}
      truncate_partition_01 = %TruncatedRelation{relation: {"public", "partition_01"}}
      truncate_partition_02 = %TruncatedRelation{relation: {"public", "partition_02"}}

      batch = %TransactionFragment{changes: [truncate_partitioned]}

      assert {_,
              %TransactionFragment{
                changes: [^truncate_partitioned, ^truncate_partition_01, ^truncate_partition_02]
              }} = Partitions.handle_txn_fragment(ctx.partitions, batch)
    end

    test "truncation of partition truncates root and that partition", ctx do
      truncate_partitioned = %TruncatedRelation{relation: {"public", "partitioned"}}
      truncate_partition_02 = %TruncatedRelation{relation: {"public", "partition_02"}}

      batch = %TransactionFragment{changes: [truncate_partition_02]}

      assert {_, %TransactionFragment{changes: [^truncate_partition_02, ^truncate_partitioned]}} =
               Partitions.handle_txn_fragment(ctx.partitions, batch)
    end
  end

  describe "a partition that stops being one" do
    setup do
      {:ok, partitions} =
        Partitions.new(inspector: @inspector)
        |> Partitions.add_shape("s1", Shape.new!("partitioned", inspector: @inspector))

      insert = %TransactionFragment{
        changes: [%NewRecord{relation: {"public", "partition_02"}, record: %{"id" => "2"}}]
      }

      relation = %Relation{
        schema: "public",
        table: "partition_02",
        id: :erlang.phash2({"public", "partition_02"})
      }

      %{partitions: partitions, insert: insert, relation: relation}
    end

    # what the catalog reports for the name once the partition is detached, or dropped and
    # recreated as a plain table
    @plain_partition_02 StubInspector.new(%{
                          {"public", "partition_02"} => %{
                            relation: %{children: nil, parent: nil},
                            columns: [%{name: "id", type: "int8", pk_position: 0}]
                          }
                        })

    test "no longer sends its changes to the old root once it has no parent", ctx do
      %{partitions: partitions, insert: insert} = ctx

      assert {_, %TransactionFragment{changes: [_, _]}} =
               Partitions.handle_txn_fragment(partitions, insert)

      {:ok, partitions} =
        Partitions.handle_relation(%{partitions | inspector: @plain_partition_02}, ctx.relation)

      assert {_, ^insert} = Partitions.handle_txn_fragment(partitions, insert)
    end

    test "no longer sends its changes to the old root once it is gone", ctx do
      %{partitions: partitions, insert: insert} = ctx

      gone = {Electric.Shapes.PartitionsTest.ConstInspector, :table_not_found}

      {:ok, partitions} =
        Partitions.handle_relation(%{partitions | inspector: gone}, ctx.relation)

      assert {_, ^insert} = Partitions.handle_txn_fragment(partitions, insert)
    end

    test "sends its changes to the root again once attached back, and cleans up after", ctx do
      %{partitions: partitions, insert: insert} = ctx

      {:ok, partitions} =
        Partitions.handle_relation(%{partitions | inspector: @plain_partition_02}, ctx.relation)

      {:ok, partitions} =
        Partitions.handle_relation(%{partitions | inspector: @inspector}, ctx.relation)

      root = %NewRecord{relation: {"public", "partitioned"}, record: %{"id" => "2"}}

      assert {_, %TransactionFragment{changes: [_, ^root]}} =
               Partitions.handle_txn_fragment(partitions, insert)

      partitions = Partitions.remove_shape(partitions, "s1")

      assert {0, %{}, %{}} ==
               {partitions.active, partitions.partitions, partitions.partition_ownership}
    end
  end

  describe "introspection failures" do
    defmodule ConstInspector do
      def load_relation_info(_oid, response), do: response
    end

    test "handle_relation/2 ignores relations whose table no longer exists" do
      partitions = Partitions.new(inspector: {ConstInspector, :table_not_found})
      relation = %Relation{id: 1, schema: "public", table: "dropped", columns: []}

      assert {:ok, %Partitions{}} = Partitions.handle_relation(partitions, relation)
    end

    test "handle_relation/2 returns unexpected introspection errors" do
      error = "ERROR 53200 (out_of_memory) out of memory"
      partitions = Partitions.new(inspector: {ConstInspector, {:error, error}})
      relation = %Relation{id: 1, schema: "public", table: "a_table", columns: []}

      assert {:error, ^error} = Partitions.handle_relation(partitions, relation)
    end

    test "add_shape/3 returns unexpected introspection errors instead of raising" do
      shape = Shape.new!("partitioned", inspector: @inspector)
      partitions = Partitions.new(inspector: {ConstInspector, {:error, "boom"}})

      assert {:error, "boom"} = Partitions.add_shape(partitions, "s1", shape)
    end

    test "preload_root_table/2 leaves what the introspection found to add_shape/3" do
      shape = Shape.new!("partitioned", inspector: @inspector)

      for response <- [:table_not_found, {:error, "boom"}, {:error, :connection_not_available}] do
        partitions = Partitions.new(inspector: {ConstInspector, response})

        assert :ok = Partitions.preload_root_table(partitions, shape.root_table_id)
      end

      partitions = Partitions.new(inspector: {ConstInspector, {:error, "boom"}})
      :ok = Partitions.preload_root_table(partitions, shape.root_table_id)

      assert {:error, "boom"} = Partitions.add_shape(partitions, "s1", shape)
    end
  end
end
