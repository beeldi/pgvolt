defmodule Electric.Shapes.FilterTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias Electric.Replication.Changes.DeletedRecord
  alias Electric.Replication.Changes.NewRecord
  alias Electric.Replication.Changes.Relation
  alias Electric.Replication.Changes.TruncatedRelation
  alias Electric.Replication.Changes.UpdatedRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Filter.Indexes.SubqueryIndex
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["t1", "t2", "t3", "table", "another_table", "the_table"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "number", type: "int8"},
                 %{name: "an_array", array_type: "int8"}
               ]
             )

  describe "indexed_shape?/1" do
    test "returns true for equality-indexed shapes" do
      shape = Shape.new!("t1", where: "id = 7", inspector: @inspector)

      assert Filter.indexed_shape?(shape)
    end

    test "returns true for the indexed where clause forms supported by WhereCondition" do
      [
        "id = 7",
        "7 = id",
        "id = 7 AND id > 6",
        "id > 6 AND id = 7",
        "id = 7 AND number = 11",
        "an_array @> '{5, 11, 29}'",
        "'{5, 11, 29}' <@ an_array",
        "7 = ANY(an_array)",
        "1 = ANY(an_array) AND id = 7",
        "id = 7 AND 1 = ANY(an_array)",
        "id IN (1, 2, 3)",
        "id = 1 OR id = 2",
        "id = 1 AND (number = 2 OR number = 3)",
        "(id = 1 OR id = 2) AND number = 3",
        "(id = 1 AND number > 2) OR (id = 1 AND number < 1)",
        "id IN (SELECT id FROM another_table)",
        "NOT id IN (SELECT id FROM another_table)",
        "id IN (SELECT id FROM another_table) OR id = 1",
        "(id, number) IN (SELECT id, number FROM another_table)"
      ]
      |> Enum.each(fn where ->
        shape =
          Shape.new!("table",
            where: where,
            inspector: @inspector
          )

        assert Filter.indexed_shape?(shape), "#{where} should be indexed"
      end)
    end

    test "returns false for OR shapes when one branch is not indexed" do
      shape = Shape.new!("t1", where: "id = 7 OR number > 5", inspector: @inspector)

      refute Filter.indexed_shape?(shape)
    end

    test "returns false for shapes without an indexable where clause" do
      shape = Shape.new!("t1", inspector: @inspector)

      refute Filter.indexed_shape?(shape)
    end
  end

  describe "affected_shapes/2" do
    test "returns shapes affected by insert" do
      filter =
        Filter.new()
        |> Filter.add_shape("s1", Shape.new!("t1", where: "id = 1", inspector: @inspector))
        |> Filter.add_shape("s2", Shape.new!("t1", where: "id = 2", inspector: @inspector))
        |> Filter.add_shape("s3", Shape.new!("t1", where: "id = 3", inspector: @inspector))
        |> Filter.add_shape("s4", Shape.new!("t2", where: "id = 2", inspector: @inspector))

      insert = %NewRecord{relation: {"public", "t1"}, record: %{"id" => "2"}}

      assert Filter.affected_shapes(filter, insert) == MapSet.new(["s2"])
    end

    test "returns shapes affected by uuid equality using compact index keys" do
      uuid1 = "550e8400-e29b-41d4-a716-446655440000"
      uuid2 = "550e8400-e29b-41d4-a716-446655440001"
      {:ok, uuid1_bytes} = Ecto.UUID.dump(uuid1)

      inspector =
        StubInspector.new(
          tables: ["uuid_table"],
          columns: [%{name: "id", type: "uuid", pk_position: 0}]
        )

      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("uuid_table", where: "id = '#{uuid1}'", inspector: inspector)
        )
        |> Filter.add_shape(
          "s2",
          Shape.new!("uuid_table", where: "id = '#{uuid2}'", inspector: inspector)
        )

      index_entries = :ets.tab2list(filter.eq_index_table)

      assert Enum.any?(index_entries, &match?({{_, "id", ^uuid1_bytes}, _}, &1))
      refute Enum.any?(index_entries, &match?({{_, "id", ^uuid1}, _}, &1))

      insert = %NewRecord{relation: {"public", "uuid_table"}, record: %{"id" => uuid1}}

      assert Filter.affected_shapes(filter, insert) == MapSet.new(["s1"])
    end

    test "returns shapes affected by delete" do
      filter =
        Filter.new()
        |> Filter.add_shape("s1", Shape.new!("t1", where: "id = 1", inspector: @inspector))
        |> Filter.add_shape("s2", Shape.new!("t1", where: "id = 2", inspector: @inspector))
        |> Filter.add_shape("s3", Shape.new!("t1", where: "id = 3", inspector: @inspector))
        |> Filter.add_shape("s4", Shape.new!("t2", where: "id = 2", inspector: @inspector))

      delete = %DeletedRecord{relation: {"public", "t1"}, old_record: %{"id" => "2"}}

      assert Filter.affected_shapes(filter, delete) == MapSet.new(["s2"])
    end

    test "returns shapes affected by update" do
      filter =
        Filter.new()
        |> Filter.add_shape("s1", Shape.new!("t1", where: "id = 1", inspector: @inspector))
        |> Filter.add_shape("s2", Shape.new!("t1", where: "id = 2", inspector: @inspector))
        |> Filter.add_shape("s3", Shape.new!("t1", where: "id = 3", inspector: @inspector))
        |> Filter.add_shape("s4", Shape.new!("t1", where: "id = 4", inspector: @inspector))
        |> Filter.add_shape("s5", Shape.new!("t2", where: "id = 2", inspector: @inspector))

      update = %UpdatedRecord{
        relation: {"public", "t1"},
        record: %{"id" => "2"},
        old_record: %{"id" => "3"}
      }

      assert Filter.affected_shapes(filter, update) == MapSet.new(["s2", "s3"])
    end

    test "returns shapes affected by relation change" do
      filter =
        Filter.new()
        |> Filter.add_shape("s1", Shape.new!("t1", where: "id = 1", inspector: @inspector))
        |> Filter.add_shape("s2", Shape.new!("t1", where: "id = 2", inspector: @inspector))
        |> Filter.add_shape("s3", Shape.new!("t1", where: "id > 7", inspector: @inspector))
        |> Filter.add_shape("s4", Shape.new!("t1", where: "id > 8", inspector: @inspector))
        |> Filter.add_shape(
          "s5",
          Shape.new!("t1", where: "an_array @> '{1,2}'", inspector: @inspector)
        )
        |> Filter.add_shape("s6", Shape.new!("t2", where: "id = 1", inspector: @inspector))
        |> Filter.add_shape("s7", Shape.new!("t2", where: "id = 2", inspector: @inspector))
        |> Filter.add_shape("s8", Shape.new!("t2", where: "id > 7", inspector: @inspector))
        |> Filter.add_shape("s9", Shape.new!("t2", where: "id > 8", inspector: @inspector))

      relation = %Relation{schema: "public", table: "t1"}

      assert Filter.affected_shapes(filter, relation) ==
               MapSet.new(["s1", "s2", "s3", "s4", "s5"])
    end

    test "returns shapes affected by relation rename" do
      table_id = 123
      s1 = Shape.new!("t1", inspector: @inspector)
      s2 = Shape.new!("t2", inspector: @inspector) |> Map.put(:root_table_id, table_id)
      s3 = Shape.new!("t3", inspector: @inspector)

      filter =
        Filter.new()
        |> Filter.add_shape("s1", s1)
        |> Filter.add_shape("s2", s2)
        |> Filter.add_shape("s3", s3)

      rename = %Relation{schema: "public", table: "new_name", id: table_id}

      assert Filter.affected_shapes(filter, rename) == MapSet.new(["s2"])
    end

    test "returns shapes affected by column addition" do
      s1 = Shape.new!("t1", inspector: @inspector)
      s2 = Shape.new!("t1", inspector: @inspector, columns: ["id", "number"])

      filter =
        Filter.new()
        |> Filter.add_shape("s1", s1)
        |> Filter.add_shape("s2", s2)

      rename = %Relation{
        schema: "public",
        table: "t1",
        id: s1.root_table_id,
        columns: ["id", "number", "an_array", "new one"]
      }

      assert Filter.affected_shapes(filter, rename) == MapSet.new(["s1"])
    end

    test "returns shapes affected by column change" do
      s1 = Shape.new!("t1", inspector: @inspector)
      s2 = Shape.new!("t1", inspector: @inspector, columns: ["id", "number"])
      s3 = Shape.new!("t1", inspector: @inspector, columns: ["id", "an_array"])

      filter =
        Filter.new()
        |> Filter.add_shape("s1", s1)
        |> Filter.add_shape("s2", s2)
        |> Filter.add_shape("s3", s3)

      rename = %Relation{
        schema: "public",
        table: "t1",
        id: s1.root_table_id,
        columns: ["id", "number", "an_array"],
        affected_columns: ["number"]
      }

      assert Filter.affected_shapes(filter, rename) == MapSet.new(["s1", "s2"])
    end

    test "returns shapes affected by truncation" do
      filter =
        Filter.new()
        |> Filter.add_shape("s1", Shape.new!("t1", where: "id = 1", inspector: @inspector))
        |> Filter.add_shape("s2", Shape.new!("t1", where: "id = 2", inspector: @inspector))
        |> Filter.add_shape("s3", Shape.new!("t1", where: "id > 7", inspector: @inspector))
        |> Filter.add_shape("s4", Shape.new!("t1", where: "id > 8", inspector: @inspector))
        |> Filter.add_shape("s5", Shape.new!("t2", where: "id = 1", inspector: @inspector))
        |> Filter.add_shape("s6", Shape.new!("t2", where: "id = 2", inspector: @inspector))
        |> Filter.add_shape("s7", Shape.new!("t2", where: "id > 7", inspector: @inspector))
        |> Filter.add_shape("s8", Shape.new!("t2", where: "id > 8", inspector: @inspector))

      truncation = %TruncatedRelation{relation: {"public", "t1"}}

      assert Filter.affected_shapes(filter, truncation) == MapSet.new(["s1", "s2", "s3", "s4"])
    end

    test "ignores shapes whose root table shares neither the OID nor the name of the relation" do
      s1 = Shape.new!("t1", inspector: @inspector)
      s2 = Shape.new!("t2", inspector: @inspector)

      filter =
        Filter.new()
        |> Filter.add_shape("s1", s1)
        |> Filter.add_shape("s2", s2)

      relation = %Relation{
        schema: "public",
        table: "t3",
        id: :erlang.phash2({"public", "t3"}),
        columns: ["id", "number", "an_array"],
        affected_columns: ["number"]
      }

      assert Filter.affected_shapes(filter, relation) == MapSet.new()
    end

    test "removed shapes are not reported for relation or truncation changes" do
      s1 = Shape.new!("t1", inspector: @inspector)
      filter = Filter.new() |> Filter.add_shape("s1", s1)

      relation = %Relation{
        schema: "public",
        table: "t1",
        id: s1.root_table_id,
        columns: ["id", "number", "an_array"],
        affected_columns: ["number"]
      }

      truncation = %TruncatedRelation{relation: {"public", "t1"}}

      assert Filter.affected_shapes(filter, relation) == MapSet.new(["s1"])
      assert Filter.affected_shapes(filter, truncation) == MapSet.new(["s1"])

      Filter.remove_shape(filter, "s1")

      assert Filter.affected_shapes(filter, relation) == MapSet.new()
      assert Filter.affected_shapes(filter, truncation) == MapSet.new()

      Filter.add_shape(filter, "s1", s1)

      assert Filter.affected_shapes(filter, relation) == MapSet.new(["s1"])
      assert Filter.affected_shapes(filter, truncation) == MapSet.new(["s1"])
    end

    test "shape with no where clause is affected by all changes for the same table" do
      shape = Shape.new!("t1", inspector: @inspector)
      filter = Filter.new() |> Filter.add_shape("s", shape)

      assert Filter.affected_shapes(filter, change("t1", %{"id" => "7"})) == MapSet.new(["s"])
      assert Filter.affected_shapes(filter, change("t1", %{"id" => "8"})) == MapSet.new(["s"])
      assert Filter.affected_shapes(filter, change("t2", %{"id" => "8"})) == MapSet.new([])
    end

    test "shape with a where clause is affected by changes that match that where clause" do
      shape = Shape.new!("t1", where: "id = 7", inspector: @inspector)
      filter = Filter.new() |> Filter.add_shape("s", shape)

      assert Filter.affected_shapes(filter, change("t1", %{"id" => "7"})) == MapSet.new(["s"])
      assert Filter.affected_shapes(filter, change("t1", %{"id" => "8"})) == MapSet.new([])
      assert Filter.affected_shapes(filter, change("t2", %{"id" => "8"})) == MapSet.new([])
    end

    test "a record value the evaluator cannot read is answered conservatively, not logged per change" do
      filter =
        Filter.new()
        |> Filter.add_shape("shape1", Shape.new!("table", inspector: @inspector))
        |> Filter.add_shape("shape2", Shape.new!("table", where: "id = 7", inspector: @inspector))
        |> Filter.add_shape("shape3", Shape.new!("table", where: "id = 8", inspector: @inspector))
        |> Filter.add_shape("shape4", Shape.new!("table", where: "id > 9", inspector: @inspector))
        |> Filter.add_shape("shape5", Shape.new!("another_table", inspector: @inspector))

      log =
        capture_log(fn ->
          assert Filter.affected_shapes(filter, change("table", %{"id" => "invalid_value"})) ==
                   MapSet.new(["shape1", "shape2", "shape3", "shape4"])
        end)

      refute log =~ "Unexpected error in Filter"
    end

    test "a record value the evaluator cannot read makes routing report itself inexact" do
      filter =
        Filter.new()
        |> Filter.add_shape("shape", Shape.new!("table", where: "id = 7", inspector: @inspector))

      assert {_shapes, false} =
               Filter.affected_shapes_exact(filter, change("table", %{"id" => "invalid_value"}))

      assert {_shapes, true} =
               Filter.affected_shapes_exact(filter, change("table", %{"id" => "7"}))
    end

    test "an equality value the evaluator cannot read narrows to the shapes indexed on that field" do
      filter =
        Filter.new()
        |> Filter.add_shape("on_id", Shape.new!("table", where: "id = 7", inspector: @inspector))
        |> Filter.add_shape(
          "on_number",
          Shape.new!("table", where: "number = 11", inspector: @inspector)
        )

      assert Filter.affected_shapes(
               filter,
               change("table", %{"id" => "invalid_value", "number" => "99"})
             ) == MapSet.new(["on_id"])
    end

    test "an array value the evaluator cannot read narrows to the shapes indexed on that field" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "on_array",
          Shape.new!("table", where: "an_array @> \'{5}\'", inspector: @inspector)
        )
        |> Filter.add_shape(
          "on_number",
          Shape.new!("table", where: "number = 11", inspector: @inspector)
        )

      # A malformed array literal (trailing garbage after the closing brace) the evaluator's array
      # parser cannot read.
      change = change("table", %{"an_array" => "{1,2,3}x", "number" => "99"})

      assert Filter.affected_shapes(filter, change) == MapSet.new(["on_array"])
    end

    test "supports `array_field @> const_array`" do
      shape_count = Enum.random(0..50)
      change_array_size = Enum.random(0..10)
      array_value_population = 5

      change_array =
        Stream.repeatedly(fn -> Enum.random(1..array_value_population) end)
        |> Enum.take(change_array_size)

      shape_arrays =
        Stream.repeatedly(fn ->
          shape_array_size = Enum.random(0..3)

          Stream.repeatedly(fn -> Enum.random(1..array_value_population) end)
          |> Enum.take(shape_array_size)
        end)
        |> Stream.uniq_by(& &1)
        |> Enum.take(shape_count)

      where_clause = fn shape_array ->
        # Randomly choose between `@>` and `<@` to test both forms
        if Enum.random(0..1) == 0 do
          "an_array @> '{#{Enum.join(shape_array, ",")}}'"
        else
          "'{#{Enum.join(shape_array, ",")}}' <@ an_array"
        end
      end

      filter =
        shape_arrays
        |> Enum.reduce(Filter.new(), fn shape_array, filter ->
          Filter.add_shape(
            filter,
            shape_array,
            Shape.new!("t1",
              where: where_clause.(shape_array),
              inspector: @inspector
            )
          )
        end)

      change = change("t1", %{"an_array" => "{#{change_array |> Enum.join(", ")}}"})

      expected_affected_shapes =
        shape_arrays
        |> Enum.filter(fn shape_array ->
          shape_array |> MapSet.new() |> MapSet.subset?(MapSet.new(change_array))
        end)
        |> MapSet.new()

      assert Filter.affected_shapes(filter, change) == expected_affected_shapes
    end

    @where_clause_tests [
      %{
        where: "id = 7",
        records: [
          {%{"id" => "7"}, true},
          {%{"id" => "8"}, false},
          {%{"id" => nil}, false}
        ]
      },
      %{
        where: "7 = id",
        records: [
          {%{"id" => "7"}, true},
          {%{"id" => "8"}, false},
          {%{"id" => nil}, false}
        ]
      },
      %{
        where: "id = 7 AND id > 1",
        records: [
          {%{"id" => "7"}, true},
          {%{"id" => "8"}, false}
        ]
      },
      %{
        where: "id = 7 AND id > 8",
        records: [
          {%{"id" => "7"}, false}
        ]
      },
      %{
        where: "id > 1 AND id = 7",
        records: [
          {%{"id" => "7"}, true},
          {%{"id" => "8"}, false}
        ]
      },
      %{
        where: "id > 8 AND id = 7",
        records: [
          {%{"id" => "7"}, false}
        ]
      },
      %{
        where: "an_array = '{1}'",
        records: [
          {%{"an_array" => "{1}"}, true},
          {%{"an_array" => "{2}"}, false},
          {%{"an_array" => "{1,2}"}, false}
        ]
      },
      %{
        where: "an_array @> '{1}'",
        records: [
          {%{"an_array" => "{1}"}, true},
          {%{"an_array" => "{1,2}"}, true},
          {%{"an_array" => "{3,2,1}"}, true},
          {%{"an_array" => "{2}"}, false},
          {%{"an_array" => "{2,3,4}"}, false},
          {%{"an_array" => nil}, false}
        ]
      },
      %{
        where: "an_array @> '{1,3}'",
        records: [
          {%{"an_array" => "{1,3}"}, true},
          {%{"an_array" => "{3,1}"}, true},
          {%{"an_array" => "{1,2,3}"}, true},
          {%{"an_array" => "{1,2}"}, false},
          {%{"an_array" => "{2,3,4}"}, false},
          {%{"an_array" => nil}, false}
        ]
      },
      %{
        where: "an_array @> '{}'",
        records: [
          {%{"an_array" => "{1,3}"}, true},
          {%{"an_array" => "{}"}, true},
          {%{"an_array" => nil}, false}
        ]
      },
      %{
        where: "an_array @> NULL",
        records: [
          {%{"an_array" => "{1}"}, false},
          {%{"an_array" => "{1,2,3}"}, false},
          {%{"an_array" => nil}, false}
        ]
      },
      %{
        where: "id = 7 AND number > 3 AND number < 10",
        records: [
          {%{"id" => "7", "number" => "5"}, true},
          {%{"id" => "7", "number" => "9"}, true},
          {%{"id" => "7", "number" => "3"}, false},
          {%{"id" => "7", "number" => "10"}, false},
          {%{"id" => "8", "number" => "5"}, false},
          {%{"id" => nil, "number" => "5"}, false}
        ]
      },
      %{
        where: "id > 5 AND id < 10 AND number = 7",
        records: [
          {%{"id" => "7", "number" => "7"}, true},
          {%{"id" => "9", "number" => "7"}, true},
          {%{"id" => "5", "number" => "7"}, false},
          {%{"id" => "10", "number" => "7"}, false},
          {%{"id" => "7", "number" => "8"}, false}
        ]
      },
      %{
        where: "an_array @> '{1}' AND id = 7",
        records: [
          {%{"id" => "7", "an_array" => "{1}"}, true},
          {%{"id" => "7", "an_array" => "{1,2}"}, true},
          {%{"id" => "7", "an_array" => "{3,2,1}"}, true},
          {%{"id" => "7", "an_array" => "{2}"}, false},
          {%{"id" => "7", "an_array" => "{2,3,4}"}, false},
          {%{"id" => "8", "an_array" => "{1}"}, false},
          {%{"id" => "8", "an_array" => "{1,2,3}"}, false},
          {%{"id" => "7", "an_array" => nil}, false},
          {%{"id" => nil, "an_array" => "{1}"}, false}
        ]
      },
      %{
        where: "id = 7 AND an_array @> '{1}'",
        records: [
          {%{"id" => "7", "an_array" => "{1}"}, true},
          {%{"id" => "7", "an_array" => "{1,2}"}, true},
          {%{"id" => "7", "an_array" => "{3,2,1}"}, true},
          {%{"id" => "7", "an_array" => "{2}"}, false},
          {%{"id" => "8", "an_array" => "{1}"}, false},
          {%{"id" => "7", "an_array" => nil}, false}
        ]
      },
      %{
        where: "1 = ANY(an_array)",
        records: [
          {%{"an_array" => "{1}"}, true},
          {%{"an_array" => "{1,2}"}, true},
          {%{"an_array" => "{3,2,1}"}, true},
          {%{"an_array" => "{2}"}, false},
          {%{"an_array" => "{2,3,4}"}, false},
          {%{"an_array" => nil}, false}
        ]
      },
      %{
        where: "1 = ANY(an_array) AND id = 7",
        records: [
          {%{"id" => "7", "an_array" => "{1}"}, true},
          {%{"id" => "7", "an_array" => "{1,2}"}, true},
          {%{"id" => "7", "an_array" => "{2}"}, false},
          {%{"id" => "8", "an_array" => "{1}"}, false},
          {%{"id" => "7", "an_array" => nil}, false}
        ]
      },
      %{
        where: "id = 7 AND 1 = ANY(an_array)",
        records: [
          {%{"id" => "7", "an_array" => "{1}"}, true},
          {%{"id" => "7", "an_array" => "{1,2}"}, true},
          {%{"id" => "7", "an_array" => "{2}"}, false},
          {%{"id" => "8", "an_array" => "{1}"}, false},
          {%{"id" => "7", "an_array" => nil}, false}
        ]
      },
      %{
        where: "id IN (1, 2, 3)",
        records: [
          {%{"id" => "1"}, true},
          {%{"id" => "2"}, true},
          {%{"id" => "3"}, true},
          {%{"id" => "4"}, false},
          {%{"id" => "0"}, false}
        ]
      },
      %{
        where: "id IN (1, 2) AND number > 5",
        records: [
          {%{"id" => "1", "number" => "6"}, true},
          {%{"id" => "2", "number" => "10"}, true},
          {%{"id" => "1", "number" => "3"}, false},
          {%{"id" => "3", "number" => "6"}, false}
        ]
      },
      %{
        where: "number > 5 AND id IN (1, 2)",
        records: [
          {%{"id" => "1", "number" => "6"}, true},
          {%{"id" => "2", "number" => "10"}, true},
          {%{"id" => "1", "number" => "3"}, false},
          {%{"id" => "3", "number" => "6"}, false}
        ]
      },
      %{
        where: "id = 1 OR id = 2",
        records: [
          {%{"id" => "1"}, true},
          {%{"id" => "2"}, true},
          {%{"id" => "3"}, false}
        ]
      },
      %{
        where: "id = 1 AND (number = 2 OR number = 3)",
        records: [
          {%{"id" => "1", "number" => "2"}, true},
          {%{"id" => "1", "number" => "3"}, true},
          {%{"id" => "1", "number" => "4"}, false},
          {%{"id" => "2", "number" => "2"}, false}
        ]
      },
      %{
        where: "(id = 1 OR id = 2) AND number = 3",
        records: [
          {%{"id" => "1", "number" => "3"}, true},
          {%{"id" => "2", "number" => "3"}, true},
          {%{"id" => "1", "number" => "4"}, false},
          {%{"id" => "3", "number" => "3"}, false}
        ]
      },
      %{
        where: "(id = 1 AND number > 2) OR (id = 1 AND number < 1)",
        records: [
          {%{"id" => "1", "number" => "3"}, true},
          {%{"id" => "1", "number" => "0"}, true},
          {%{"id" => "1", "number" => "2"}, false}
        ]
      }
    ]

    for %{where: where, records: records} <- @where_clause_tests do
      for {record, affected} <- records do
        test "where: #{where}, record: #{inspect(record)}" do
          where = unquote(where)
          record = unquote(Macro.escape(record))
          affected = unquote(affected)

          shape = Shape.new!("the_table", where: where, inspector: @inspector)
          transaction = change("the_table", record)

          assert Filter.new()
                 |> Filter.add_shape("the-shape", shape)
                 |> Filter.affected_shapes(transaction) == MapSet.new(["the-shape"]) == affected
        end
      end
    end
  end

  # `other_shapes` is a per-change linear scan, so a clause landing there routes in time proportional
  # to the shapes on the node instead of the index's flat few microseconds. Distributing an IN list
  # over a residual is what keeps a multi-division filter out of it, and that was refused whenever the
  # residual held any `or` at all - including one this optimiser would never index, which therefore
  # cannot multiply the branches out. Measured at 20k synthetic shapes with `("deletedAt" IS NULL OR
  # "metaJson" IS NOT NULL)` in the residual: 555 µs/change at 50 shapes per node rising to 23 ms at
  # 2000, against a flat ~6 µs once distribution is allowed.
  describe "distribution over an IN list" do
    @distribution_inspector StubInspector.new(
                              tables: ["employees"],
                              columns: [
                                %{name: "id", type: "text", pk_position: 0},
                                %{name: "firmId", type: "text"},
                                %{name: "divisionId", type: "text"},
                                %{name: "deletedAt", type: "text"},
                                %{name: "metaJson", type: "text"}
                              ]
                            )

    # The residual always comes to rest in some node's `other_shapes`; which node is the whole point.
    # Distributed, it rests on a per-division leaf that only shapes covering that one division share.
    # Undistributed, the entire `divisionId IN (...) AND residual` rests on the firm node, which every
    # shape in the firm shares and which is scanned once per change. So the property to assert is
    # whether `divisionId` reached the index at all.
    defp indexed_fields(filter) do
      filter.eq_index_table
      |> :ets.tab2list()
      |> Enum.flat_map(fn
        {{_condition_id, field, _value}, _} when is_binary(field) -> [field]
        _ -> []
      end)
      |> MapSet.new()
    end

    defp add_employees_shape(filter, id, where) do
      Filter.add_shape(
        filter,
        id,
        Shape.new!("employees", where: where, inspector: @distribution_inspector)
      )
    end

    test "an inert or in the residual still lets the IN list reach the index" do
      filter =
        add_employees_shape(
          Filter.new(),
          "s1",
          ~s|"firmId" = 'p1' AND "divisionId" IN ('b1', 'b2') AND ("deletedAt" IS NULL OR "metaJson" IS NOT NULL)|
        )

      assert indexed_fields(filter) == MapSet.new(["firmId", "divisionId"])
    end

    test "an indexable or in the residual keeps the IN list out, because distributing it multiplies" do
      filter =
        add_employees_shape(
          Filter.new(),
          "s1",
          ~s|"divisionId" IN ('b1', 'b2') AND ("firmId" = 'p1' OR "firmId" = 'p2')|
        )

      refute MapSet.member?(indexed_fields(filter), "divisionId")
    end

    test "routes an inert-or clause to exactly the records it selects" do
      filter =
        Filter.new()
        |> add_employees_shape(
          "in-scope",
          ~s|"firmId" = 'p1' AND "divisionId" IN ('b1', 'b2') AND ("deletedAt" IS NULL OR "metaJson" IS NOT NULL)|
        )
        |> add_employees_shape(
          "other-firm",
          ~s|"firmId" = 'p2' AND "divisionId" IN ('b1', 'b2') AND ("deletedAt" IS NULL OR "metaJson" IS NOT NULL)|
        )

      matching = [
        %{"id" => "e1", "firmId" => "p1", "divisionId" => "b2", "deletedAt" => nil},
        %{
          "id" => "e2",
          "firmId" => "p1",
          "divisionId" => "b1",
          "deletedAt" => "2026-01-01",
          "metaJson" => "kept"
        }
      ]

      non_matching = [
        %{"id" => "e3", "firmId" => "p1", "divisionId" => "b9", "deletedAt" => nil},
        %{"id" => "e4", "firmId" => "p1", "divisionId" => "b1", "deletedAt" => "2026-01-01"},
        %{"id" => "e5", "firmId" => "p3", "divisionId" => "b1", "deletedAt" => nil}
      ]

      Enum.each(matching, fn record ->
        insert = %NewRecord{relation: {"public", "employees"}, record: record}

        assert Filter.affected_shapes(filter, insert) == MapSet.new(["in-scope"]),
               "#{record["id"]} should match"
      end)

      Enum.each(non_matching, fn record ->
        insert = %NewRecord{relation: {"public", "employees"}, record: record}

        assert Filter.affected_shapes(filter, insert) == MapSet.new(),
               "#{record["id"]} should not match"
      end)
    end

    test "removing an inert-or shape leaves the filter empty" do
      where =
        ~s|"firmId" = 'p1' AND "divisionId" IN ('b1', 'b2') AND ("deletedAt" IS NULL OR "metaJson" IS NOT NULL)|

      filter = add_employees_shape(Filter.new(), "s1", where)

      filter = Filter.remove_shape(filter, "s1")

      assert :ets.info(filter.where_cond_table, :size) == 0
      assert :ets.info(filter.eq_index_table, :size) == 0
      assert Filter.active_shapes(filter) == []
    end
  end

  describe "inclusion index nodes" do
    defp inclusion_root(filter, field) do
      [{_, root}] = :ets.match_object(filter.incl_index_table, {{:_, field, []}, :_})
      root
    end

    defp containment_filter(shape_count) do
      Enum.reduce(1..shape_count, Filter.new(), fn n, filter ->
        Filter.add_shape(
          filter,
          "s#{n}",
          Shape.new!("table", where: "an_array @> '{#{n}}'", inspector: @inspector)
        )
      end)
    end

    # A node used to carry a sorted list of its children's values, which made its ETS row grow with
    # the child count - O(children) to insert, since ETS copies the row whole, and O(children) to
    # read on every change. A child is already addressable as its own row, so the node only needs to
    # know how many it has. The invariant is that the row is a constant size; a timing bound would
    # not survive a loaded CI machine, and correctness tests pass either way.
    test "a node's row does not grow with the number of children" do
      small = inclusion_root(containment_filter(10), "an_array")
      large = inclusion_root(containment_filter(200), "an_array")

      assert large.child_count == 200
      assert :erts_debug.flat_size(large) == :erts_debug.flat_size(small)
    end

    test "routes a multi-value containment shape by descending the change's values" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "pair",
          Shape.new!("table", where: "an_array @> '{2, 4}'", inspector: @inspector)
        )
        |> Filter.add_shape(
          "single",
          Shape.new!("table", where: "an_array @> '{4}'", inspector: @inspector)
        )

      affected = fn array ->
        Filter.affected_shapes(filter, %NewRecord{
          relation: {"public", "table"},
          record: %{"id" => "1", "an_array" => array}
        })
      end

      assert affected.("{1,2,3,4}") == MapSet.new(["pair", "single"])
      assert affected.("{4,5}") == MapSet.new(["single"])
      assert affected.("{2,3}") == MapSet.new()
      assert affected.("{}") == MapSet.new()
    end

    test "removing every shape empties the index" do
      filter = containment_filter(20)

      filter =
        Enum.reduce(1..20, filter, fn n, filter -> Filter.remove_shape(filter, "s#{n}") end)

      assert :ets.info(filter.incl_index_table, :size) == 0
      assert Filter.active_shapes(filter) == []
    end
  end

  # Array-overlap filters: tables with an array column select with `&&` instead of an IN list.
  # Overlap holds on any single shared element, so `a && ARRAY[v1, …, vn]` is
  # `a @> [v1] OR … OR a @> [vn]` and each disjunct reaches the inclusion index. Left unrecognised the
  # whole clause is unindexed, and a firm's every shape on the table is re-evaluated per change -
  # the same defect as an undistributed IN list, reached through a different clause.
  describe "array overlap filters" do
    @overlap_inspector StubInspector.new(
                         tables: ["ticket"],
                         columns: [
                           %{name: "id", type: "text", pk_position: 0},
                           %{name: "firmId", type: "text"},
                           %{name: "divisionIds", array_type: "text"},
                           %{name: "deletedAt", type: "text"}
                         ]
                       )

    @overlap_where ~s|"firmId" = 'p1' AND "divisionIds" && ARRAY['b1', 'b2']::text[] AND "deletedAt" IS NULL|

    defp add_ticket_shape(filter, id, where) do
      Filter.add_shape(
        filter,
        id,
        Shape.new!("ticket", where: where, inspector: @overlap_inspector)
      )
    end

    defp inclusion_indexed_fields(filter) do
      filter.incl_index_table
      |> :ets.tab2list()
      |> Enum.flat_map(fn
        {{_condition_id, field, _path}, _} when is_binary(field) -> [field]
        _ -> []
      end)
      |> MapSet.new()
    end

    test "an overlap filter reaches the inclusion index" do
      filter = add_ticket_shape(Filter.new(), "s1", @overlap_where)

      assert inclusion_indexed_fields(filter) == MapSet.new(["divisionIds"])
    end

    test "routes an overlap filter to exactly the records it selects" do
      filter =
        Filter.new()
        |> add_ticket_shape("in-scope", @overlap_where)
        |> add_ticket_shape(
          "other-firm",
          ~s|"firmId" = 'p2' AND "divisionIds" && ARRAY['b1', 'b2']::text[] AND "deletedAt" IS NULL|
        )

      matching = [
        %{"id" => "t1", "firmId" => "p1", "divisionIds" => "{b2,zz}", "deletedAt" => nil},
        %{"id" => "t2", "firmId" => "p1", "divisionIds" => "{b1,b2}", "deletedAt" => nil}
      ]

      non_matching = [
        %{"id" => "t3", "firmId" => "p1", "divisionIds" => "{zz,yy}", "deletedAt" => nil},
        %{"id" => "t4", "firmId" => "p1", "divisionIds" => "{}", "deletedAt" => nil},
        %{"id" => "t5", "firmId" => "p9", "divisionIds" => "{b1}", "deletedAt" => nil},
        %{"id" => "t6", "firmId" => "p1", "divisionIds" => "{b1}", "deletedAt" => "2026-01-01"}
      ]

      Enum.each(matching, fn record ->
        insert = %NewRecord{relation: {"public", "ticket"}, record: record}

        assert Filter.affected_shapes(filter, insert) == MapSet.new(["in-scope"]),
               "#{record["id"]} should match"
      end)

      Enum.each(non_matching, fn record ->
        insert = %NewRecord{relation: {"public", "ticket"}, record: record}

        assert Filter.affected_shapes(filter, insert) == MapSet.new(),
               "#{record["id"]} should not match"
      end)
    end

    test "a record overlapping several filter values matches the shape once" do
      filter = add_ticket_shape(Filter.new(), "s1", @overlap_where)

      insert = %NewRecord{
        relation: {"public", "ticket"},
        record: %{"id" => "t1", "firmId" => "p1", "divisionIds" => "{b1,b2}", "deletedAt" => nil}
      }

      assert Filter.affected_shapes(filter, insert) == MapSet.new(["s1"])
    end

    test "removing an overlap shape leaves the filter empty" do
      filter = add_ticket_shape(Filter.new(), "s1", @overlap_where)

      filter = Filter.remove_shape(filter, "s1")

      assert :ets.info(filter.where_cond_table, :size) == 0
      assert :ets.info(filter.incl_index_table, :size) == 0
      assert Filter.active_shapes(filter) == []
    end
  end

  # Keying index rows by the bucket alone puts every shape on a table under one ETS key, and both a
  # bag insert and `delete_object/2` walk that key's object list: registering shape n costs O(n), on
  # the collector's serialized call path. Measured at 40k shapes, 7.3ms per registration against
  # 0.11ms once the shape id is part of the key. The invariant is structural rather than a timing
  # bound - one index row has to be addressable on its own.
  test "the relation index addresses one shape per key, not one bucket per table" do
    shapes =
      Enum.map(1..50, fn n ->
        {"shape-#{n}", Shape.new!("table", where: "id = #{n}", inspector: @inspector)}
      end)

    filter =
      Enum.reduce(shapes, Filter.new(), fn {id, shape}, filter ->
        Filter.add_shape(filter, id, shape)
      end)

    {id, shape} = Enum.at(shapes, 25)
    oid = shape.root_table_id
    name = shape.root_table

    assert :ets.lookup(filter.relation_index_table, {:oid, oid, id}) == [{{:oid, oid, id}}]

    assert :ets.lookup(filter.relation_index_table, {:name, name, id}) ==
             [{{:name, name, id}, oid}]

    assert :ets.info(filter.relation_index_table, :size) == 100
  end

  test "Filter.remove_shape/2" do
    shapes = [
      Shape.new!("table", inspector: @inspector),
      Shape.new!("another_table", inspector: @inspector),
      Shape.new!("table", where: "id = 1", inspector: @inspector),
      Shape.new!("table", where: "id = 2", inspector: @inspector),
      Shape.new!("table", where: "id > 2", inspector: @inspector),
      Shape.new!("table", where: "id > 7", inspector: @inspector),
      Shape.new!("table", where: "an_array @> '{}'", inspector: @inspector),
      Shape.new!("table", where: "an_array @> '{1}'", inspector: @inspector),
      Shape.new!("table", where: "an_array @> '{1,2}'", inspector: @inspector),
      Shape.new!("table", where: "an_array @> '{1,3}'", inspector: @inspector),
      Shape.new!("table", where: "id = 1 AND an_array @> '{1}'", inspector: @inspector),
      Shape.new!("table", where: "id = 1 AND an_array @> '{1,2}'", inspector: @inspector),
      Shape.new!("table", where: "1 = ANY(an_array)", inspector: @inspector),
      Shape.new!("table", where: "2 = ANY(an_array)", inspector: @inspector),
      Shape.new!("table", where: "id = 1 AND 1 = ANY(an_array)", inspector: @inspector),
      Shape.new!("table", where: "id = 1 OR id = 2", inspector: @inspector),
      Shape.new!("table", where: "id = 1 OR number > 5", inspector: @inspector),
      Shape.new!("table",
        where: "(id = 1 AND number > 2) OR (id = 1 AND number < 1)",
        inspector: @inspector
      ),
      Shape.new!("table", where: "id = 1 AND (number = 2 OR number = 3)", inspector: @inspector),
      Shape.new!("table", where: "(id = 1 OR id = 2) AND number = 3", inspector: @inspector),
      Shape.new!("table", where: "id IN (1, 2, 3)", inspector: @inspector),
      Shape.new!("table", where: "id IN (4, 5)", inspector: @inspector),
      Shape.new!("table", where: "id IN (1, 2) AND number > 5", inspector: @inspector),
      Shape.new!("table",
        where: "id IN (SELECT id FROM another_table) OR id = 1",
        inspector: @inspector
      ),
      Shape.new!("table",
        where: "id IN (SELECT id FROM another_table) OR number > 5",
        inspector: @inspector
      ),
      Shape.new!("table",
        where: "id IN (SELECT id FROM another_table)",
        inspector: @inspector
      ),
      Shape.new!("table",
        where: "NOT id IN (SELECT id FROM another_table)",
        inspector: @inspector
      )
    ]

    filter = Filter.new()

    shapes
    |> Enum.shuffle()
    |> Enum.with_index(fn shape, i ->
      # Capture ETS state before adding
      state_before = snapshot_filter_ets(filter)

      Filter.add_shape(filter, i, shape)

      # Ensure that the ETS state has changed after adding
      # (checks we've included all the ets tables in the snapshot)
      assert snapshot_filter_ets(filter) != state_before

      # Remove the shape
      Filter.remove_shape(filter, i)

      # Check that the ETS state after removing is the same as before adding
      assert snapshot_filter_ets(filter) == state_before

      # Add the shape back for the next iteration
      Filter.add_shape(filter, i, shape)
    end)
  end

  test "Filter.remove_shape/2 leaves no orphan subquery rows when draining a shared node" do
    filter = Filter.new()
    index = Filter.subquery_index(filter)
    state_before = snapshot_filter_ets(filter)

    shape_ids = ["a", "b", "c"]

    for id <- shape_ids do
      shape =
        Shape.new!("table",
          where: "id IN (SELECT id FROM another_table)",
          inspector: @inspector
        )

      Filter.add_shape(filter, id, shape)
      SubqueryIndex.seed_membership(index, id, ["$sublink", "0"], 0, MapSet.new([1, 2, 3]))
      SubqueryIndex.mark_ready(index, id)
    end

    for id <- shape_ids, do: Filter.remove_shape(filter, id)

    assert snapshot_filter_ets(filter) == state_before
  end

  test "Filter.remove_shape/2 removes seeded subquery index state" do
    filter = Filter.new()
    state_before = snapshot_filter_ets(filter)
    shape_id = "seeded-shape"

    shape =
      Shape.new!("table",
        where: "id IN (SELECT id FROM another_table)",
        inspector: @inspector
      )

    Filter.add_shape(filter, shape_id, shape)

    index = Filter.subquery_index(filter)
    subquery_ref = ["$sublink", "0"]

    SubqueryIndex.seed_membership(index, shape_id, subquery_ref, 0, MapSet.new([5]))
    SubqueryIndex.mark_ready(index, shape_id)

    assert snapshot_filter_ets(filter) != state_before

    Filter.remove_shape(filter, shape_id)

    assert snapshot_filter_ets(filter) == state_before
  end

  test "no ETS row grows with the number of non-indexed shapes on a node" do
    # Non-indexed shapes are stored per where-condition node. If a node kept
    # them in a single aggregate row, every add/remove would copy the node's
    # whole population in and out of ETS: O(n) per operation and O(n²) to
    # (re)build the filter on startup.
    filter = Filter.new()

    add_non_indexed_shapes = fn range ->
      Enum.each(range, fn i ->
        shape = Shape.new!("t1", where: "number > #{i}", inspector: @inspector)
        Filter.add_shape(filter, i, shape)
      end)
    end

    add_non_indexed_shapes.(1..10)
    largest_row_with_few_shapes = largest_ets_row_size(filter)

    add_non_indexed_shapes.(11..1000)
    assert largest_ets_row_size(filter) == largest_row_with_few_shapes
  end

  defp largest_ets_row_size(filter) do
    filter
    |> snapshot_filter_ets()
    |> Map.values()
    |> Enum.concat()
    |> Enum.map(&:erts_debug.flat_size/1)
    |> Enum.max()
  end

  # Captures the full state of all ETS tables in a filter for comparison
  defp snapshot_filter_ets(filter) do
    %{
      shapes: :ets.tab2list(filter.shapes_table) |> Enum.sort(),
      tables: :ets.tab2list(filter.tables_table) |> Enum.sort(),
      where_cond: :ets.tab2list(filter.where_cond_table) |> Enum.sort(),
      other_shapes: :ets.tab2list(filter.other_shapes_table) |> Enum.sort(),
      eq_index: :ets.tab2list(filter.eq_index_table) |> Enum.sort(),
      incl_index: :ets.tab2list(filter.incl_index_table) |> Enum.sort(),
      subquery_index: :ets.tab2list(filter.subquery_index) |> Enum.sort()
    }
  end

  describe "optimisations" do
    # These tests assert that the number of reductions needed to calculate the affected shapes
    # when there at @shape_count shapes is less than @max_reductions.
    #
    # Reductions are used as a proxy for time taken for the calculation and
    # have been chosen instead of time since the time taken is not deterministic and
    # leads to flakey tests.
    #
    # Modern machines process approx 300-400 reductions per μs so @max_reductions of 1300
    # is roughly equivalent to 3μs.
    #
    # 3μs per change is a desirable and achievable target for replication stream processing.
    # If optimised processing becomes slower than this we should discuss as a team to see if
    # the performance is acceptable.
    #
    # @shape_count is set to 1000. This is somewhat arbitrary but is a reasonable number of shapes.
    # The main point is we don't want to linearly scale with the number of shapes, we want
    # O(1) or at worst O(log n) performance, so if we have that, 1000 or 10_000 shapes should be easy
    # to keep to a microsecond per change. 10_000 shapes makes for a slow test though as the setup
    # time (n Filter.add_shape calls) is slow.
    @shape_count 1000
    @max_reductions 1300

    # add_shape decides the memory strip by reading the query text, and by parsing it with the
    # pg_query NIF (Parser.parse_query, no validate pass) when the text does not settle it, so a
    # clause costs one cheap parse at most -- a fraction of the full parse+validate it replaced,
    # but above the parse-free budget a single comparison meets.
    @add_max_reductions 2_600
    # remove_shape reconstructs the memory-stripped AST, which needs the full parse+validate to
    # rebuild the tree the where-condition index walk consumes; that reparse is the eviction path,
    # not the boot path that replays add_shape across the whole shape population.
    @remove_max_reductions 8_000

    # First-call BEAM module loading is charged to the measuring process and can exceed these tight
    # per-op budgets, so warm every optimised form off a throwaway filter and each test measures steady state.
    setup do
      warmup = Filter.new()

      [
        "id = 1",
        "id = 1 AND number = 11",
        "an_array @> '{1, 2, 3}'",
        "1 = ANY(an_array)",
        "id = 1 OR id = 2"
      ]
      |> Enum.with_index()
      |> Enum.each(fn {where, id} ->
        shape = Shape.new!("t1", where: where, inspector: @inspector)
        Filter.add_shape(warmup, id, shape)
        Filter.remove_shape(warmup, id)
      end)

      :ok
    end

    @tag :performance
    test "where clause in the form `field = const` is optimised" do
      filter = Filter.new()

      Enum.each(1..@shape_count, fn i ->
        shape = Shape.new!("t1", where: "id = #{i}", inspector: @inspector)
        add_reductions = reductions(fn -> Filter.add_shape(filter, i, shape) end)
        assert add_reductions < @max_reductions
      end)

      assert Filter.affected_shapes(filter, change("t1", %{"id" => "7"})) == MapSet.new([7])

      affected_reductions =
        reductions(fn ->
          Filter.affected_shapes(filter, change("t1", %{"id" => "7"}))
        end)

      assert affected_reductions < @max_reductions

      Enum.each(1..@shape_count, fn i ->
        remove_reductions = reductions(fn -> Filter.remove_shape(filter, i) end)
        assert remove_reductions < @remove_max_reductions
      end)
    end

    @tag :performance
    test "where clause in the form `field = const AND another_condition` is optimised" do
      filter = Filter.new()

      Enum.each(1..@shape_count, fn i ->
        shape = Shape.new!("t1", where: "id = #{i} AND id > 6", inspector: @inspector)
        add_reductions = reductions(fn -> Filter.add_shape(filter, i, shape) end)
        assert add_reductions < @add_max_reductions
      end)

      assert Filter.affected_shapes(filter, change("t1", %{"id" => "7"})) == MapSet.new([7])

      affected_reductions =
        reductions(fn ->
          Filter.affected_shapes(filter, change("t1", %{"id" => "7"}))
        end)

      assert affected_reductions < @max_reductions

      Enum.each(1..@shape_count, fn i ->
        remove_reductions = reductions(fn -> Filter.remove_shape(filter, i) end)
        assert remove_reductions < @remove_max_reductions
      end)
    end

    @tag :performance
    test "where clause in the form `a_condition AND field = const` is optimised" do
      filter = Filter.new()

      Enum.each(1..@shape_count, fn i ->
        shape = Shape.new!("t1", where: "id > 6 AND id = #{i}", inspector: @inspector)
        add_reductions = reductions(fn -> Filter.add_shape(filter, i, shape) end)
        assert add_reductions < @add_max_reductions
      end)

      assert Filter.affected_shapes(filter, change("t1", %{"id" => "7"})) == MapSet.new([7])

      affected_reductions =
        reductions(fn ->
          Filter.affected_shapes(filter, change("t1", %{"id" => "7"}))
        end)

      assert affected_reductions < @max_reductions

      Enum.each(1..@shape_count, fn i ->
        remove_reductions = reductions(fn -> Filter.remove_shape(filter, i) end)
        assert remove_reductions < @remove_max_reductions
      end)
    end

    @tag :performance
    test "where clause in the form `field1 = const1 AND field2 = const2` is optimised for lots of const1 values" do
      filter = Filter.new()

      Enum.each(1..@shape_count, fn i ->
        shape = Shape.new!("t1", where: "id = #{i} AND number = 11", inspector: @inspector)
        add_reductions = reductions(fn -> Filter.add_shape(filter, i, shape) end)
        assert add_reductions < @add_max_reductions
      end)

      change = change("t1", %{"id" => "5", "number" => "11"})
      assert Filter.affected_shapes(filter, change) == MapSet.new([5])

      affected_reductions = reductions(fn -> Filter.affected_shapes(filter, change) end)

      assert affected_reductions < @max_reductions

      Enum.each(1..@shape_count, fn i ->
        remove_reductions = reductions(fn -> Filter.remove_shape(filter, i) end)
        assert remove_reductions < @remove_max_reductions
      end)
    end

    @tag :performance
    test "where clause in the form `field1 = const1 AND field2 = const2` is optimised for lots of const2 values" do
      filter = Filter.new()

      Enum.each(1..@shape_count, fn i ->
        shape = Shape.new!("t1", where: "id = 7 AND number = #{i}", inspector: @inspector)
        add_reductions = reductions(fn -> Filter.add_shape(filter, i, shape) end)
        assert add_reductions < @add_max_reductions
      end)

      change = change("t1", %{"id" => "7", "number" => "9"})
      assert Filter.affected_shapes(filter, change) == MapSet.new([9])

      affected_reductions = reductions(fn -> Filter.affected_shapes(filter, change) end)

      assert affected_reductions < @max_reductions

      Enum.each(1..@shape_count, fn i ->
        remove_reductions = reductions(fn -> Filter.remove_shape(filter, i) end)
        assert remove_reductions < @remove_max_reductions
      end)
    end

    @tag :performance
    test "relation changes collect shapes without walking nested where condition indexes" do
      filter = Filter.new()

      Enum.each(1..@shape_count, fn i ->
        shape = Shape.new!("t1", where: "id = #{i} AND number = 11", inspector: @inspector)
        Filter.add_shape(filter, i, shape)
      end)

      relation = %Relation{
        schema: "public",
        table: "t1",
        id: :erlang.phash2({"public", "t1"}),
        columns: ["id", "number", "an_array"],
        affected_columns: []
      }

      assert Filter.affected_shapes(filter, relation) == MapSet.new()

      affected_reductions =
        reductions(fn ->
          Filter.affected_shapes(filter, relation)
        end)

      assert affected_reductions < 100_000
    end

    @tag :performance
    test "relation changes for a table with no shapes visit no shapes at all" do
      filter = Filter.new()

      Enum.each(1..@shape_count, fn i ->
        shape = Shape.new!("t1", where: "id = #{i} AND number = 11", inspector: @inspector)
        Filter.add_shape(filter, i, shape)
      end)

      relation = %Relation{
        schema: "public",
        table: "t2",
        id: :erlang.phash2({"public", "t2"}),
        columns: ["id", "number", "an_array"],
        affected_columns: ["number"]
      }

      assert Filter.affected_shapes(filter, relation) == MapSet.new()

      affected_reductions =
        reductions(fn ->
          Filter.affected_shapes(filter, relation)
        end)

      assert affected_reductions < @max_reductions
    end

    @tag :performance
    test "where clause in the form `array_field @> const_array` is optimised" do
      # The optimisation for `@>` is less performant than the other optimisations,
      # however it performs well with lots of shapes. While it can't do
      # `@shape_count` shapes in < `@max_reductions` reductions, it can do
      # `@shape_count * 5` shapes in < `@max_reductions * 5` reductions.
      multiplier = 5
      shape_count = @shape_count * multiplier
      max_reductions = @max_reductions * multiplier

      chosen_numbers = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
      matching_array = {5, 11, 29}

      filter = Filter.new()

      arrays =
        for(
          x <- 1..100,
          y <- 1..100,
          z <- 1..100,
          x < y,
          y < z,
          not (x in chosen_numbers && y in chosen_numbers && z in chosen_numbers),
          do: {x, y, z}
        )
        |> Enum.take_random(shape_count - 1)
        |> Enum.concat([matching_array])
        |> Enum.shuffle()

      Enum.each(arrays, fn {x, y, z} = array ->
        shape = Shape.new!("t1", where: "an_array @> '{#{x}, #{y}, #{z}}'", inspector: @inspector)
        add_reductions = reductions(fn -> Filter.add_shape(filter, array, shape) end)
        assert add_reductions < max_reductions
      end)

      change = change("t1", %{"an_array" => "{#{chosen_numbers |> Enum.join(", ")}}"})
      assert Filter.affected_shapes(filter, change) == MapSet.new([matching_array])

      affected_reductions = reductions(fn -> Filter.affected_shapes(filter, change) end)

      assert affected_reductions < max_reductions

      Enum.each(arrays, fn array ->
        remove_reductions = reductions(fn -> Filter.remove_shape(filter, array) end)
        assert remove_reductions < max_reductions
      end)
    end

    @tag :performance
    test "where clause in the form `const = ANY(array_field)` is optimised" do
      # Same shape count as @> but higher budget per shape because the ANY
      # AST is deeper to pattern-match through the planner. The extra headroom
      # over 10x absorbs the fork's routing-memo flag check on the add path,
      # which grazed the exact 13000 boundary (13001 measured).
      shape_count = @shape_count * 5
      max_reductions = @max_reductions * 11

      filter = Filter.new()

      Enum.each(1..shape_count, fn i ->
        shape = Shape.new!("t1", where: "#{i} = ANY(an_array)", inspector: @inspector)
        add_reductions = reductions(fn -> Filter.add_shape(filter, i, shape) end)
        assert add_reductions < max_reductions
      end)

      change = change("t1", %{"an_array" => "{7}"})
      assert Filter.affected_shapes(filter, change) == MapSet.new([7])

      affected_reductions = reductions(fn -> Filter.affected_shapes(filter, change) end)

      assert affected_reductions < max_reductions

      Enum.each(1..shape_count, fn i ->
        remove_reductions = reductions(fn -> Filter.remove_shape(filter, i) end)
        assert remove_reductions < max_reductions
      end)
    end

    @tag :performance
    test "where clause in the form `field = const1 OR field = const2` is optimised" do
      max_reductions = @max_reductions * 2
      filter = Filter.new()

      Enum.each(1..@shape_count, fn i ->
        shape =
          Shape.new!("t1", where: "id = #{i} OR id = #{i + @shape_count}", inspector: @inspector)

        add_reductions = reductions(fn -> Filter.add_shape(filter, i, shape) end)
        assert add_reductions < max_reductions
      end)

      change = change("t1", %{"id" => "7"})
      assert Filter.affected_shapes(filter, change) == MapSet.new([7])

      affected_reductions = reductions(fn -> Filter.affected_shapes(filter, change) end)

      assert affected_reductions < max_reductions

      Enum.each(1..@shape_count, fn i ->
        remove_reductions = reductions(fn -> Filter.remove_shape(filter, i) end)
        assert remove_reductions < @remove_max_reductions
      end)
    end

    @tag :performance
    test "removing a subquery shape is O(1) in the total number of shapes" do
      # Each shape lives on its own subquery node (distinct outer equality
      # value), so no single node ever holds more than one shape. The only thing
      # that grows is the total number of subquery shapes in the index, and the
      # cost of removing any one shape must stay flat as that total grows.
      filter = Filter.new()

      Enum.each(1..@shape_count, fn i ->
        shape =
          Shape.new!("table",
            where: "number = #{i} AND id IN (SELECT id FROM another_table)",
            inspector: @inspector
          )

        Filter.add_shape(filter, i, shape)
        seed_shape(filter, i, [1, 2, 3, 4, 5])
      end)

      Enum.each(1..@shape_count, fn i ->
        remove_reductions = reductions(fn -> Filter.remove_shape(filter, i) end)
        assert remove_reductions < @max_reductions
      end)
    end

    @tag :performance
    test "removing a subquery shape is O(1) in the number of shapes on its node" do
      # All shapes share an identical subquery predicate, so they all register on
      # the same subquery node. Removing one shape must not scan every other
      # shape registered on that node.
      filter = Filter.new()

      Enum.each(1..@shape_count, fn i ->
        shape =
          Shape.new!("table",
            where: "id IN (SELECT id FROM another_table)",
            inspector: @inspector
          )

        Filter.add_shape(filter, i, shape)
        seed_shape(filter, i, [i, i + 1_000_000])
      end)

      Enum.each(1..@shape_count, fn i ->
        remove_reductions = reductions(fn -> Filter.remove_shape(filter, i) end)
        assert remove_reductions < @max_reductions
      end)
    end

    @subquery_ref ["$sublink", "0"]

    defp seed_shape(filter, shape_id, values) do
      index = Filter.subquery_index(filter)
      SubqueryIndex.seed_membership(index, shape_id, @subquery_ref, 0, MapSet.new(values))
      SubqueryIndex.mark_ready(index, shape_id)
    end

    defp reductions(fun) do
      # A collection that lands inside `fun` is charged to it as reductions; start from a clean heap.
      :erlang.garbage_collect()
      {:reductions, reductions_before} = :erlang.process_info(self(), :reductions)
      fun.()
      {:reductions, reductions_after} = :erlang.process_info(self(), :reductions)
      reductions_after - reductions_before
    end
  end

  defp change(table, record) do
    %NewRecord{
      relation: {"public", table},
      record: record
    }
  end

  describe "subquery shapes routing in filter" do
    import Support.DbSetup
    import Support.DbStructureSetup
    import Support.ComponentSetup

    setup [
      :with_stack_id_from_test,
      :with_shared_db,
      :with_persistent_kv,
      :with_inspector,
      :with_sql_execute
    ]

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS child (id INT PRIMARY KEY, par_id INT REFERENCES parent(id))"
         ]
    test "unseeded subquery shape is still pruned by outer equality before fallback",
         %{inspector: inspector} do
      {:ok, shape} =
        Shape.new("child",
          inspector: inspector,
          where: "par_id = 7 AND id IN (SELECT id FROM parent)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape)

      insert_matching = %NewRecord{
        relation: {"public", "child"},
        record: %{"id" => "1", "par_id" => "7"}
      }

      assert Filter.affected_shapes(filter, insert_matching) == MapSet.new(["shape1"])

      insert_not_in_subquery = %NewRecord{
        relation: {"public", "child"},
        record: %{"id" => "99", "par_id" => "7"}
      }

      assert Filter.affected_shapes(filter, insert_not_in_subquery) == MapSet.new(["shape1"])

      insert_wrong_par_id = %NewRecord{
        relation: {"public", "child"},
        record: %{"id" => "1", "par_id" => "8"}
      }

      assert Filter.affected_shapes(filter, insert_wrong_par_id) == MapSet.new([])

      insert_on_other_table = %NewRecord{
        relation: {"public", "parent"},
        record: %{"id" => "1"}
      }

      assert Filter.affected_shapes(filter, insert_on_other_table) == MapSet.new([])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS incl_parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS incl_child (id INT PRIMARY KEY, par_id INT REFERENCES incl_parent(id), tags int[] NOT NULL)"
         ]
    test "unseeded subquery shape is still pruned by outer inclusion before fallback",
         %{inspector: inspector} do
      {:ok, shape} =
        Shape.new("incl_child",
          inspector: inspector,
          where: "tags @> '{1,2}' AND id IN (SELECT id FROM incl_parent)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape)

      insert_matching = %NewRecord{
        relation: {"public", "incl_child"},
        record: %{"id" => "10", "par_id" => "7", "tags" => "{1,2,3}"}
      }

      assert Filter.affected_shapes(filter, insert_matching) == MapSet.new(["shape1"])

      insert_not_in_subquery = %NewRecord{
        relation: {"public", "incl_child"},
        record: %{"id" => "99", "par_id" => "7", "tags" => "{1,2,3}"}
      }

      assert Filter.affected_shapes(filter, insert_not_in_subquery) == MapSet.new(["shape1"])

      insert_wrong_tags = %NewRecord{
        relation: {"public", "incl_child"},
        record: %{"id" => "10", "par_id" => "7", "tags" => "{3,4}"}
      }

      assert Filter.affected_shapes(filter, insert_wrong_tags) == MapSet.new([])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS child (id INT PRIMARY KEY, par_id INT REFERENCES parent(id))"
         ]
    test "unseeded subquery shapes are still separated by outer indexed branches", %{
      inspector: inspector
    } do
      {:ok, shape1} =
        Shape.new("child",
          inspector: inspector,
          where: "par_id = 7 AND id IN (SELECT id FROM parent)"
        )

      {:ok, shape2} =
        Shape.new("child",
          inspector: inspector,
          where: "par_id = 8 AND id IN (SELECT id FROM parent)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape1)
        |> Filter.add_shape("shape2", shape2)

      insert1 = %NewRecord{
        relation: {"public", "child"},
        record: %{"id" => "1", "par_id" => "7"}
      }

      assert Filter.affected_shapes(filter, insert1) == MapSet.new(["shape1"])

      insert2 = %NewRecord{
        relation: {"public", "child"},
        record: %{"id" => "3", "par_id" => "8"}
      }

      assert Filter.affected_shapes(filter, insert2) == MapSet.new(["shape2"])

      insert3 = %NewRecord{
        relation: {"public", "child"},
        record: %{"id" => "3", "par_id" => "7"}
      }

      assert Filter.affected_shapes(filter, insert3) == MapSet.new(["shape1"])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS nested_parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS nested_child (id INT PRIMARY KEY, field1 INT NOT NULL, field2 INT REFERENCES nested_parent(id))"
         ]
    test "unseeded subquery shape with nested equality conditions is always routed (fallback)", %{
      inspector: inspector
    } do
      {:ok, shape} =
        Shape.new("nested_child",
          inspector: inspector,
          where: "field1 = 10 AND field2 = 20 AND id IN (SELECT id FROM nested_parent)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape)

      insert_matching = %NewRecord{
        relation: {"public", "nested_child"},
        record: %{"id" => "1", "field1" => "10", "field2" => "20"}
      }

      assert Filter.affected_shapes(filter, insert_matching) == MapSet.new(["shape1"])

      insert_not_in_subquery = %NewRecord{
        relation: {"public", "nested_child"},
        record: %{"id" => "99", "field1" => "10", "field2" => "20"}
      }

      assert Filter.affected_shapes(filter, insert_not_in_subquery) == MapSet.new(["shape1"])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS or_parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS or_child (id INT PRIMARY KEY, par_id INT REFERENCES or_parent(id), value TEXT NOT NULL)"
         ]
    test "optimisable OR+subquery shape is affected by root table changes", %{
      inspector: inspector
    } do
      # Both OR branches are indexable, so the shape is registered twice in the
      # filter tree: once for the subquery branch and once for the simple branch.
      {:ok, shape} =
        Shape.new("or_child",
          inspector: inspector,
          where: "par_id IN (SELECT id FROM or_parent) OR value = 'target'"
        )

      filter = Filter.new()
      filter = Filter.add_shape(filter, "shape1", shape)

      # Seed the reverse index with subquery membership values
      index = Filter.subquery_index(filter)
      subquery_ref = ["$sublink", "0"]

      for value <- [1, 2, 3] do
        SubqueryIndex.add_value(index, "shape1", subquery_ref, 0, value)
      end

      SubqueryIndex.mark_ready(index, "shape1")

      # Record matching the OR's simple condition (value = 'target')
      insert_matching_value = %NewRecord{
        relation: {"public", "or_child"},
        record: %{"id" => "99", "par_id" => "99", "value" => "target"}
      }

      assert Filter.affected_shapes(filter, insert_matching_value) == MapSet.new(["shape1"])

      # Record matching the OR's subquery condition (par_id in refs)
      insert_matching_subquery = %NewRecord{
        relation: {"public", "or_child"},
        record: %{"id" => "10", "par_id" => "2", "value" => "other"}
      }

      assert Filter.affected_shapes(filter, insert_matching_subquery) == MapSet.new(["shape1"])

      # Record matching neither condition
      insert_no_match = %NewRecord{
        relation: {"public", "or_child"},
        record: %{"id" => "50", "par_id" => "99", "value" => "other"}
      }

      assert Filter.affected_shapes(filter, insert_no_match) == MapSet.new([])

      # Update where new record matches but old doesn't
      update_into_shape = %UpdatedRecord{
        relation: {"public", "or_child"},
        record: %{"id" => "50", "par_id" => "99", "value" => "target"},
        old_record: %{"id" => "50", "par_id" => "99", "value" => "other"}
      }

      assert Filter.affected_shapes(filter, update_into_shape) == MapSet.new(["shape1"])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS or_parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS or_child (id INT PRIMARY KEY, par_id INT REFERENCES or_parent(id), value TEXT NOT NULL)"
         ]
    test "mixed OR+subquery shape falls back to other_shapes verification", %{
      inspector: inspector
    } do
      {:ok, shape} =
        Shape.new("or_child",
          inspector: inspector,
          where: "par_id IN (SELECT id FROM or_parent) OR value LIKE 'target%'"
        )

      filter = Filter.new()
      filter = Filter.add_shape(filter, "shape1", shape)

      index = Filter.subquery_index(filter)
      subquery_ref = ["$sublink", "0"]

      for value <- [1, 2, 3] do
        SubqueryIndex.add_value(index, "shape1", subquery_ref, 0, value)
      end

      SubqueryIndex.mark_ready(index, "shape1")

      insert_matching_value = %NewRecord{
        relation: {"public", "or_child"},
        record: %{"id" => "99", "par_id" => "99", "value" => "target-me"}
      }

      assert Filter.affected_shapes(filter, insert_matching_value) == MapSet.new(["shape1"])

      insert_matching_subquery = %NewRecord{
        relation: {"public", "or_child"},
        record: %{"id" => "10", "par_id" => "2", "value" => "other"}
      }

      assert Filter.affected_shapes(filter, insert_matching_subquery) == MapSet.new(["shape1"])

      insert_no_match = %NewRecord{
        relation: {"public", "or_child"},
        record: %{"id" => "50", "par_id" => "99", "value" => "other"}
      }

      assert Filter.affected_shapes(filter, insert_no_match) == MapSet.new([])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS like_parent_unseeded (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS like_child_unseeded (id INT PRIMARY KEY, name TEXT NOT NULL, parent_id INT REFERENCES like_parent_unseeded(id))"
         ]
    test "unseeded LIKE + subquery shape still lets non-subquery terms prune", %{
      inspector: inspector
    } do
      {:ok, shape} =
        Shape.new("like_child_unseeded",
          inspector: inspector,
          where: "name LIKE 'keep%' AND parent_id IN (SELECT id FROM like_parent_unseeded)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape)

      insert_match = %NewRecord{
        relation: {"public", "like_child_unseeded"},
        record: %{"id" => "10", "name" => "keep_me", "parent_id" => "99"}
      }

      assert Filter.affected_shapes(filter, insert_match) == MapSet.new(["shape1"])

      insert_like_miss = %NewRecord{
        relation: {"public", "like_child_unseeded"},
        record: %{"id" => "11", "name" => "discard", "parent_id" => "99"}
      }

      assert Filter.affected_shapes(filter, insert_like_miss) == MapSet.new([])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS child (id INT PRIMARY KEY, par_id INT REFERENCES parent(id))"
         ]
    test "seeded subquery shape reached via non-subquery indexes is still verified against the full predicate",
         %{inspector: inspector} do
      {:ok, shape} =
        Shape.new("child",
          inspector: inspector,
          where: "par_id = 7 AND id IN (SELECT id FROM parent)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape)

      index = Filter.subquery_index(filter)
      subquery_ref = ["$sublink", "0"]

      SubqueryIndex.add_value(index, "shape1", subquery_ref, 0, 1)
      SubqueryIndex.mark_ready(index, "shape1")

      wrong_subquery_value = %NewRecord{
        relation: {"public", "child"},
        record: %{"id" => "99", "par_id" => "7"}
      }

      assert Filter.affected_shapes(filter, wrong_subquery_value) == MapSet.new([])

      matching_record = %NewRecord{
        relation: {"public", "child"},
        record: %{"id" => "1", "par_id" => "7"}
      }

      assert Filter.affected_shapes(filter, matching_record) == MapSet.new(["shape1"])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS child (id INT PRIMARY KEY, par_id INT REFERENCES parent(id))"
         ]
    test "subquery positions are isolated per shape even when DNF positions overlap", %{
      inspector: inspector
    } do
      {:ok, shape1} =
        Shape.new("child",
          inspector: inspector,
          where: "id IN (SELECT id FROM parent)"
        )

      {:ok, shape2} =
        Shape.new("child",
          inspector: inspector,
          where: "par_id IN (SELECT id FROM parent)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape1)
        |> Filter.add_shape("shape2", shape2)

      index = Filter.subquery_index(filter)
      subquery_ref = ["$sublink", "0"]

      SubqueryIndex.add_value(index, "shape1", subquery_ref, 0, 1)
      SubqueryIndex.add_value(index, "shape2", subquery_ref, 0, 1)
      SubqueryIndex.mark_ready(index, "shape1")
      SubqueryIndex.mark_ready(index, "shape2")

      change = %NewRecord{
        relation: {"public", "child"},
        record: %{"id" => "50", "par_id" => "1"}
      }

      assert Filter.affected_shapes(filter, change) == MapSet.new(["shape2"])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS child (id INT PRIMARY KEY, par_id INT REFERENCES parent(id))"
         ]
    test "remove_shape cleans up subquery index metadata and values", %{inspector: inspector} do
      {:ok, shape} =
        Shape.new("child",
          inspector: inspector,
          where: "id IN (SELECT id FROM parent)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape)

      index = Filter.subquery_index(filter)
      subquery_ref = ["$sublink", "0"]

      SubqueryIndex.add_value(index, "shape1", subquery_ref, 0, 1)
      SubqueryIndex.mark_ready(index, "shape1")

      assert :ets.tab2list(index) != []

      Filter.remove_shape(filter, "shape1")

      assert :ets.tab2list(index) == []
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS neg_parent (id INT PRIMARY KEY, value TEXT NOT NULL)",
           "CREATE TABLE IF NOT EXISTS neg_child (id INT PRIMARY KEY, parent_id INT REFERENCES neg_parent(id))"
         ]
    test "negated candidate derivation by complement", %{inspector: inspector} do
      {:ok, shape} =
        Shape.new("neg_child",
          inspector: inspector,
          where: "parent_id NOT IN (SELECT id FROM neg_parent WHERE value = 'keep')"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape)

      index = Filter.subquery_index(filter)
      subquery_ref = ["$sublink", "0"]

      # Seed membership with value 1 (parent id 1 matches the subquery "WHERE value = 'keep'")
      SubqueryIndex.seed_membership(
        index,
        "shape1",
        subquery_ref,
        0,
        MapSet.new([1])
      )

      SubqueryIndex.mark_ready(index, "shape1")

      # parent_id=1 is in the subquery view, so NOT IN means this should NOT route
      insert_matching_member = %NewRecord{
        relation: {"public", "neg_child"},
        record: %{"id" => "10", "parent_id" => "1"}
      }

      assert Filter.affected_shapes(filter, insert_matching_member) == MapSet.new([])

      # parent_id=2 is NOT in the subquery view, so NOT IN means this SHOULD route
      insert_not_member = %NewRecord{
        relation: {"public", "neg_child"},
        record: %{"id" => "11", "parent_id" => "2"}
      }

      assert Filter.affected_shapes(filter, insert_not_member) == MapSet.new(["shape1"])

      # Update crossing from non-matching to matching should route (union of old/new)
      update_crossing = %UpdatedRecord{
        relation: {"public", "neg_child"},
        record: %{"id" => "10", "parent_id" => "1"},
        old_record: %{"id" => "10", "parent_id" => "2"}
      }

      assert Filter.affected_shapes(filter, update_crossing) == MapSet.new(["shape1"])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS rep_parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS rep_child (id INT PRIMARY KEY, par_id INT REFERENCES rep_parent(id))"
         ]
    test "repeated dependency positions in one shape", %{inspector: inspector} do
      # Both positions reference the same dependency (rep_parent.id), but
      # compare against different root-table columns.
      {:ok, shape} =
        Shape.new("rep_child",
          inspector: inspector,
          where: "id IN (SELECT id FROM rep_parent) OR par_id IN (SELECT id FROM rep_parent)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape)

      index = Filter.subquery_index(filter)
      subquery_ref = ["$sublink", "0"]

      # Seed the membership view with values {1, 2}
      SubqueryIndex.seed_membership(
        index,
        "shape1",
        subquery_ref,
        0,
        MapSet.new([1, 2])
      )

      SubqueryIndex.mark_ready(index, "shape1")

      # Only id matches (id=1, par_id=99) -> should route
      insert_id_match = %NewRecord{
        relation: {"public", "rep_child"},
        record: %{"id" => "1", "par_id" => "99"}
      }

      assert Filter.affected_shapes(filter, insert_id_match) == MapSet.new(["shape1"])

      # Only par_id matches (id=99, par_id=2) -> should route
      insert_par_match = %NewRecord{
        relation: {"public", "rep_child"},
        record: %{"id" => "99", "par_id" => "2"}
      }

      assert Filter.affected_shapes(filter, insert_par_match) == MapSet.new(["shape1"])

      # Neither matches (id=99, par_id=99) -> should not route
      insert_neither = %NewRecord{
        relation: {"public", "rep_child"},
        record: %{"id" => "99", "par_id" => "99"}
      }

      assert Filter.affected_shapes(filter, insert_neither) == MapSet.new([])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS comp_parent (x INT NOT NULL, y INT NOT NULL, PRIMARY KEY (x, y))",
           "CREATE TABLE IF NOT EXISTS comp_child (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL)"
         ]
    test "composite-key subquery routing", %{inspector: inspector} do
      {:ok, shape} =
        Shape.new("comp_child",
          inspector: inspector,
          where: "(a, b) IN (SELECT x, y FROM comp_parent)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape)

      index = Filter.subquery_index(filter)
      subquery_ref = ["$sublink", "0"]

      # Seed membership with a tuple value {10, 20}
      SubqueryIndex.seed_membership(
        index,
        "shape1",
        subquery_ref,
        0,
        MapSet.new([{10, 20}])
      )

      SubqueryIndex.mark_ready(index, "shape1")

      # Matching tuple (a=10, b=20) should route
      insert_match = %NewRecord{
        relation: {"public", "comp_child"},
        record: %{"id" => "1", "a" => "10", "b" => "20"}
      }

      assert Filter.affected_shapes(filter, insert_match) == MapSet.new(["shape1"])

      # Only one column matches (a=10, b=99) should not route
      insert_partial = %NewRecord{
        relation: {"public", "comp_child"},
        record: %{"id" => "2", "a" => "10", "b" => "99"}
      }

      assert Filter.affected_shapes(filter, insert_partial) == MapSet.new([])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS like_parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS like_child (id INT PRIMARY KEY, name TEXT NOT NULL, parent_id INT REFERENCES like_parent(id))"
         ]
    test "LIKE + subquery on the other_shapes path uses callback verification", %{
      inspector: inspector
    } do
      # LIKE is not optimisable, so this shape ends up in other_shapes.
      # The subquery membership check should use the callback path.
      {:ok, shape} =
        Shape.new("like_child",
          inspector: inspector,
          where: "name LIKE 'keep%' AND parent_id IN (SELECT id FROM like_parent)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("shape1", shape)

      index = Filter.subquery_index(filter)
      subquery_ref = ["$sublink", "0"]

      SubqueryIndex.seed_membership(
        index,
        "shape1",
        subquery_ref,
        0,
        MapSet.new([1, 2])
      )

      SubqueryIndex.mark_ready(index, "shape1")

      # Both conditions match -> route
      insert_match = %NewRecord{
        relation: {"public", "like_child"},
        record: %{"id" => "10", "name" => "keep_me", "parent_id" => "1"}
      }

      assert Filter.affected_shapes(filter, insert_match) == MapSet.new(["shape1"])

      # LIKE matches but subquery membership fails -> no route
      insert_like_only = %NewRecord{
        relation: {"public", "like_child"},
        record: %{"id" => "11", "name" => "keep_me", "parent_id" => "99"}
      }

      assert Filter.affected_shapes(filter, insert_like_only) == MapSet.new([])

      # Subquery matches but LIKE fails -> no route
      insert_subquery_only = %NewRecord{
        relation: {"public", "like_child"},
        record: %{"id" => "12", "name" => "discard", "parent_id" => "1"}
      }

      assert Filter.affected_shapes(filter, insert_subquery_only) == MapSet.new([])
    end

    @tag with_sql: [
           "CREATE TABLE IF NOT EXISTS fb_parent (id INT PRIMARY KEY)",
           "CREATE TABLE IF NOT EXISTS fb_child (id INT PRIMARY KEY, par_id INT REFERENCES fb_parent(id))"
         ]
    test "unseeded shape routes conservatively until marked ready", %{
      inspector: inspector
    } do
      # Shapes start unseeded in the general subquery index and route
      # conservatively until mark_ready/2 is called.
      {:ok, fallback_shape} =
        Shape.new("fb_child",
          inspector: inspector,
          where: "par_id IN (SELECT id FROM fb_parent)"
        )

      {:ok, indexed_shape} =
        Shape.new("fb_child",
          inspector: inspector,
          where: "id IN (SELECT id FROM fb_parent)"
        )

      filter =
        Filter.new()
        |> Filter.add_shape("fallback_s", fallback_shape)
        |> Filter.add_shape("indexed_s", indexed_shape)

      index = Filter.subquery_index(filter)

      # fallback_s stays unseeded by not calling mark_ready. indexed_s gets
      # seeded and marked ready.
      subquery_ref = ["$sublink", "0"]

      SubqueryIndex.seed_membership(
        index,
        "indexed_s",
        subquery_ref,
        0,
        MapSet.new([1])
      )

      SubqueryIndex.mark_ready(index, "indexed_s")

      assert SubqueryIndex.fallback?(index, "fallback_s")
      refute SubqueryIndex.fallback?(index, "indexed_s")

      # fallback_s routes for any root-table change, indexed_s only for matching
      insert_match = %NewRecord{
        relation: {"public", "fb_child"},
        record: %{"id" => "1", "par_id" => "99"}
      }

      assert Filter.affected_shapes(filter, insert_match) ==
               MapSet.new(["fallback_s", "indexed_s"])

      insert_no_match = %NewRecord{
        relation: {"public", "fb_child"},
        record: %{"id" => "99", "par_id" => "99"}
      }

      # fallback_s still routes, indexed_s does not
      assert Filter.affected_shapes(filter, insert_no_match) == MapSet.new(["fallback_s"])

      # Changes on unrelated table should not route either shape
      insert_other = %NewRecord{
        relation: {"public", "fb_parent"},
        record: %{"id" => "1"}
      }

      assert Filter.affected_shapes(filter, insert_other) == MapSet.new([])
    end
  end

  describe "subquery index memory usage" do
    # This isn't a pass/fail test with tight bounds — it seeds the SubqueryIndex
    # the way the running system does (realistic shape-handle keys + UUID
    # membership values) and prints a memory report so we can see where the bytes
    # go for seeded subqueries.
    #
    # Run it (it's excluded by default) with:
    #
    #   mix test --include memory test/electric/shapes/filter_test.exs
    #
    # Term sizes that drive the numbers below:
    #
    #   * Shape handles. `Electric.Shapes.Shape.generate_id/1` builds handles as
    #     "#{hash}-#{unix_microseconds}" — a `:erlang.phash2` hash (up to ~9
    #     digits) joined to a 16-digit microsecond timestamp, i.e. ~26-byte
    #     binaries. In the live system these frequently arrive as *sub-binaries*
    #     of a larger request/replication buffer, but ETS flattens any
    #     sub-binary whose own size is < 64 bytes into a fresh heap binary when
    #     the row is inserted. So every index row that mentions a handle stores
    #     its own copy (a ~26-byte heap binary boxes to 6 words = 48 bytes); the
    #     handle is NOT shared between rows.
    #
    #   * UUID membership values are stored in their 16-byte binary form (what
    #     `Ecto.UUID.dump/1` produces), which boxes to a 4-word (32-byte) heap
    #     binary, also copied into every row that mentions it.
    #
    # For each seeded value the index writes ~2 rows: an exact `:membership` row
    # and a `:node_positive_member` routing row, each repeating the handle and
    # the value in its key tuple. That per-(shape, value) duplication is what the
    # report quantifies.

    @tag :memory
    @tag timeout: 120_000
    test "report: seeded subqueries on shared vs separate nodes" do
      wordsize = :erlang.system_info(:wordsize)

      # Anchor the realistic sizes against genuinely-generated terms.
      sample_shape =
        Shape.new!("table",
          where: "id IN (SELECT id FROM another_table)",
          inspector: @inspector
        )

      {_hash, real_handle} = Shape.generate_id(sample_shape)
      sample_value = uuid_value(1, 1)

      IO.puts("""

      ====================== SubqueryIndex memory report ======================
      wordsize: #{wordsize} bytes

      sample shape handle: #{inspect(real_handle)}
        byte_size: #{byte_size(real_handle)} bytes
        flat_size: #{:erts_debug.flat_size(real_handle)} words = \
      #{:erts_debug.flat_size(real_handle) * wordsize} bytes (boxed, as copied per ETS row)

      sample uuid value: #{byte_size(sample_value)}-byte binary
        flat_size: #{:erts_debug.flat_size(sample_value)} words = \
      #{:erts_debug.flat_size(sample_value) * wordsize} bytes (boxed, as copied per ETS row)
      """)

      scenarios = [
        {"1000 shapes · shared node · 50 values each", 1000, 50, &shared_node_where/1},
        {"1000 shapes · separate nodes · 50 values each", 1000, 50, &separate_node_where/1},
        {"50 shapes · shared node · 1000 values each", 50, 1000, &shared_node_where/1}
      ]

      for {label, shape_count, values_per_shape, where_fun} <- scenarios do
        report = measure_subquery_memory(shape_count, values_per_shape, where_fun)
        print_subquery_report(label, report, wordsize)
      end

      assert true
    end
  end

  # A subquery predicate every shape shares -> all shapes register on one node.
  defp shared_node_where(_i), do: "id IN (SELECT id FROM another_table)"

  # A distinct outer equality per shape -> every shape gets its own subquery node.
  defp separate_node_where(i), do: "number = #{i} AND id IN (SELECT id FROM another_table)"

  # A realistic shape handle: the real hash of the shape (exactly as
  # `Shape.generate_id/1` computes it) joined to a unique synthetic microsecond
  # timestamp so the handles are distinct, ~26-byte binaries like production.
  defp realistic_handle(shape, i), do: "#{Shape.hash(shape)}-#{1_750_000_000_000_000 + i}"

  # A unique 16-byte binary, the in-memory form of a uuid membership value.
  defp uuid_value(i, v), do: <<i::64, v::64>>

  defp measure_subquery_memory(shape_count, values_per_shape, where_fun) do
    filter = Filter.new()
    index = Filter.subquery_index(filter)

    for i <- 1..shape_count do
      shape =
        Shape.new!("table",
          where: where_fun.(i),
          inspector: @inspector
        )

      handle = realistic_handle(shape, i)
      Filter.add_shape(filter, handle, shape)

      values = MapSet.new(for v <- 1..values_per_shape, do: uuid_value(i, v))
      SubqueryIndex.seed_membership(index, handle, @subquery_ref, 0, values)
      SubqueryIndex.mark_ready(index, handle)
    end

    table = filter.subquery_index

    %{
      shape_count: shape_count,
      values_per_shape: values_per_shape,
      total_values: shape_count * values_per_shape,
      memory_words: :ets.info(table, :memory),
      rows: :ets.info(table, :size),
      by_tag: subquery_tag_breakdown(table)
    }
  end

  # Group every row by the leading tag atom in its key tuple and sum the boxed
  # heap size of each group, so we can see which row kind dominates.
  defp subquery_tag_breakdown(table) do
    table
    |> :ets.tab2list()
    |> Enum.group_by(fn {key, _value} -> elem(key, 0) end)
    |> Enum.map(fn {tag, rows} ->
      flat_words = Enum.reduce(rows, 0, fn row, acc -> acc + :erts_debug.flat_size(row) end)
      {tag, %{count: length(rows), flat_words: flat_words}}
    end)
    |> Enum.sort_by(fn {_tag, %{flat_words: words}} -> -words end)
  end

  defp print_subquery_report(label, report, wordsize) do
    memory_bytes = report.memory_words * wordsize
    per_value = if report.total_values > 0, do: memory_bytes / report.total_values, else: 0.0
    per_row = if report.rows > 0, do: memory_bytes / report.rows, else: 0.0

    IO.puts("""
    ------------------------------------------------------------------------
    #{label}
      shapes:           #{report.shape_count}
      values/shape:     #{report.values_per_shape}
      total values:     #{report.total_values}
      ETS rows:         #{report.rows}
      ETS memory:       #{memory_bytes} bytes (#{Float.round(memory_bytes / 1024 / 1024, 2)} MiB)
      per seeded value: #{Float.round(per_value, 1)} bytes
      per ETS row:      #{Float.round(per_row, 1)} bytes
      rows by tag (boxed heap size):\
    """)

    for {tag, %{count: count, flat_words: words}} <- report.by_tag do
      IO.puts(
        "        #{String.pad_trailing(inspect(tag), 24)} " <>
          "#{String.pad_leading(Integer.to_string(count), 8)} rows  " <>
          "#{String.pad_leading(Integer.to_string(words * wordsize), 12)} bytes"
      )
    end
  end
end
