defmodule Electric.Shapes.Filter.Indexes.EqualityIndexNumericConstTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes.NewRecord
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @inspector StubInspector.new(
               tables: ["products"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0},
                 %{name: "price", type: "float8"},
                 %{name: "ratio", type: "float4"},
                 %{name: "score", type: "numeric"}
               ]
             )

  # `price = 100` keeps the literal as the Elixir integer Postgres left after the implicit
  # int->float cast, while the record side parses every value for the column to a float. Keying the
  # :set index on the raw integer then never matches the parsed float, so the shape gets its snapshot
  # and never another row. The index must route every WAL spelling of the value to the shape.
  describe "integer literal against a float/numeric column" do
    test "routes a float8 record to a shape filtered on an integer literal, for every WAL spelling" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("products", where: "price = 100", inspector: @inspector)
        )

      for spelling <- ["100", "100.0", "1e2"] do
        change = %NewRecord{
          relation: {"public", "products"},
          record: %{"id" => "1", "price" => spelling}
        }

        affected = Filter.affected_shapes(filter, change)

        assert affected == MapSet.new(["s1"]),
               "a float8 record #{inspect(spelling)} was not routed to `price = 100`"
      end
    end

    test "routes a numeric record to a shape filtered on an integer literal" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("products", where: "score = 100", inspector: @inspector)
        )

      for spelling <- ["100", "100.00"] do
        change = %NewRecord{
          relation: {"public", "products"},
          record: %{"id" => "1", "score" => spelling}
        }

        affected = Filter.affected_shapes(filter, change)

        assert affected == MapSet.new(["s1"]),
               "a numeric record #{inspect(spelling)} was not routed to `score = 100`"
      end
    end

    test "routes a float4 record to a shape filtered on an integer literal" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("products", where: "ratio = 2", inspector: @inspector)
        )

      change = %NewRecord{
        relation: {"public", "products"},
        record: %{"id" => "1", "ratio" => "2.0"}
      }

      affected = Filter.affected_shapes(filter, change)

      assert affected == MapSet.new(["s1"])
    end

    test "does not route a float4 record Postgres considers unequal above the float32 boundary" do
      # `real = 16777217` compares in double precision, where 16777217.0 and 16777216.0 are
      # distinct; narrowing both index keys to float32 would collapse them and admit the excluded
      # record. Arrange
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("products", where: "ratio = 16777217", inspector: @inspector)
        )

      change = %NewRecord{
        relation: {"public", "products"},
        record: %{"id" => "1", "ratio" => "16777216"}
      }

      affected = Filter.affected_shapes(filter, change)

      assert affected == MapSet.new([])
    end

    test "does not route a value the operator considers unequal" do
      filter =
        Filter.new()
        |> Filter.add_shape(
          "s1",
          Shape.new!("products", where: "price = 100", inspector: @inspector)
        )

      change = %NewRecord{
        relation: {"public", "products"},
        record: %{"id" => "1", "price" => "101"}
      }

      affected = Filter.affected_shapes(filter, change)

      assert affected == MapSet.new([])
    end
  end
end
