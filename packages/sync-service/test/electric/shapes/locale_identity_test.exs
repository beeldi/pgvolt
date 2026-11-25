defmodule Electric.Shapes.LocaleIdentityTest do
  use ExUnit.Case, async: true

  alias Electric.Shapes.Shape

  # Two readers on different languages must never land on the same shape: the handle is derived from this
  # comparable, so a locale missing from it would serve Dutch rows to an English reader.
  defp shape(locale) do
    %Shape{
      root_table: {"public", "CatalogueProduct"},
      root_table_id: 1,
      root_pk: ["id"],
      selected_columns: ["id", "name"],
      explicitly_selected_columns: ["id", "name"],
      locale: locale,
      localized_columns: ["name"]
    }
  end

  test "tells two locales of the same table apart" do
    refute Shape.comparable(shape("EN")) == Shape.comparable(shape("NL"))
  end

  test "tells a projected shape apart from an unprojected one" do
    refute Shape.comparable(shape("FR")) == Shape.comparable(shape(nil))
  end

  test "keeps two readers of the same locale on one shape" do
    assert Shape.comparable(shape("EN")) == Shape.comparable(shape("EN"))
  end

  test "keeps locales in separate fan-out classes" do
    refute Shape.fanout_class(shape("EN")) == Shape.fanout_class(shape("NL"))
  end

  describe "validate_locale/1" do
    test "accepts a language tag and the absence of one" do
      assert {:ok, "EN"} = Shape.validate_locale("EN")
      assert {:ok, "pt-BR"} = Shape.validate_locale("pt-BR")
      assert {:ok, nil} = Shape.validate_locale(nil)
    end

    test "refuses a locale carrying a quote, which would reach a SQL literal" do
      assert {:error, _} = Shape.validate_locale("E'N")
    end

    test "refuses a locale that is neither a string nor absent" do
      assert {:error, _} = Shape.validate_locale(42)
    end
  end

  # A shape is written to disk and read back; losing the locale there gives it the legacy identity and
  # stops it projecting, with nothing to show for it until a reader sees the wrong language.
  describe "JSON round-trip" do
    test "keeps the language and the columns it projects" do
      # Arrange
      original = %{shape("EN") | localized_backing_columns: ["name_EN", "name_FR"]}

      # Act
      {:ok, restored} =
        original
        |> Shape.to_json_safe()
        |> Jason.encode!()
        |> Jason.decode!()
        |> Shape.from_json_safe()

      # Assert
      assert restored.locale == "EN"
      assert restored.localized_columns == ["name"]
      assert restored.localized_backing_columns == ["name_EN", "name_FR"]
      assert Shape.comparable(restored) == Shape.comparable(original)
    end

    test "reads a payload written before the language existed" do
      # Arrange
      legacy =
        shape(nil)
        |> Shape.to_json_safe()
        |> Map.drop([:locale, :localized_columns, :localized_backing_columns])

      # Act
      {:ok, restored} = legacy |> Jason.encode!() |> Jason.decode!() |> Shape.from_json_safe()

      # Assert
      assert restored.locale == nil
      assert restored.localized_columns == []
      assert restored.localized_backing_columns == []
    end
  end

  describe "guarding what can actually be projected" do
    defp catalogue(names_generated),
      do: Enum.map(names_generated, fn {n, g} -> %{name: n, is_generated: g} end)

    test "declines a language the table was never migrated for" do
      # Arrange
      columns = catalogue([{"name", false}, {"name_FR", true}, {"name_EN", true}])

      # Act
      declared =
        Shape.localized_columns(%{locale: "DE", localized_columns: ["name"]}, ["name"], columns)

      # Assert
      assert declared == []
    end

    test "declines a column whose backing one is not generated" do
      # Arrange
      columns = catalogue([{"name", false}, {"name_EN", false}])

      # Act
      declared =
        Shape.localized_columns(%{locale: "EN", localized_columns: ["name"]}, ["name"], columns)

      # Assert
      assert declared == []
    end

    test "keeps a column the catalogue can serve" do
      # Arrange
      columns = catalogue([{"name", false}, {"name_EN", true}])

      # Act
      declared =
        Shape.localized_columns(%{locale: "EN", localized_columns: ["name"]}, ["name"], columns)

      # Assert
      assert declared == ["name"]
    end

    test "declines a backing column left out of queryable_columns" do
      # Arrange
      columns = catalogue([{"id", false}, {"name", false}, {"name_EN", true}])
      opts = %{locale: "EN", localized_columns: ["name"], queryable_columns: ["id", "name"]}

      # Act
      declared = Shape.localized_columns(opts, ["id", "name"], columns)

      # Assert
      assert declared == []
    end

    test "keeps a backing column listed in queryable_columns" do
      # Arrange
      columns = catalogue([{"id", false}, {"name", false}, {"name_EN", true}])

      opts = %{
        locale: "EN",
        localized_columns: ["name"],
        queryable_columns: ["id", "name", "name_EN"]
      }

      # Act
      declared = Shape.localized_columns(opts, ["id", "name"], columns)

      # Assert
      assert declared == ["name"]
    end
  end

  describe "is_affected_by_relation_change?/2" do
    alias Electric.Replication.Changes

    test "invalidates the shape when the column it reads behind the scenes changes" do
      # Arrange
      localized = %{
        shape("EN")
        | selected_columns: ["id", "name"],
          localized_backing_columns: ["name_EN"]
      }

      change = %Changes.Relation{
        id: 1,
        schema: "public",
        table: "CatalogueProduct",
        columns: [],
        affected_columns: ["name_EN"]
      }

      # Act & Assert
      assert Shape.is_affected_by_relation_change?(localized, change)
    end

    test "leaves a shape alone when an unrelated column changes" do
      # Arrange
      localized = %{
        shape("EN")
        | selected_columns: ["id", "name"],
          localized_backing_columns: ["name_EN"]
      }

      change = %Changes.Relation{
        id: 1,
        schema: "public",
        table: "CatalogueProduct",
        columns: [],
        affected_columns: ["internalCode"]
      }

      # Act & Assert
      refute Shape.is_affected_by_relation_change?(localized, change)
    end
  end
end
