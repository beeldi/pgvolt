defmodule Electric.Replication.LocaleProjectionTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.DeletedRecord
  alias Electric.Replication.Changes.NewRecord
  alias Electric.Replication.Changes.UpdatedRecord

  # Postgres fills one generated column per language, and the publication carries them all. Projecting is
  # renaming the reader's language onto the column it knows, and dropping the languages it will not display.
  @backing ["name_EN", "name_FR", "name_NL"]
  @row %{
    "id" => "1",
    "name" => ~s({"FR":"Théière","EN":"Teapot","NL":"Theepot"}),
    "name_FR" => "Théière",
    "name_EN" => "Teapot",
    "name_NL" => "Theepot"
  }

  describe "project_locale/3" do
    test "renames the reader's language onto the column it knows" do
      change = %NewRecord{record: @row}

      assert %NewRecord{record: %{"id" => "1", "name" => "Teapot"}} =
               Changes.project_locale(change, "EN", ["name"], @backing)
    end

    test "drops the languages the reader will not display" do
      change = %NewRecord{record: @row}

      %NewRecord{record: record} = Changes.project_locale(change, "EN", ["name"], @backing)

      assert Map.keys(record) |> Enum.sort() == ["id", "name"]
    end

    test "leaves a column the caller did not declare alone" do
      change = %NewRecord{record: @row}

      assert %NewRecord{record: %{"name" => name}} =
               Changes.project_locale(change, "EN", ["other"], @backing)

      assert name == @row["name"]
    end

    test "passes a row replicated before the columns existed through untouched" do
      change = %NewRecord{record: %{"id" => "1", "name" => "Théière"}}

      assert %NewRecord{record: %{"id" => "1", "name" => "Théière"}} =
               Changes.project_locale(change, "EN", ["name"], @backing)
    end

    test "projects both sides of an update so a reader sees one language on each" do
      change = %UpdatedRecord{
        old_record: @row,
        record: %{
          "id" => "1",
          "name" => "{}",
          "name_FR" => "Tasse",
          "name_EN" => "Cup",
          "name_NL" => "Kop"
        }
      }

      assert %UpdatedRecord{old_record: %{"name" => "Theepot"}, record: %{"name" => "Kop"}} =
               Changes.project_locale(change, "NL", ["name"], @backing)
    end

    test "projects the row a delete carries" do
      change = %DeletedRecord{old_record: @row}

      assert %DeletedRecord{old_record: %{"name" => "Théière"}} =
               Changes.project_locale(change, "FR", ["name"], @backing)
    end

    test "leaves a change alone when no language was asked for" do
      change = %NewRecord{record: @row}

      assert ^change = Changes.project_locale(change, nil, ["name"], @backing)
    end

    test "leaves a change alone when no column was declared" do
      change = %NewRecord{record: @row}

      assert ^change = Changes.project_locale(change, "EN", [], @backing)
    end
  end

  describe "Shape.localized_backing_columns/2" do
    alias Electric.Shapes.Shape

    defp column(name, generated?), do: %{name: name, is_generated: generated?}

    test "names the generated columns that back a localized one" do
      columns = [column("name", false), column("name_EN", true), column("name_FR", true)]

      assert Shape.localized_backing_columns(columns, ["name"]) == ["name_EN", "name_FR"]
    end

    test "leaves a real column alone even when it shares the prefix" do
      # `name_short` is a column of its own, not a translation of `name`: dropping it would lose data.
      columns = [column("name", false), column("name_short", false), column("name_search", false)]

      assert Shape.localized_backing_columns(columns, ["name"]) == []
    end

    test "backs each localized column without swallowing the other" do
      columns = [
        column("name", false),
        column("name_EN", true),
        column("name_short", false),
        column("name_short_EN", true)
      ]

      assert Shape.localized_backing_columns(columns, ["name", "name_short"]) ==
               ["name_EN", "name_short_EN"]
    end

    test "answers nothing when the shape declares no localized column" do
      assert Shape.localized_backing_columns([column("name_EN", true)], []) == []
    end
  end

  # The two paths a live change can take through a shape. Both have to project, and both have to keep the
  # backing columns out of what the reader sees.
  describe "Shape.convert_change/3 on a localized shape" do
    alias Electric.Shapes.Shape

    defp localized_shape(overrides) do
      Map.merge(
        %Shape{
          root_table: {"public", "CatalogueSection"},
          root_table_id: 1,
          root_pk: ["id"],
          locale: "EN",
          localized_columns: ["name"],
          localized_backing_columns: @backing
        },
        Map.new(overrides)
      )
    end

    test "projects before honouring a column selection that omits the backing column" do
      shape =
        localized_shape(
          selected_columns: ["id", "name"],
          explicitly_selected_columns: ["id", "name"]
        )

      change = %NewRecord{relation: {"public", "CatalogueSection"}, record: @row}

      assert [%NewRecord{record: record}] = Shape.convert_change(shape, change)
      assert record == %{"id" => "1", "name" => "Teapot"}
    end

    test "projects a whole-table shape, which used to take a fast path that skipped it" do
      shape = localized_shape(flags: %{selects_all_columns: true})

      updated = %{
        "id" => "1",
        "name" => ~s({"FR":"Tasse","EN":"Cup","NL":"Kop"}),
        "name_FR" => "Tasse",
        "name_EN" => "Cup",
        "name_NL" => "Kop"
      }

      change = %UpdatedRecord{
        relation: {"public", "CatalogueSection"},
        old_record: @row,
        record: updated,
        changed_columns: MapSet.new(["name"])
      }

      assert [%UpdatedRecord{record: record}] = Shape.convert_change(shape, change)
      assert record["name"] == "Cup"
      refute Map.has_key?(record, "name_EN")
    end
  end
end
