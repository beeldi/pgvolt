defmodule Electric.Shapes.LocaleDependencyShapeTest do
  @moduledoc """
  A subquery dependency decides membership, so it reads the stored value. Inheriting the root shape's locale
  would have it compare on one language, and its own table may carry no backing column at all.
  """
  use ExUnit.Case, async: true

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup

  alias Electric.Shapes.Shape

  setup [
    :with_stack_id_from_test,
    :with_shared_db,
    :with_persistent_kv,
    :with_inspector,
    :with_sql_execute
  ]

  @tag with_sql: [
         ~s|CREATE TABLE IF NOT EXISTS locale_parent (id INT PRIMARY KEY)|,
         ~s|CREATE TABLE IF NOT EXISTS locale_child (
              id INT PRIMARY KEY,
              par_id INT REFERENCES locale_parent(id),
              name JSONB,
              "name_EN" TEXT GENERATED ALWAYS AS (name ->> 'EN') STORED
            )|
       ]
  test "leaves the dependency shape unprojected", %{inspector: inspector} do
    # Act
    {:ok, shape} =
      Shape.new("locale_child",
        inspector: inspector,
        locale: "EN",
        localized_columns: ["name"],
        where: "par_id IN (SELECT id FROM locale_parent)"
      )

    # Assert
    assert shape.locale == "EN"
    assert shape.localized_columns == ["name"]

    assert [%Shape{root_table: {"public", "locale_parent"}} = dependency] =
             shape.shape_dependencies

    assert dependency.locale == nil
    assert dependency.localized_columns == []
    assert dependency.localized_backing_columns == []
  end

  @tag with_sql: [
         ~s|CREATE TABLE IF NOT EXISTS locale_partial (id INT PRIMARY KEY, name JSONB,
              "name_FR" TEXT GENERATED ALWAYS AS (name ->> 'FR') STORED)|
       ]
  test "declines a language the table carries no column for", %{inspector: inspector} do
    # Act
    {:ok, shape} =
      Shape.new("locale_partial", inspector: inspector, locale: "DE", localized_columns: ["name"])

    # Assert
    assert shape.localized_columns == []
    assert shape.localized_backing_columns == []
  end
end
