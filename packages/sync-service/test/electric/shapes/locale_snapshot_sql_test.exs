defmodule Electric.Shapes.LocaleSnapshotSqlTest do
  @moduledoc """
  Postgres holds the projection as a stored generated column per language. The snapshot query reads that
  column under the name the reader knows, since Electric selects columns and cannot alias them.
  """
  use ExUnit.Case, async: true

  alias Electric.Shapes.Querying

  describe "pg_column_reference/3" do
    test "reads a column the shape did not declare localized as it stands" do
      assert Querying.pg_column_reference("name", false, "EN") == ~s["name"]
    end

    test "reads the language's column for a localized one" do
      assert Querying.pg_column_reference("name", true, "EN") == ~s["name_EN"]
    end

    test "escapes a quote in the column name exactly once" do
      # The caller used to escape the base name before this function escaped the combined identifier, which
      # doubled every quote and made PostgreSQL resolve a different column.
      assert Querying.pg_column_reference(~s(display"name), true, "EN") == ~s["display""name_EN"]
      assert Querying.pg_column_reference(~s(display"name), false, "EN") == ~s["display""name"]
    end
  end
end
