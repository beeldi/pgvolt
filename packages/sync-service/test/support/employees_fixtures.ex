defmodule Support.EmployeesFixtures do
  @moduledoc """
  The `public.employees` table the shape suites route against.

  These suites exist to prove two code paths agree — routing against conversion, shared
  encoding against independent encoding — and such a comparison only means anything while both
  sides see the same table, so the table lives here rather than once per suite.
  """
  alias Electric.Postgres.Lsn
  alias Electric.Replication.Changes.NewRecord
  alias Electric.Replication.LogOffset
  alias Electric.Shapes.Shape
  alias Support.StubInspector

  @relation {"public", "employees"}
  @root_table "public.employees"

  def relation, do: @relation

  @doc """
  Inspector over the named columns. A bare name is a text column, `id` being the primary key;
  a `{name, type}` pair carries any other type, and `{name, {:array, type}}` an array of it.
  """
  def inspector(columns) do
    StubInspector.new(tables: ["employees"], columns: Enum.map(columns, &column/1))
  end

  defp column("id"), do: %{name: "id", type: "text", pk_position: 0}
  defp column({name, {:array, type}}), do: %{name: name, array_type: type}
  defp column({name, type}), do: %{name: name, type: type}
  defp column(name), do: %{name: name, type: "text"}

  def shape!(inspector, opts), do: Shape.new!(@root_table, [inspector: inspector] ++ opts)

  def key(id), do: ~s|"public"."employees"/"#{id}"|

  def insert(record, opts \\ []) do
    %NewRecord{
      relation: @relation,
      record: record,
      log_offset: Keyword.get(opts, :log_offset, LogOffset.new(Lsn.from_integer(1000), 1)),
      key: key(record["id"])
    }
  end
end
