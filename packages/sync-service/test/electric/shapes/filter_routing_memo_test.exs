defmodule Electric.Shapes.FilterRoutingMemoTest do
  @moduledoc """
  The routing memo must be invisible: for any change stream, a filter that has already routed a
  record must answer exactly like a filter that never saw it, and any shape add/remove must
  drop cached decisions for that table. A fresh filter per assertion is the oracle.
  """
  use ExUnit.Case, async: true

  alias Electric.Replication.Changes.DeletedRecord
  alias Electric.Replication.Changes.UpdatedRecord
  alias Electric.Shapes.Filter

  import Support.EmployeesFixtures, only: [inspector: 1, insert: 1, relation: 0, shape!: 2]

  @inspector inspector(~w[id firmId divisionId deletedAt note])

  defp shape!(where), do: shape!(@inspector, where: where)

  defp record(firm, division, deleted \\ nil) do
    %{
      "id" => "eq-1",
      "firmId" => firm,
      "divisionId" => division,
      "deletedAt" => deleted,
      "note" => "n"
    }
  end

  # A mix of routing paths: indexed equality with residual, IN-list, OR disjunction,
  # unindexed inequality, and a where-less catch-all.
  defp shapes do
    [
      {"eq", shape!(~s|"firmId" = 'F0' AND "deletedAt" IS NULL|)},
      {"in", shape!(~s|"firmId" = 'F0' AND "divisionId" IN ('D1','D2')|)},
      {"or", shape!(~s|"divisionId" = 'D1' OR "divisionId" = 'D9'|)},
      {"neq", shape!(~s|"note" != 'skip'|)},
      {"all", shape!(nil)}
    ]
  end

  defp new_filter(shape_list \\ nil, opts \\ [filter_routing_memo: true]) do
    Enum.reduce(shape_list || shapes(), Filter.new(opts), fn {id, shape}, filter ->
      Filter.add_shape(filter, id, shape)
    end)
  end

  defp changes do
    [
      insert(record("F0", "D1")),
      insert(record("F0", "D2")),
      insert(record("F0", "D3")),
      insert(record("F1", "D1")),
      insert(record("F0", "D1", "2026-01-01")),
      insert(%{record("F0", "D1") | "note" => "skip"}),
      %DeletedRecord{relation: relation(), old_record: record("F0", "D2")},
      UpdatedRecord.new(
        relation: relation(),
        old_record: record("F0", "D1"),
        record: record("F1", "D9")
      )
    ]
  end

  test "a memoized answer is identical to a fresh filter's answer, for every change kind" do
    # Arrange
    filter = new_filter()

    Enum.each(changes(), fn change ->
      # Act
      first = Filter.affected_shapes(filter, change)
      memoized = Filter.affected_shapes(filter, change)
      oracle = Filter.affected_shapes(new_filter(), change)

      # Assert
      assert memoized == first, "memo hit diverged from its own miss for #{inspect(change)}"
      assert memoized == oracle, "memo diverged from a fresh filter for #{inspect(change)}"
    end)
  end

  test "adding a shape drops cached decisions for its table" do
    # Arrange
    filter = new_filter()
    change = insert(record("F0", "D7"))
    before_add = Filter.affected_shapes(filter, change)
    refute MapSet.member?(before_add, "late")

    # Act
    filter = Filter.add_shape(filter, "late", shape!(~s|"divisionId" = 'D7'|))

    # Assert
    assert MapSet.member?(Filter.affected_shapes(filter, change), "late")
  end

  test "removing a shape drops cached decisions for its table" do
    # Arrange
    filter = new_filter()
    change = insert(record("F0", "D1"))
    assert MapSet.member?(Filter.affected_shapes(filter, change), "in")

    # Act
    filter = Filter.remove_shape(filter, "in")

    # Assert
    refute MapSet.member?(Filter.affected_shapes(filter, change), "in")

    assert Filter.affected_shapes(filter, change) ==
             Filter.affected_shapes(new_filter(), change) |> MapSet.delete("in")
  end

  test "the memo stays bounded and correct past its cap" do
    # Arrange
    filter = new_filter()

    # Act
    Enum.each(1..9_000, fn i ->
      Filter.affected_shapes(filter, insert(record("F0", "B-#{i}")))
    end)

    # Assert
    change = insert(record("F0", "D1"))
    assert Filter.affected_shapes(filter, change) == Filter.affected_shapes(new_filter(), change)
    assert :ets.info(filter.route_memo_table, :size) <= 8192
  end

  test "the memo stays bounded in bytes when every entry holds a wide shape set" do
    # Arrange
    # 400 shapes reading the same routing field, all matching, so every distinct division id
    # caches a 400-id set. The entry count stays well under its cap; the bytes are what grows
    # (measured at 83 MiB across these 4000 keys with no byte budget).
    wide = Enum.map(1..400, fn i -> {"wide-#{i}", shape!(~s|"divisionId" != 'skip-#{i}'|)} end)
    filter = new_filter(wide)

    # Act
    Enum.each(1..4_000, fn i ->
      Filter.affected_shapes(filter, insert(record("F0", "B-#{i}")))
    end)

    # Assert
    change = insert(record("F0", "D1"))

    assert Filter.affected_shapes(filter, change) ==
             Filter.affected_shapes(new_filter(wide), change)

    assert :ets.info(filter.route_memo_table, :size) < 8192,
           "the entry cap, not the byte budget, is what bounded this run"

    assert :ets.info(filter.route_memo_table, :memory) * :erlang.system_info(:wordsize) <
             40 * 1024 * 1024
  end

  test "the memo is off unless asked for, and routing is unchanged" do
    # Arrange
    unmemoized = new_filter(shapes(), [])

    # Act + Assert
    Enum.each(changes(), fn change ->
      assert Filter.affected_shapes(unmemoized, change) ==
               Filter.affected_shapes(new_filter(), change)
    end)

    assert unmemoized.route_memo_table == nil, "the memo allocated a table it will never read"
    assert unmemoized.route_fields_table == nil
  end

  test "routing on a record with missing columns memoizes consistently" do
    # Arrange
    # Delete/update old records can carry only the replica identity columns; a missing column
    # and an SQL NULL must land on the same cached decision the direct evaluation makes.
    filter = new_filter()
    partial = %DeletedRecord{relation: relation(), old_record: %{"id" => "eq-9"}}

    # Act + Assert
    first = Filter.affected_shapes(filter, partial)
    assert Filter.affected_shapes(filter, partial) == first
    assert Filter.affected_shapes(new_filter(), partial) == first
  end
end
