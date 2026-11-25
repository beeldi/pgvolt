defmodule PgInterop.IntervalOracleTest do
  @moduledoc """
  Differential test of the interval parser and formatter against real Postgres.

  The last of Electric's re-implementations of a Postgres text format. It is reached only from
  literals and function results — `interval` is not a column type a shape can filter on — so a
  divergence costs a clause that means the wrong thing rather than a row routed wrongly. It is
  covered here for the same reason the others were: the four existing suites assert what we believe
  Postgres does, and none of them ever asked it.
  """
  use ExUnit.Case, async: false

  import Support.DbSetup

  alias PgInterop.Interval

  setup :with_shared_db

  test "ago is a case-insensitive token across interval whitespace", %{pool: pool} do
    {:ok, comparisons} =
      in_pinned_session(pool, fn conn ->
        for literal <- ["1 day AGO", "1 day\n2 hours\nago", "1 day\tAgO \n"] do
          expected = scalar(conn, "SELECT $1::text::interval::text", [literal])
          actual = with {:ok, parsed} <- Interval.parse(literal), do: Interval.format(parsed)
          {literal, actual, expected}
        end
      end)

    assert Enum.all?(comparisons, fn {_, actual, expected} -> actual == expected end),
           inspect(comparisons)
  end

  @literals [
    "1-2",
    "3 4:05:06",
    "5 minutes 3d 4 hours 6",
    "P1Y2M3DT4H5M6S",
    "P0001-02-03T04:05:06",
    "1 year",
    "1 month",
    "1 day",
    "24 hours",
    "1 week",
    "-1 day",
    "1 day ago",
    "1.5 hours",
    "00:00:00.123456",
    "-00:00:01",
    "2 years 3 months -4 days",
    "1 millennium",
    "1 decade",
    "10000 days",
    "-1-2",
    "04:05:06",
    "4:05",
    "1.5",
    "@ 1 day",
    "1 mil",
    "1 century",
    "2 millennia",
    "1 day 2 hours ago",
    "P1W",
    "PT1H",
    "1 sec ago",
    "-1 year 2 mons"
  ]

  test "every documented input format formats back to what Postgres prints", %{pool: pool} do
    # Act
    {:ok, mismatches} =
      in_pinned_session(pool, fn conn ->
        for literal <- @literals, reduce: [] do
          acc ->
            postgres = scalar(conn, "SELECT $1::text::interval::text", [literal])

            electric = literal |> Interval.parse!() |> Interval.format()

            case postgres == electric do
              true -> acc
              false -> [{literal, postgres, electric} | acc]
            end
        end
      end)

    # Assert
    assert mismatches == [], """
    #{Enum.map_join(mismatches, "\n", fn {literal, postgres, electric} -> "#{literal}: postgres #{postgres}, electric #{electric}" end)}
    """
  end

  test "intervals compare the way Postgres compares them", %{pool: pool} do
    # Arrange
    parsed = Map.new(@literals, &{&1, Interval.parse!(&1)})

    # Act
    {:ok, mismatches} =
      in_pinned_session(pool, fn conn ->
        for left <- @literals, right <- @literals, left != right, reduce: [] do
          acc ->
            postgres =
              scalar(
                conn,
                "SELECT ARRAY[$1::text::interval = $2::text::interval, " <>
                  "$1::text::interval < $2::text::interval]",
                [left, right]
              )

            comparison = Interval.compare(parsed[left], parsed[right])
            electric = [comparison == :eq, comparison == :lt]

            case postgres == electric do
              true -> acc
              false -> [{left, right, postgres, electric} | acc]
            end
        end
      end)

    # Assert
    assert mismatches == [], """
    #{Enum.map_join(mismatches, "\n", fn {left, right, postgres, electric} -> "#{left} vs #{right}: postgres #{inspect(postgres)}, electric #{inspect(electric)}" end)}
    """
  end
end
