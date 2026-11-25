defmodule Electric.Replication.Eval.TextInputOracleTest do
  @moduledoc """
  Differential test of the scalar text input functions against real Postgres.

  Every non-text column a shape filters on reaches the evaluator as the text Postgres wrote, and
  `Env.parse_const/3` is what turns it back into a value to compare. The array parser next door was
  wrong in both of its directions, and these are the same kind of code: a second implementation of a
  Postgres text format, reached both from the replication stream and from a literal the client wrote
  in a where clause.

  So both directions are asked of Postgres. One feeds it values and checks that what it emitted
  parses, and that Electric's own equality and ordering over the results agree with Postgres' over
  the values. The other feeds it the input forms a client may write and checks each parses to the
  same value as the canonical text of the same thing.
  """
  use ExUnit.Case, async: false

  import Support.DbSetup

  alias Electric.Replication.Eval.Env

  setup :with_shared_db

  @env Env.new()

  @emitted [
    {:bool, "bool", ["true", "false"]},
    {:int2, "int2", ["0", "-32768", "32767"]},
    {:int4, "int4", ["0", "-2147483648", "2147483647"]},
    {:int8, "int8", ["0", "-9223372036854775808", "9223372036854775807"]},
    {:float8, "float8", ["0", "-0.0", "0.1", "1e20", "3.141592653589793"]},
    {:float4, "float4", ["0", "0.1", "3.14159"]},
    {:date, "date", ["2026-08-19", "0001-01-01", "9999-12-31"]},
    {:time, "time", ["00:00:00", "12:30:00", "00:30:00", "23:59:59.123456"]},
    {:timestamp, "timestamp", ["2026-08-19 14:00:00", "2026-08-19 14:00:00.123456"]},
    {:timestamptz, "timestamptz", ["2026-08-19 14:00:00+00", "2026-08-19 14:00:00.123456+00"]},
    {:uuid, "uuid", ["7c9e6679-7425-40de-944b-e07fc1f90ae7"]}
  ]

  @accepted [
    {:bool, "bool", "true", ["t", "T", "TRUE", "True", "yes", "y", "on", "1", " true "]},
    {:bool, "bool", "false", ["f", "FALSE", "no", "n", "off", "of", "0", " false "]},
    {:int4, "int4", "42", ["+42", " 42 ", "42"]},
    {:float8, "float8", "0.5", [" 0.5 ", "5e-1"]},
    {:time, "time", "12:30:00",
     ["12:30 PM", "12:30:00 PM", "12:30", "12:30 pm", "00:30 PM", "0:30 PM"]},
    {:time, "time", "00:30:00", ["12:30 AM", "12:30:00 AM", "00:30 AM", "0:30 AM"]},
    {:time, "time", "13:05:00", ["1:05 PM", "13:05"]},
    {:uuid, "uuid", "7c9e6679-7425-40de-944b-e07fc1f90ae7",
     ["7C9E6679-7425-40DE-944B-E07FC1F90AE7"]}
  ]

  @refused [
    {:float8, "float8", "Float.parse has no notion of a bare fraction or a signed one",
     [".5", "+.5"]},
    {:date, "date", "date input order is a DateStyle-dependent grammar; Electric parses ISO 8601",
     ["19-08-2026", "20260819", "Aug 19, 2026"]},
    {:timestamptz, "timestamptz",
     "a timezone name needs a tz database, and Postgres emits an offset under the pinned settings",
     ["2026-08-19 14:00:00 UTC", "2026-08-19 14:00:00 Europe/Paris"]},
    {:uuid, "uuid", "Postgres emits the canonical hyphenated form only",
     ["{7c9e6679-7425-40de-944b-e07fc1f90ae7}"]}
  ]

  for {type, pg_type, values} <- @emitted do
    test "what Postgres emits for #{pg_type} parses, and compares the way Postgres compares it",
         %{
           pool: pool
         } do
      {type, pg_type, values} = unquote(Macro.escape({type, pg_type, values}))

      {:ok, {parsed, disagreements}} =
        in_pinned_session(pool, fn conn ->
          parsed =
            Map.new(values, fn value ->
              text = scalar(conn, "SELECT $1::text::#{pg_type}::text", [value])
              {value, {text, Env.parse_const(@env, text, type)}}
            end)

          disagreements =
            for left <- values, right <- values, left != right, reduce: [] do
              acc ->
                {_, {:ok, left_value}} = parsed[left]
                {_, {:ok, right_value}} = parsed[right]

                postgres =
                  scalar(
                    conn,
                    "SELECT ARRAY[$1::text::#{pg_type} = $2::text::#{pg_type}, " <>
                      "$1::text::#{pg_type} < $2::text::#{pg_type}]",
                    [left, right]
                  )

                electric = [left_value == right_value, left_value < right_value]

                case postgres == electric do
                  true -> acc
                  false -> [{left, right, postgres, electric} | acc]
                end
            end

          {parsed, disagreements}
        end)

      unparseable = for {value, {text, :error}} <- parsed, do: {value, text}
      assert unparseable == []

      assert disagreements == [], """
      #{Enum.map_join(disagreements, "\n", fn {left, right, postgres, electric} -> "#{left} vs #{right}: postgres #{inspect(postgres)}, electric #{inspect(electric)}" end)}
      """
    end
  end

  for {type, pg_type, canonical, inputs} <- @accepted do
    test "every #{pg_type} input form for #{canonical} parses to the same value", %{pool: pool} do
      {type, pg_type, canonical, inputs} =
        unquote(Macro.escape({type, pg_type, canonical, inputs}))

      {:ok, {expected, actual}} =
        in_pinned_session(pool, fn conn ->
          expected = Env.parse_const(@env, canonical, type)

          actual =
            Map.new(inputs, fn input ->
              postgres_accepts =
                scalar(conn, "SELECT $1::text::#{pg_type} = $2::text::#{pg_type}", [
                  input,
                  canonical
                ])

              {input, {postgres_accepts, Env.parse_const(@env, input, type)}}
            end)

          {expected, actual}
        end)

      assert Enum.all?(actual, fn {_, {postgres_accepts, _}} -> postgres_accepts end)

      assert Map.new(actual, fn {input, {_, parsed}} -> {input, parsed} end) ==
               Map.new(inputs, &{&1, expected})
    end
  end

  for {type, pg_type, reason, inputs} <- @refused do
    test "the #{pg_type} input forms Postgres accepts and Electric refuses are exactly these", %{
      pool: pool
    } do
      {type, pg_type, reason, inputs} =
        unquote(Macro.escape({type, pg_type, reason, inputs}))

      accepted_by_postgres =
        Enum.filter(inputs, fn input ->
          {:ok, accepted} =
            in_pinned_session(pool, fn conn ->
              match?({:ok, _}, Postgrex.query(conn, "SELECT $1::text::#{pg_type}", [input]))
            end)

          accepted
        end)

      assert accepted_by_postgres == inputs

      assert Enum.map(inputs, &Env.parse_const(@env, &1, type)) ==
               Enum.map(inputs, fn _ -> :error end),
             "each of these is refused because #{reason}; widening one is a decision, not a drift"
    end
  end

  test "two numerics Postgres calls different are the same value here", %{pool: pool} do
    left = "12345678901234567890.12345"
    right = "12345678901234567890.12346"

    {:ok, postgres} =
      in_pinned_session(pool, fn conn ->
        scalar(conn, "SELECT $1::text::numeric = $2::text::numeric", [left, right])
      end)

    refute postgres

    assert Env.parse_const(@env, left, :numeric) == Env.parse_const(@env, right, :numeric),
           "`numeric(text)` delegates to the float8 parser, so anything past a double's precision " <>
             "collapses -- fail-open on the row path, unlike every other divergence here"
  end

  test "float4 narrows to single precision, so it is not equal to the float8 of the same literal",
       %{pool: pool} do
    literal = "0.1"

    {:ok, postgres_equal?} =
      in_pinned_session(pool, fn conn ->
        scalar(conn, "SELECT $1::text::float4 = $1::text::float8", [literal])
      end)

    {:ok, narrowed} = Env.parse_const(@env, literal, :float4)
    {:ok, wide} = Env.parse_const(@env, literal, :float8)

    refute postgres_equal?

    assert narrowed != wide,
           "0.1::float4 rounds to a single-precision value distinct from 0.1::float8; " <>
             "parsing float4 as a float8 makes them wrongly equal"
  end
end
