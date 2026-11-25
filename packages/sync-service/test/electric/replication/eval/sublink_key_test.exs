defmodule Electric.Replication.Eval.SublinkKeyTest do
  use Support.TransactionCase, async: true

  alias Electric.Replication.Eval.SublinkKey

  doctest SublinkKey, import: true

  # Postgres allows 131072 digits before the point and 16383 after, past Decimal's default limits.
  @long_integer "1" <> String.duplicate("0", 131_071)
  @long_fraction "0." <> String.duplicate("7", 16_381)
  @sixty_digits "1" <> String.duplicate("0", 58)

  # Pairs of numeric text, with the keys equal exactly when Postgres calls the values equal.
  @pairs [
    {"1.50", "1.5"},
    {"1.50", "1.51"},
    {"100", "100.000"},
    {"100", "1e2"},
    {"0", "-0.00"},
    {"0.000", "0"},
    {"NaN", "NaN"},
    {"NaN", "nan"},
    {"NaN", "0"},
    {"Infinity", "inf"},
    {"Infinity", "-Infinity"},
    {"9007199254740993", "9007199254740992"},
    {"9007199254740993", "9007199254740993.000"},
    {"-12.30", "-12.3"},
    {"-12.3", "12.3"},
    {@long_integer <> ".000", @long_integer},
    {@long_integer, String.replace_suffix(@long_integer, "0", "1")},
    {@long_fraction <> "1", @long_fraction <> "10"},
    {@long_fraction <> "1", @long_fraction <> "2"},
    {@sixty_digits <> "1", @sixty_digits <> "2"},
    {"-" <> @sixty_digits <> "1.50", "-" <> @sixty_digits <> "1.5"}
  ]

  test "two numeric keys are equal exactly when Postgres calls the values equal", %{db_conn: conn} do
    for {left, right} <- @pairs do
      label = {String.slice(left, -20, 20), String.slice(right, -20, 20)}

      %{rows: [[postgres?]]} =
        Postgrex.query!(conn, "SELECT ($1::text)::numeric = ($2::text)::numeric", [left, right])

      assert {^label, {:ok, left_key}} = {label, SublinkKey.parse(left, :numeric)}
      assert {^label, {:ok, right_key}} = {label, SublinkKey.parse(right, :numeric)}
      assert {label, left_key === right_key} == {label, postgres?}
    end
  end

  test "a key binds as the value Postgres stored", %{db_conn: conn} do
    values = [
      "NaN",
      "Infinity",
      "-Infinity",
      "9007199254740993",
      "-12.30",
      "0.000",
      @long_fraction
    ]

    keys = Enum.map(values, &elem(SublinkKey.parse(&1, :numeric), 1))

    %{rows: [[matched]]} =
      Postgrex.query!(
        conn,
        "SELECT bool_and(v::numeric = ANY($1::numeric[])) FROM unnest($2::text[]) v",
        [keys, values]
      )

    assert matched
  end

  # Values Postgres stores that the evaluator's parse refuses, each with another spelling of the
  # same value.
  @text_keyed [
    {"float8", :float8, "NaN", "nan"},
    {"float8", :float8, "Infinity", "inf"},
    {"float8", :float8, "-Infinity", "-inf"},
    {"float4", :float4, "NaN", "NaN"},
    {"date", :date, "infinity", "+infinity"},
    {"date", :date, "-infinity", "-infinity"},
    {"date", :date, "0044-03-15 BC", "0044-3-15 BC"},
    {"date", :date, "10000-01-01", "10000-1-1"},
    {"time", :time, "24:00:00", "24:00"},
    {"timestamp", :timestamp, "infinity", "infinity"},
    {"timestamp", :timestamp, "0044-03-15 12:00:00 BC", "0044-03-15 12:00 BC"},
    {"timestamptz", :timestamptz, "-infinity", "-infinity"}
  ]

  test "a value the evaluator's parse refuses is keyed by the text Postgres writes", %{
    db_conn: conn
  } do
    for {sql_type, type, spelling, other_spelling} <- @text_keyed do
      label = {sql_type, spelling}

      %{rows: [[text, other_text]]} =
        Postgrex.query!(
          conn,
          "SELECT $1::text::#{sql_type}::text, $2::text::#{sql_type}::text",
          [spelling, other_spelling]
        )

      assert {label, SublinkKey.parse(text, type)} ==
               {label, {:ok, %SublinkKey.Text{text: text}}}

      assert {label, SublinkKey.parse(other_text, type)} == {label, SublinkKey.parse(text, type)}
    end
  end

  test "a key held as text binds back as the value Postgres stored", %{db_conn: conn} do
    for {sql_type, type, spelling, _} <- @text_keyed do
      %{rows: [[text]]} = Postgrex.query!(conn, "SELECT $1::text::#{sql_type}::text", [spelling])
      {:ok, %SublinkKey.Text{text: key_text}} = SublinkKey.parse(text, type)

      %{rows: [[bound?]]} =
        Postgrex.query!(
          conn,
          "SELECT $1::text::#{sql_type} = ANY ($2::text[]::#{sql_type}[])",
          [spelling, [key_text]]
        )

      assert {sql_type, spelling, bound?} == {sql_type, spelling, true}
    end
  end

  # {SQL type, type, a spelling, another spelling of the same array, its key}
  @element_keyed [
    {"float8[]", {:array, :float8}, "{NaN,1}", "{nan,1.0}", [%SublinkKey.Text{text: "NaN"}, 1.0]},
    {"date[]", {:array, :date}, "{infinity,2026-01-05}", "{+infinity,2026-1-5}",
     [%SublinkKey.Text{text: "infinity"}, ~D[2026-01-05]]},
    {"timestamp[]", {:array, :timestamp}, ~s|{"0044-03-15 12:00 BC",NULL}|,
     ~s|{"0044-3-15 12:00:00 BC",null}|, [%SublinkKey.Text{text: "0044-03-15 12:00:00 BC"}, nil]}
  ]

  test "an array the evaluator's parse refuses is keyed element by element", %{db_conn: conn} do
    for {sql_type, type, spelling, other_spelling, key} <- @element_keyed do
      %{rows: [[text, other_text]]} =
        Postgrex.query!(
          conn,
          "SELECT $1::text::#{sql_type}::text, $2::text::#{sql_type}::text",
          [spelling, other_spelling]
        )

      assert {sql_type, SublinkKey.parse(text, type)} == {sql_type, {:ok, key}}
      assert {sql_type, SublinkKey.parse(other_text, type)} == {sql_type, {:ok, key}}
    end
  end

  # `==` takes `-0.0` for `0.0`, as the subquery index's ordered set does, so the key is matched.
  test "a float zero beside a refused element is unsigned" do
    for type <- [:float4, :float8] do
      assert {:ok, [%SublinkKey.Text{text: "NaN"}, zero]} =
               SublinkKey.parse("{NaN,-0}", {:array, type})

      assert zero === 0.0
    end
  end

  # Postgres 17 stores an interval at infinity, which the evaluator refuses: its array's other
  # elements compare by their span.
  test "the parsed elements of an array keyed element by element take their canonical form" do
    assert SublinkKey.parse("{infinity,\"1 mon\"}", {:array, :interval}) ===
             SublinkKey.parse("{infinity,\"30 days\"}", {:array, :interval})

    assert SublinkKey.parse("{NaN,-0}", {:array, :float8}) ===
             SublinkKey.parse("{NaN,0}", {:array, :float8})
  end

  test "a type with no text key still refuses a value its parse refuses" do
    assert SublinkKey.parse("not a number", :int4) == :error
    assert SublinkKey.parse("{1,x}", {:array, :int8}) == :error
    assert SublinkKey.parse("maybe", :bool) == :error
  end

  # Pairs of jsonb values. Each goes through Postgres first, as a key reads the text Postgres
  # writes for a jsonb column.
  @jsonb_pairs [
    {~s|{"rate": 1.0}|, ~s|{"rate": 1}|},
    {~s|{"rate": 1.5}|, ~s|{"rate": 1.51}|},
    {~s|{"a": 1, "b": 2}|, ~s|{"b": 2, "a": 1}|},
    {~s|{"a": 1, "a": 2}|, ~s|{"a": 2}|},
    {~s|{"a": 1}|, ~s|{"a": 1, "b": null}|},
    {~s|{"a": [1.10, {"b": 2.0}]}|, ~s|{"a": [1.1, {"b": 2}]}|},
    {"[1, 2]", "[2, 1]"},
    {"1e2", "100"},
    {"-0.0", "0"},
    {"-12.30", "-12.3"},
    {~s|"1"|, "1"},
    {"null", ~s|"null"|},
    {~s|"é"|, ~s|"\\u00e9"|},
    {"true", "false"},
    {"1." <> String.duplicate("0", 34), "1"},
    {String.duplicate("9", 1100) <> ".0", String.duplicate("9", 1100)},
    {String.duplicate("9", 1100), String.duplicate("9", 1099) <> "8"},
    {~s|"1.50"|, ~s|"1.5"|},
    {~s|["a\\" 1.50", 1.50]|, ~s|["a\\" 1.5", 1.5]|},
    {~s|["a\\" 1.50", 1.50]|, ~s|["a\\" 1.50", 1.5]|},
    {~s|{"1.0": 1}|, ~s|{"1": 1}|},
    {~s|{"1.0": 1.0}|, ~s|{"1.0": 1}|},
    {"0.000", "0"},
    {"10.010", "10.01"},
    {"100.0", "1"},
    {String.duplicate("[", 1000) <> "1.0" <> String.duplicate("]", 1000),
     String.duplicate("[", 1000) <> "1" <> String.duplicate("]", 1000)}
  ]

  test "two jsonb keys are equal exactly when Postgres calls the values equal",
       %{db_conn: conn} do
    for {left, right} <- @jsonb_pairs do
      %{rows: [[postgres?]]} =
        Postgrex.query!(conn, "SELECT ($1::text)::jsonb = ($2::text)::jsonb", [left, right])

      [left, right] = jsonb_column_text(conn, [left, right])
      assert {:ok, left_key} = SublinkKey.parse(left, :jsonb)
      assert {:ok, right_key} = SublinkKey.parse(right, :jsonb)
      assert {{left, right}, left_key === right_key} == {{left, right}, postgres?}
    end
  end

  test "two char(n) keys are equal exactly when Postgres calls the values equal",
       %{db_conn: conn} do
    pairs = [{"ab ", "ab   "}, {"ab", "ab  "}, {" ab", "ab"}, {"ab c", "ab"}, {"ab\t", "ab"}]

    for {left, right} <- pairs do
      %{rows: [[postgres?]]} =
        Postgrex.query!(conn, "SELECT ($1::text)::bpchar = ($2::text)::bpchar", [left, right])

      assert {:ok, left_key} = SublinkKey.parse(left, :bpchar)
      assert {:ok, right_key} = SublinkKey.parse(right, :bpchar)
      assert {{left, right}, left_key === right_key} == {{left, right}, postgres?}
    end
  end

  test "a jsonb or char(n) key binds as the value Postgres stored", %{db_conn: conn} do
    values = jsonb_column_text(conn, Enum.flat_map(@jsonb_pairs, &Tuple.to_list/1))
    keys = Enum.map(values, &elem(SublinkKey.parse(&1, :jsonb), 1))

    %{rows: [[matched]]} =
      Postgrex.query!(
        conn,
        "SELECT bool_and(k::jsonb = v::jsonb) FROM unnest($1::text[], $2::text[]) AS t(k, v)",
        [keys, values]
      )

    assert matched

    %{rows: [[matched]]} =
      Postgrex.query!(
        conn,
        "SELECT ($1::text)::char(5) = ANY($2::text[]::bpchar[])",
        ["ab", [elem(SublinkKey.parse("ab   ", :bpchar), 1)]]
      )

    assert matched
  end

  test "other types keep the evaluator's parse, and NULL stays NULL" do
    assert SublinkKey.parse("12", :int4) == {:ok, 12}
    assert SublinkKey.parse("1.5", :float8) == {:ok, 1.5}
    assert SublinkKey.parse(nil, :numeric) == {:ok, nil}
    assert SublinkKey.parse("not a number", :numeric) == :error
  end

  defp jsonb_column_text(conn, values) do
    %{rows: [[texts]]} =
      Postgrex.query!(
        conn,
        "SELECT array(SELECT v::jsonb::text FROM unnest($1::text[]) WITH ORDINALITY AS t(v, i) ORDER BY i)",
        [values]
      )

    texts
  end
end
