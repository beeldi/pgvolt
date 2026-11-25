defmodule PgInterop.ArrayOracleTest do
  @moduledoc """
  Differential test of the array text parser against real Postgres.

  Every `text[]` column in a shape's predicate reaches the router as the text Postgres wrote into
  the replication stream, and `PgInterop.Array.parse/2` is the only thing standing between that
  text and the value the predicate compares. A parse that disagrees with Postgres by one character
  routes the row to the wrong shapes and nothing anywhere reports it, so the expectations here are
  asked of Postgres rather than written down: one direction feeds it values and parses back what it
  emitted, the other feeds it a literal and compares both parsers on it.
  """
  use ExUnit.Case, async: false

  import Support.DbSetup

  alias PgInterop.Array

  setup :with_shared_db

  @round_trips [
    {"plain elements", ["one", "two"]},
    {"an empty array", []},
    {"an empty string", [""]},
    {"a real null beside a null-looking string", [nil, "NULL", "null"]},
    {"a backslash", ["a\\b"]},
    {"consecutive backslashes", ["a\\\\b"]},
    {"a trailing backslash", ["path\\"]},
    {"a windows path", ["C:\\Users\\me\\file.txt"]},
    {"a quote", ["q\"z"]},
    {"a backslash and a quote together", ["back\\slash and \"quote\""]},
    {"braces", ["{brace}", "}", "{"]},
    {"a comma", ["comma,inside"]},
    {"surrounding whitespace", ["  padded  "]},
    {"a tab and a newline", ["tab\there", "new\nline"]},
    {"a nested array of awkward elements", [["a\\b", "c,d"], ["{e}", "\"f\""]]}
  ]

  @literals [
    {"an escaped comma", ~S|{a\,b,c}|},
    {"an escaped space", ~S|{a\ b}|},
    {"an escaped space before the closing brace", ~S|{a\  }|},
    {"an escaped null keyword", ~S|{\N\U\L\L}|},
    {"escaped braces", ~S|{\{x\}}|},
    {"an escaped character with no meaning of its own", ~S|{"a\bc"}|},
    {"an escaped backslash inside quotes", ~S|{"a\\b"}|},
    {"an escaped quote inside quotes", ~S|{"a\"b"}|},
    {"a quoted null keyword", ~S|{"null"}|},
    {"an unquoted null keyword in mixed case", ~S|{NuLl}|}
  ]

  for {name, values} <- @round_trips do
    test "Postgres' own encoding of #{name} parses back to it", %{db_conn: conn} do
      # Arrange
      values = unquote(Macro.escape(values))

      %Postgrex.Result{rows: [[encoded]]} =
        Postgrex.query!(conn, "SELECT $1::text[]::text", [values])

      # Act
      parsed = Array.parse(encoded)

      # Assert
      assert parsed == values, "Postgres wrote #{inspect(encoded)}"
    end
  end

  for {name, literal} <- @literals do
    test "a literal with #{name} parses as Postgres parses it", %{db_conn: conn} do
      # Arrange
      literal = unquote(literal)

      %Postgrex.Result{rows: [[expected]]} =
        Postgrex.query!(conn, "SELECT $1::text::text[]", [literal])

      # Act
      parsed = Array.parse(literal)

      # Assert
      assert parsed == expected
    end
  end

  # The direction the round that wrote this file never ran: what must *not* parse. A parser is only
  # pinned by both, and the interesting failure is the one that accepts what Postgres refuses —
  # `{a"b}` was exactly that until the clause rejecting a bare quote in an unquoted element landed.
  @both_accept [
    ~S|{a,b}|,
    ~S|{a,b} |,
    ~S|{a,b}  |,
    "{a,b}\t",
    "{a,b}\n",
    "{a,b}   \n ",
    ~S| {a,b}|,
    ~S|  {a,b}|,
    ~S|{ a , b }|,
    ~S|{}|,
    ~S|{NULL}|,
    ~S|{null}|,
    ~S|{"NULL"}|,
    ~S|{a b}|,
    ~S|{a\"b}|,
    # Dimension bounds whose lower bounds are all 1, which Postgres reads as the plain array.
    ~S|[1:2]={a,b}|,
    ~S|[1]={a}|
  ]

  @both_reject [
    ~S|{a,b|,
    ~S|a,b}|,
    ~S|{a,,b}|,
    ~S|{,a}|,
    ~S|{a,}|,
    ~S|{"a}|,
    ~S|{a}}|,
    ~S|{{a}|,
    ~S|{{1},2}|,
    ~S|{{1},{2,3}}|,
    ~S|{1,{2}}|,
    ~S|{{{{{{{1}}}}}}}|,
    ~S||,
    ~S| |,
    ~S|NULL|,
    ~S|{"a" "b"}|,
    ~S|{a,b}x|,
    ~S|{a\}|,
    ~S|{a"b}|,
    ~S|[1:3]={a,b}|,
    ~S|[2:1]={a}|,
    ~S|[0:1]={}|,
    ~S|[0:1][1:1]={a,b}|,
    ~S|[ 0 : 1 ]={a,b}|
  ]

  # Postgres accepts these and Electric does not. Refusing is the safe direction — a refused literal
  # is a refused shape, where accepting one Postgres reads differently would route rows by a
  # predicate the snapshot query never applied — and Postgres emits neither form, so the only way to
  # meet one is a client writing it by hand.
  @electric_stricter [
    # An array whose whole content is whitespace. Postgres reads it as empty.
    {~S|{ }|, []},
    {~S|{  }|, []},
    # Whitespace around dimension bounds, which Postgres never writes.
    {~S|[0:1] = {a,b}|, ["a", "b"]},
    {~S| [0:1]={a,b}|, ["a", "b"]}
  ]

  for input <- @both_accept do
    test "both parse #{inspect(input)}", %{pool: pool} do
      # Act
      parsed = Array.parse(unquote(input))

      # Assert
      assert {:ok, ^parsed} = cast_to_array(pool, unquote(input))
    end
  end

  for input <- @both_reject do
    test "both refuse #{inspect(input)}", %{pool: pool} do
      # Assert
      assert_raise RuntimeError, fn -> Array.parse(unquote(input)) end
      assert :error = cast_to_array(pool, unquote(input))
    end
  end

  for {input, value} <- @electric_stricter do
    test "Postgres parses #{inspect(input)} and Electric refuses it", %{pool: pool} do
      # Assert
      assert_raise RuntimeError, fn -> Array.parse(unquote(input)) end
      assert {:ok, unquote(value)} == cast_to_array(pool, unquote(input))
    end
  end

  # Postgres writes an array whose lower bounds are not all 1 with its bounds, in the replication
  # stream as in its output, so a row's value arrives that way.
  for literal <- [~S|[0:1]={a,b}|, ~S|[-3:-2][1:1]={{a},{b}}|, ~S|[1:1][5:6]={{a,"b,c"}}|] do
    test "Postgres' own encoding of #{literal} parses to its lower bounds and elements",
         %{db_conn: conn} do
      # Arrange
      %Postgrex.Result{rows: [[encoded, elements, lower_bounds]]} =
        Postgrex.query!(
          conn,
          """
          SELECT a::text, a, ARRAY(SELECT array_lower(a, d) FROM generate_series(1, array_ndims(a)) d)
          FROM (SELECT $1::text::text[] AS a) r
          """,
          [unquote(literal)]
        )

      # Act
      parsed = Array.parse(encoded)

      # Assert
      assert parsed == %Array.Bounded{lower_bounds: lower_bounds, elements: elements}
    end
  end

  defp cast_to_array(pool, input) do
    case Postgrex.transaction(pool, fn conn ->
           Postgrex.query(conn, "SELECT $1::text::text[]", [input])
         end) do
      {:ok, {:ok, %Postgrex.Result{rows: [[value]]}}} -> {:ok, value}
      _ -> :error
    end
  end

  test "a value survives being serialized and parsed back by Postgres", %{db_conn: conn} do
    # Arrange
    values = ["a\\b", "q\"z", "comma,inside", "{brace}", nil, "NULL"]

    # Act
    serialized = Array.serialize(values)

    # Assert
    %Postgrex.Result{rows: [[parsed]]} =
      Postgrex.query!(conn, "SELECT $1::text::text[]", [serialized])

    assert parsed == values
  end
end
