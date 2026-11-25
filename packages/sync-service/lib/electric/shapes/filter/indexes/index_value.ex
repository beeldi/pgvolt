defmodule Electric.Shapes.Filter.Indexes.IndexValue do
  @moduledoc """
  Canonicalises a value of a Postgres type into the ETS `:set` key an index routes on.

  The equality and inclusion indexes key their `:set` tables on a value, and a `:set` distinguishes
  keys by Erlang term equality. That is finer than the equality Postgres uses for several types, so a
  where-clause constant and the record value Postgres calls equal to it can carry different Elixir
  representations and land on distinct keys. A live change both engines match then routes nowhere and
  the shape silently stops syncing. This collapses the representational differences Postgres ignores
  so both sides of a comparison land on one key.

  The evaluator's array comparisons (`=`, `<>`, `@>`, `<@`, `&&`) key their elements by the same
  function, so that the evaluator and the indexes make one decision about a row.
  """

  alias PgInterop.Interval

  # `interval = interval` uses Postgres 30-day-month equality (PgInterop.Interval.compare/2), which
  # is coarser than the raw `%Interval{}` struct's term equality the ETS :set key uses, so two
  # intervals Postgres calls equal must collapse to one key or a row both engines match never routes.
  def index_value(:interval, %Interval{} = value), do: {:interval, Interval.comparable(value)}

  # `float8` and `float4` have a negative zero, which Postgres calls equal to zero and a term
  # comparison does not. `numeric` has none: `Casting.parse_numeric/1` reads `-0.00` as `0.0`, so
  # a `-0.0` numeric is a negative value too small for a float, which Postgres does not call zero.
  def index_value(type, value) when type in [:float8, :float4] and value == 0, do: 0.0

  # A numeric holds an integer exactly, so one past float precision keeps its value: rounding it
  # would match the float of its neighbour, which Postgres does not call equal.
  def index_value(:numeric, value) when is_integer(value) do
    float = value * 1.0
    if float == value, do: float, else: value
  end

  # A shape's `field = <integer literal>` keeps the literal as the Elixir integer Postgres left it
  # after the implicit int->float/numeric cast, but the record side parses every value for the
  # column through its text input function (`float8`/`numeric`/`float4` -> a float), so the raw
  # integer key never matches the parsed float key in the :set table and no live change routes to
  # the shape. Coerce the const to a float — this same `index_value/2` also runs over the parsed
  # record value, so both sides land on one key. `float4` is coerced to float, not narrowed to
  # 32-bit: Postgres resolves `real = <int/numeric literal>` in double precision, so narrowing both
  # sides to float32 would collapse `16777217` onto `16777216` and admit a row Postgres excludes.
  def index_value(type, value) when type in [:float8, :numeric, :float4] and is_number(value),
    do: value * 1.0

  # Postgres compares `time`/`timestamp`/`timestamptz` by instant (Time/NaiveDateTime/DateTime
  # .compare/2), but the Elixir struct carries `microsecond` as `{value, precision}`, and the
  # precision digit tracks the text spelling of the value: `12:00:00` parses to `{0, 0}` while
  # `12:00:00.000000` parses to `{0, 6}`. Two spellings Postgres calls equal then take distinct :set
  # keys and no live change routes. The value component already holds full microseconds regardless of
  # the precision digit, so fixing the precision to Postgres's microsecond storage makes term
  # equality agree with instant equality.
  def index_value(_type, %Time{microsecond: {value, _}} = time),
    do: %{time | microsecond: {value, 6}}

  def index_value(_type, %NaiveDateTime{microsecond: {value, _}} = timestamp),
    do: %{timestamp | microsecond: {value, 6}}

  def index_value(_type, %DateTime{microsecond: {value, _}} = timestamp),
    do: %{timestamp | microsecond: {value, 6}}

  # Two arrays are equal when their elements are equal in order, a NULL element matching a NULL
  # one, so an array is keyed by its elements' keys, with its nesting and NULLs kept.
  def index_value({:array, element_type} = type, values) when is_list(values) do
    Enum.map(values, fn
      nested when is_list(nested) -> index_value(type, nested)
      value -> index_value(element_type, value)
    end)
  end

  # Postgres's `=` compares lower bounds too, so an array that has some other than 1 is keyed apart
  # from every array that has none.
  def index_value({:array, _} = type, %PgInterop.Array.Bounded{} = array),
    do: {:bounded, array.lower_bounds, index_value(type, array.elements)}

  def index_value(_type, value), do: value
end
