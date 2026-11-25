defmodule Electric.Replication.Eval.SublinkKey do
  @moduledoc """
  Parses a column value into the term an `IN (SELECT ...)` compares it by.

  The service holds a subquery's values as Elixir terms: the materializer in the view it hands the
  outer shape, the routing index in its membership rows. Both sides of the comparison are looked up
  by term equality, so the term has to be equal exactly when Postgres calls the values equal. The
  evaluator's parse already gives that for most types, but a `numeric` parses to a float, which
  holds only about 16 digits (`9007199254740993` reads as `9007199254740992`) and has no `NaN`
  (the parse refuses it). A `numeric` key is a `Decimal` instead, normalised so that one value has
  one term: no trailing zeros (`1.50` is `1.5`), an unsigned zero, and one `NaN`, equal to itself
  as Postgres has it. A `numeric[]` key holds such a `Decimal` for each element.

  Other values Postgres calls equal parse to different terms too: a float's `-0` and `0`,
  intervals of the same span written another way (`1 day` and `24:00:00`), `char(n)` values padded
  to two lengths, and `jsonb` values whose numbers have another scale (`{"rate": 1.0}` and
  `{"rate": 1}`). `canonical/2` gives each one term, on both sides of the comparison.

  Postgres also sends values the evaluator's parse refuses: a float's `NaN` and infinities, a date
  or a timestamp at infinity, before the common era or past the year 9999, a time of `24:00:00`, an
  interval at infinity. Such a value's key is its text, as a `SublinkKey.Text`, and in an array only
  that element's: the array's other elements take `canonical/2`.
  """

  alias Electric.Replication.Eval.Env
  alias PgInterop.Interval

  defmodule Text do
    @moduledoc """
    The key of a value Postgres sends that the evaluator's parse refuses. Postgres writes each of
    those values one way, so two of them are equal exactly when their texts are, and the move-in
    query binds the text back as the column's type.
    """
    @enforce_keys [:text]
    defstruct [:text]

    @type t :: %__MODULE__{text: String.t()}
  end

  # The types whose values Postgres can send in a form the evaluator's parse refuses.
  @text_key_types [:float4, :float8, :date, :time, :timestamp, :timestamptz, :interval]

  # Postgres writes up to 131072 digits before the point and 16383 after, past Decimal's default
  # limits of 34 digits and an exponent of 6144.
  @parse_limits [max_digits: :infinity, max_exponent: :infinity]

  @doc """
  The key a subquery compares the text value `text` of type `type` by.

      iex> parse("9007199254740993.00", :numeric)
      {:ok, Decimal.new("9007199254740993")}

      iex> parse("NaN", :numeric)
      {:ok, Decimal.new("NaN")}

      iex> parse("{9007199254740993.0,NULL}", {:array, :numeric})
      {:ok, [Decimal.new("9007199254740993"), nil]}

      iex> parse("12", :int4)
      {:ok, 12}

      iex> parse("infinity", :date)
      {:ok, %Electric.Replication.Eval.SublinkKey.Text{text: "infinity"}}

      iex> parse("-0", :float8)
      {:ok, 0.0}

      iex> parse("24:00:00", :interval)
      {:ok, PgInterop.Interval.parse!("1 day")}

      iex> parse("ab   ", :bpchar)
      {:ok, "ab"}

      iex> parse(~s|{"rate": 1.50, "codes": [100.0, 0.0, "1.0"]}|, :jsonb)
      {:ok, ~s|{"rate": 1.5, "codes": [100, 0, "1.0"]}|}
  """
  @spec parse(String.t() | nil, Env.pg_type()) :: {:ok, term()} | :error
  def parse(text, :numeric) when is_binary(text), do: numeric(text)

  def parse(text, {:array, :numeric}) when is_binary(text) do
    {:ok, PgInterop.Array.parse(text, &numeric!/1)}
  rescue
    _ -> :error
  end

  def parse(text, type) do
    case Env.parse_const(Env.new(), text, type) do
      {:ok, value} -> {:ok, canonical(type, value)}
      :error when is_binary(text) -> text_key(text, type)
      result -> result
    end
  end

  defp text_key(text, type) when type in @text_key_types, do: {:ok, %Text{text: text}}
  defp text_key(text, {:array, type}) when type in @text_key_types, do: element_keys(text, type)
  defp text_key(_text, _type), do: :error

  # An array is keyed element by element: only an element the evaluator refuses is its text, so
  # the others compare as they would in an array the evaluator parses whole.
  defp element_keys(text, type) do
    env = Env.new()

    {:ok,
     PgInterop.Array.parse(text, fn element ->
       case Env.parse_const(env, element, type) do
         {:ok, value} -> canonical(type, value)
         :error -> %Text{text: element}
       end
     end)}
  rescue
    _ -> :error
  end

  @microseconds_per_day 86_400_000_000
  @microseconds_per_month 30 * @microseconds_per_day
  @int32 -2_147_483_648..2_147_483_647

  @doc """
  The one term of the values of `type` that Postgres calls equal to `value`, a value the evaluator
  parsed or computed.

  A float zero is unsigned. An interval is its span (a month of 30 days, a day of 24 hours) written
  back as months, days and microseconds of one sign, so the move-in query can still bind it. An
  interval whose months would then pass what Postgres holds stays as it is. A `char(n)` value
  loses its trailing spaces, which Postgres ignores when it compares two. A `jsonb` value loses
  the trailing fractional zeros of its numbers, and the point when no other fractional digit is
  left: Postgres writes a `jsonb` column's equal values as one text but for the scale of their
  numbers. A `jsonb` value cast from text in the where clause keeps the text it was cast from.
  Any other value is already the one term of its class.
  """
  @spec canonical(Env.pg_type(), term()) :: term()
  def canonical(type, value) when type in [:float4, :float8] and is_float(value) and value == 0,
    do: 0.0

  def canonical(:interval, %Interval{} = interval) do
    span = Interval.comparable(interval)
    months = div(span, @microseconds_per_month)
    rest = rem(span, @microseconds_per_month)

    if months in @int32 do
      %Interval{
        months: months,
        days: div(rest, @microseconds_per_day),
        microseconds: rem(rest, @microseconds_per_day)
      }
    else
      interval
    end
  end

  def canonical(:bpchar, value) when is_binary(value), do: String.trim_trailing(value, " ")

  def canonical(:jsonb, value) when is_binary(value) do
    case jsonb_scale_cuts(value, 0, []) do
      [] -> value
      cuts -> value |> keep_between(Enum.reverse(cuts), 0, []) |> IO.iodata_to_binary()
    end
  end

  def canonical({:array, element_type} = type, values) when is_list(values) do
    Enum.map(values, fn
      nested when is_list(nested) -> canonical(type, nested)
      value -> canonical(element_type, value)
    end)
  end

  def canonical({:array, _} = type, %PgInterop.Array.Bounded{elements: elements} = array),
    do: %{array | elements: canonical(type, elements)}

  def canonical(_type, value), do: value

  @doc """
  Whether a key of `type` is parsed here rather than by the evaluator, so the left side of an
  `IN (SELECT ...)` has to be parsed from its text as well.
  """
  defguard exact_type?(type) when type in [:numeric, {:array, :numeric}]

  @spec numeric(String.t()) :: {:ok, Decimal.t()} | :error
  def numeric(text) do
    case Decimal.parse(String.trim(text), @parse_limits) do
      {decimal, ""} -> {:ok, normalise(decimal)}
      _ -> :error
    end
  end

  defp numeric!(text) do
    {:ok, decimal} = numeric(text)
    decimal
  end

  # One pass over the text, with no decoding: a value of any size or number of digits costs its own
  # length. The byte ranges to drop, last first, are each number's trailing fractional zeros, and
  # its point when they were all its fractional digits. A point outside a string is in a number.
  defp jsonb_scale_cuts(<<?", rest::binary>>, at, cuts) do
    {rest, at} = skip_jsonb_string(rest, at + 1)
    jsonb_scale_cuts(rest, at, cuts)
  end

  defp jsonb_scale_cuts(<<?., rest::binary>>, at, cuts) do
    {rest, digits_end, kept_end} = jsonb_fraction(rest, at + 1, at + 1)

    cuts =
      cond do
        kept_end == digits_end -> cuts
        kept_end == at + 1 -> [{at, digits_end - at} | cuts]
        true -> [{kept_end, digits_end - kept_end} | cuts]
      end

    jsonb_scale_cuts(rest, digits_end, cuts)
  end

  defp jsonb_scale_cuts(<<_, rest::binary>>, at, cuts), do: jsonb_scale_cuts(rest, at + 1, cuts)
  defp jsonb_scale_cuts(<<>>, _at, cuts), do: cuts

  defp skip_jsonb_string(<<?\\, _, rest::binary>>, at), do: skip_jsonb_string(rest, at + 2)
  defp skip_jsonb_string(<<?", rest::binary>>, at), do: {rest, at + 1}
  defp skip_jsonb_string(<<_, rest::binary>>, at), do: skip_jsonb_string(rest, at + 1)
  defp skip_jsonb_string(<<>>, at), do: {<<>>, at}

  # `kept_end` is the offset after the last fractional digit that is not a zero.
  defp jsonb_fraction(<<?0, rest::binary>>, at, kept_end),
    do: jsonb_fraction(rest, at + 1, kept_end)

  defp jsonb_fraction(<<digit, rest::binary>>, at, _kept_end) when digit in ?1..?9,
    do: jsonb_fraction(rest, at + 1, at + 1)

  defp jsonb_fraction(rest, at, kept_end), do: {rest, at, kept_end}

  defp keep_between(text, [{from, length} | cuts], at, kept),
    do: keep_between(text, cuts, from + length, [kept, binary_part(text, at, from - at)])

  defp keep_between(text, [], at, kept), do: [kept, binary_part(text, at, byte_size(text) - at)]

  defp normalise(%Decimal{coef: :NaN}), do: %Decimal{sign: 1, coef: :NaN, exp: 0}
  defp normalise(%Decimal{coef: :inf} = infinity), do: %{infinity | exp: 0}
  defp normalise(%Decimal{coef: 0}), do: %Decimal{sign: 1, coef: 0, exp: 0}

  # Through the digits: dividing by ten once per trailing zero is quadratic in a long coefficient.
  defp normalise(%Decimal{coef: coef, exp: exp} = decimal) when rem(coef, 10) == 0 do
    digits = Integer.to_string(coef)
    significant = String.trim_trailing(digits, "0")
    trailing_zeros = byte_size(digits) - byte_size(significant)

    %{decimal | coef: String.to_integer(significant), exp: exp + trailing_zeros}
  end

  defp normalise(decimal), do: decimal
end
