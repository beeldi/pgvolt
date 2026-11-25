defmodule Electric.Replication.PostgresInterop.Casting do
  @int2_range -32768..32767
  @int4_range -2_147_483_648..2_147_483_647
  @int8_range -9_223_372_036_854_775_808..9_223_372_036_854_775_807

  defguard is_pg_int2(x) when is_integer(x) and x in @int2_range
  defguard is_pg_int4(x) when is_integer(x) and x in @int4_range
  defguard is_pg_int8(x) when is_integer(x) and x in @int8_range

  def values_distinct?(nil, nil, _), do: false
  def values_distinct?(v1, v2, _) when is_nil(v1) or is_nil(v2), do: true
  def values_distinct?(_, _, plain_comparison), do: plain_comparison

  def values_not_distinct?(v1, v2, plain_comparison),
    do: not values_distinct?(v1, v2, plain_comparison)

  def parse_int2(input) do
    case input |> String.trim() |> String.to_integer() do
      x when is_pg_int2(x) -> x
    end
  end

  def parse_int4(input) do
    case input |> String.trim() |> String.to_integer() do
      x when is_pg_int4(x) -> x
    end
  end

  def parse_int8(input) do
    case input |> String.trim() |> String.to_integer() do
      x when is_pg_int8(x) -> x
    end
  end

  def parse_float8(input) do
    case input |> String.trim() |> Float.parse() do
      {value, ""} -> value
    end
  end

  # Postgres has no negative numeric zero: `-0.00` reads as `0.00`. A float zero with its sign set
  # is then left only for a negative value too small for a float, which Postgres does not call zero.
  # Postgres never prints a numeric with a negative zero, so a row's value never takes the scan.
  def parse_numeric(input), do: input |> parse_float8() |> unsign_numeric_zero(input)

  def unsign_numeric_zero(value, input) when value === -0.0 do
    if nonzero_mantissa?(input), do: value, else: 0.0
  end

  def unsign_numeric_zero(value, _input), do: value

  defp nonzero_mantissa?(<<digit, _::binary>>) when digit in ?1..?9, do: true
  defp nonzero_mantissa?(<<exponent, _::binary>>) when exponent in [?e, ?E], do: false
  defp nonzero_mantissa?(<<_, rest::binary>>), do: nonzero_mantissa?(rest)
  defp nonzero_mantissa?(<<>>), do: false

  # `real` is IEEE single precision. Parsing it as a float8 leaves the double's
  # extra mantissa bits, so `0.1::float4` compared equal to `0.1::float8` where
  # Postgres calls them distinct. Round-trip through a 32-bit float to drop them.
  def parse_float4(input) do
    <<narrowed::float-32>> = <<parse_float8(input)::float-32>>
    narrowed
  end

  @true_inputs ~w|t tr tru true y ye yes on 1|
  @false_inputs ~w|f fa fal fals false n no of off 0|

  def parse_bool(input) do
    case input |> String.trim() |> String.downcase() do
      x when x in @true_inputs -> true
      x when x in @false_inputs -> false
    end
  end

  def parse_uuid(maybe_uuid) do
    {:ok, value} = Ecto.UUID.dump(maybe_uuid)
    value
  end

  def uuid_to_string(maybe_uuid) do
    {:ok, uuid} = Ecto.UUID.cast(maybe_uuid)
    uuid
  end

  def parse_date("epoch"), do: Date.from_iso8601!("1970-01-01")

  def parse_date(maybe_date) do
    case Date.from_iso8601!(String.trim(maybe_date)) do
      # PG doesn't support years <= 0, so neither do we
      %Date{year: year} = date when year > 0 -> date
    end
  end

  def parse_time(maybe_time) do
    trimmed = maybe_time |> String.trim() |> String.upcase()

    case String.split_at(trimmed, -2) do
      {time, meridiem} when meridiem in ["AM", "PM"] ->
        time |> parse_clock_time() |> shift_to_24_hour(meridiem)

      _ ->
        parse_clock_time(trimmed)
    end
  end

  defp parse_clock_time(time) do
    case time |> String.trim() |> String.split(":") do
      [hour, minute] -> Time.from_iso8601!("#{pad_hour(hour)}:#{minute}:00")
      [hour | rest] -> Time.from_iso8601!(Enum.join([pad_hour(hour) | rest], ":"))
    end
  end

  defp pad_hour(hour), do: String.pad_leading(hour, 2, "0")

  defp shift_to_24_hour(%Time{hour: 0} = time, "AM"), do: time
  defp shift_to_24_hour(%Time{hour: 0} = time, "PM"), do: Time.add(time, 12, :hour)
  defp shift_to_24_hour(%Time{hour: 12} = time, "AM"), do: %{time | hour: 0}
  defp shift_to_24_hour(%Time{hour: hour} = time, "AM") when hour in 1..11, do: time
  defp shift_to_24_hour(%Time{hour: 12} = time, "PM"), do: time

  defp shift_to_24_hour(%Time{hour: hour} = time, "PM") when hour in 1..11,
    do: Time.add(time, 12, :hour)

  def parse_timestamp("epoch"), do: DateTime.from_unix!(0) |> DateTime.to_naive()

  def parse_timestamp(maybe_timestamp) do
    NaiveDateTime.from_iso8601!(maybe_timestamp)
  end

  def parse_timestamptz("epoch"), do: DateTime.from_unix!(0) |> DateTime.to_naive()

  def parse_timestamptz(maybe_timestamp) do
    {:ok, datetime, _} = DateTime.from_iso8601(maybe_timestamp)
    datetime
  end

  @doc """
  LIKE function from SQL. Case sensitive by default.

  ## Examples

      iex> like?("hello", "hell_")
      true

      iex> like?("helloo", "hell_")
      false

      iex> like?("helloo", "%o_")
      true

      iex> like?("HELLO", "hello")
      false

      iex> like?("HELLO", "hello", true)
      true
  """
  def like?(text, pattern, ignore_case? \\ false) do
    case like_source(pattern, "") do
      # A pattern ending mid-escape has nothing to make literal; answer false rather than raise so
      # neither side of the shape reads it as a match.
      :no_match ->
        false

      source ->
        ("\\A" <> source <> "\\z")
        |> Regex.compile!(like_regex_options(ignore_case?))
        |> Regex.match?(text)
    end
  end

  # `%` and `_` stand for any run and any single character, the escape makes the next character
  # literal, everything else is literal. Walking the pattern once is what strips the escape:
  # splitting on the wildcards left the backslash in the segment, so `a\%b` demanded a literal one.
  defp like_source(<<>>, acc), do: acc
  defp like_source(<<?\\>>, _acc), do: :no_match

  defp like_source(<<?\\, char::utf8, rest::binary>>, acc),
    do: like_source(rest, acc <> Regex.escape(<<char::utf8>>))

  defp like_source(<<?%, rest::binary>>, acc), do: like_source(rest, acc <> ".*")
  defp like_source(<<?_, rest::binary>>, acc), do: like_source(rest, acc <> ".")

  defp like_source(<<char::utf8, rest::binary>>, acc),
    do: like_source(rest, acc <> Regex.escape(<<char::utf8>>))

  # `dotall` because `%`/`_` span newlines in Postgres; `\A`/`\z` above rather than `^`/`$` because
  # `$` also matches before a trailing newline; `unicode` (which implies PCRE's ucp) because `_`
  # counts characters and `ILIKE` folds case beyond ASCII.
  defp like_regex_options(false), do: [:dotall, :unicode]
  defp like_regex_options(true), do: [:dotall, :unicode, :caseless]

  def ilike?(text, pattern), do: like?(text, pattern, true)

  @doc """
  The Postgres OR operator, which has some specific behaviour when
  comparing NULLs with booleans.

  ## Examples

      iex> pg_or(true, false)
      true

      iex> pg_or(false, false)
      false

      iex> pg_or(nil, true)
      true

      iex> pg_or(nil, false)
      nil

      iex> pg_or(nil, nil)
      nil
  """
  @spec pg_or(boolean() | nil, boolean() | nil) :: boolean() | nil
  def pg_or(a, b)
  def pg_or(nil, true), do: true
  def pg_or(nil, _), do: nil
  def pg_or(true, nil), do: true
  def pg_or(_, nil), do: nil
  def pg_or(a, b), do: Kernel.or(a, b)

  @doc """
  The Postgres AND operator, which has some specific behaviour when
  comparing NULLs with booleans.

  ## Examples

      iex> pg_and(true, true)
      true

      iex> pg_and(true, false)
      false

      iex> pg_and(false, false)
      false

      iex> pg_and(nil, true)
      nil

      iex> pg_and(nil, false)
      false

      iex> pg_and(nil, nil)
      nil
  """
  @spec pg_and(boolean() | nil, boolean() | nil) :: boolean() | nil
  def pg_and(a, b)
  def pg_and(nil, false), do: false
  def pg_and(nil, _), do: nil
  def pg_and(false, nil), do: false
  def pg_and(_, nil), do: nil
  def pg_and(a, b), do: Kernel.and(a, b)

  @doc """
  Fold the results of `x <op> ANY (array)` the way Postgres does: it is the n-ary `OR`,
  so one unknown comparison makes the whole thing unknown unless another is already true.
  `Enum.any?/1` treats that unknown as false and answers false, which under a `NOT`
  becomes true where Postgres stays unknown.

  Results arrive nested when the array was multidimensional, and Postgres asks the question
  of every element regardless of dimension, so they are flattened first.

  ## Examples

      iex> pg_any([false, true])
      true

      iex> pg_any([false, nil])
      nil

      iex> pg_any([true, nil])
      true

      iex> pg_any([false, false])
      false

      iex> pg_any([])
      false

      iex> pg_any([[false], [true]])
      true
  """
  @spec pg_any([boolean() | nil] | [list()]) :: boolean() | nil
  def pg_any(results),
    do: results |> PgInterop.Array.elements() |> List.flatten() |> Enum.reduce(false, &pg_or/2)

  @doc """
  Fold the results of `x <op> ALL (array)` the way Postgres does: it is the n-ary `AND`,
  so one unknown comparison makes the whole thing unknown unless another is already false.

  ## Examples

      iex> pg_all([true, true])
      true

      iex> pg_all([true, nil])
      nil

      iex> pg_all([false, nil])
      false

      iex> pg_all([true, false])
      false

      iex> pg_all([])
      true

      iex> pg_all([[true], [nil]])
      nil
  """
  @spec pg_all([boolean() | nil] | [list()]) :: boolean() | nil
  def pg_all(results),
    do: results |> PgInterop.Array.elements() |> List.flatten() |> Enum.reduce(true, &pg_and/2)
end
