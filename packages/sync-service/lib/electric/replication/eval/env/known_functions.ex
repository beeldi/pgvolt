defmodule Electric.Replication.Eval.Env.KnownFunctions do
  use Electric.Replication.Eval.KnownDefinition

  alias PgInterop.Interval
  alias Electric.Replication.PostgresInterop.Casting
  alias Electric.Replication.Eval.Env.BasicTypes
  alias Electric.Shapes.Filter.Indexes.IndexValue
  alias PgInterop.Array, as: PgArray

  def coalesce(values), do: Enum.find(values, &(not is_nil(&1)))

  def greatest(values), do: pick_extreme(values, :greatest)
  def least(values), do: pick_extreme(values, :least)

  defp pick_extreme(values, direction) do
    case Enum.reject(values, &is_nil/1) do
      [] ->
        nil

      [head | tail] ->
        Enum.reduce(tail, head, fn value, acc ->
          case compare(value, acc) do
            :gt when direction == :greatest -> value
            :lt when direction == :least -> value
            _ -> acc
          end
        end)
    end
  end

  defp compare(%Date{} = left, %Date{} = right), do: Date.compare(left, right)
  defp compare(%Time{} = left, %Time{} = right), do: Time.compare(left, right)

  defp compare(%NaiveDateTime{} = left, %NaiveDateTime{} = right),
    do: NaiveDateTime.compare(left, right)

  defp compare(%DateTime{} = left, %DateTime{} = right), do: DateTime.compare(left, right)
  defp compare(%Interval{} = left, %Interval{} = right), do: Interval.compare(left, right)

  defp compare(left, right)
       when is_struct(left, PgArray.Bounded) or is_struct(right, PgArray.Bounded),
       do: PgArray.compare(left, right, &compare/2)

  defp compare(left, right) do
    cond do
      left > right -> :gt
      left < right -> :lt
      true -> :eq
    end
  end

  ## Conditional expressions

  defpostgres("coalesce(VARIADIC anycompatible) -> anycompatible",
    strict?: false,
    delegate: &__MODULE__.coalesce/1
  )

  defpostgres("greatest(VARIADIC anycompatible) -> anycompatible",
    strict?: false,
    delegate: &__MODULE__.greatest/1
  )

  defpostgres("least(VARIADIC anycompatible) -> anycompatible",
    strict?: false,
    delegate: &__MODULE__.least/1
  )

  ## "input" functions

  defpostgres("int2(text) -> int2", delegate: &Casting.parse_int2/1)
  defpostgres("int4(text) -> int4", delegate: &Casting.parse_int4/1)
  defpostgres("int8(text) -> int8", delegate: &Casting.parse_int8/1)
  defpostgres("float4(text) -> float4", delegate: &Casting.parse_float4/1)
  defpostgres("float8(text) -> float8", delegate: &Casting.parse_float8/1)
  defpostgres("numeric(text) -> numeric", delegate: &Casting.parse_numeric/1)
  defpostgres("bool(text) -> bool", delegate: &Casting.parse_bool/1)
  defpostgres("uuid(text) -> uuid", delegate: &Casting.parse_uuid/1)
  defpostgres("text(text) -> text", delegate: &BasicTypes.noop/1)
  defpostgres("date(text) -> date", delegate: &Casting.parse_date/1)
  defpostgres("time(text) -> time", delegate: &Casting.parse_time/1)
  defpostgres("timestamp(text) -> timestamp", delegate: &Casting.parse_timestamp/1)
  defpostgres("timestamptz(text) -> timestamp", delegate: &Casting.parse_timestamptz/1)
  defpostgres("interval(text) -> interval", delegate: &Interval.parse!/1)
  # json/jsonb values stay in their raw text representation: no known function or
  # operator evaluates their contents, and decoding would conflate a JSON null with
  # SQL NULL in null tests ('null'::jsonb IS NULL is false in Postgres).
  defpostgres("json(text) -> json", delegate: &BasicTypes.noop/1)
  defpostgres("jsonb(text) -> jsonb", delegate: &BasicTypes.noop/1)
  # bytea likewise stays raw: Postgres emits its canonical hex form ("\x...") in
  # both the WAL and snapshots, and no known function or operator inspects it.
  defpostgres("bytea(text) -> bytea", delegate: &BasicTypes.noop/1)

  ## "output" functions

  defpostgres("int2out(int2) -> text", delegate: &Integer.to_string/1)
  defpostgres("int4out(int4) -> text", delegate: &Integer.to_string/1)
  defpostgres("int8out(int8) -> text", delegate: &Integer.to_string/1)
  defpostgres("float4out(float4) -> text", delegate: &Float.to_string/1)
  defpostgres("float8out(float8) -> text", delegate: &Float.to_string/1)
  defpostgres("numericout(numeric) -> text", delegate: &Float.to_string/1)
  defpostgres("dateout(date) -> text", delegate: &Date.to_iso8601/1)
  defpostgres("timeout(time) -> text", delegate: &Time.to_iso8601/1)
  defpostgres("timestampout(timestamp) -> text", delegate: &NaiveDateTime.to_iso8601/1)
  defpostgres("intervalout(interval) -> text", delegate: &PgInterop.Interval.format/1)

  defpostgres "boolout(bool) -> text" do
    def bool_out(true), do: "t"
    def bool_out(false), do: "f"
  end

  defpostgres("uuidout(uuid) -> text", delegate: &Casting.uuid_to_string/1)

  ## Comparison functions

  defcompare("*numeric_type*", using: Kernel)
  defcompare("text", using: Kernel)
  defcompare("uuid", using: Kernel)
  defcompare("date", using: &Date.compare/2)
  defcompare("time", using: &Time.compare/2)
  defcompare("timestamp", using: &NaiveDateTime.compare/2)
  defcompare("timestamptz", using: &DateTime.compare/2)
  defcompare("interval", using: &PgInterop.Interval.compare/2)

  defpostgres("bool = bool -> bool", delegate: &Kernel.==/2)
  defpostgres("bool <> bool -> bool", delegate: &Kernel.!=/2)

  ## Numeric functions

  defpostgres("+ *numeric_type* -> *numeric_type*", delegate: &Kernel.+/1)
  defpostgres("- *numeric_type* -> *numeric_type*", delegate: &Kernel.-/1)
  defpostgres("*numeric_type* + *numeric_type* -> *numeric_type*", delegate: &Kernel.+/2)
  defpostgres("*numeric_type* - *numeric_type* -> *numeric_type*", delegate: &Kernel.-/2)
  defpostgres("*numeric_type* * *numeric_type* -> *numeric_type*", delegate: &Kernel.*/2)
  defpostgres("*integral_type* / *integral_type* -> *integral_type*", delegate: &Kernel.div/2)
  defpostgres("float8 / float8 -> float8", delegate: &Kernel.//2)
  defpostgres("numeric ^ numeric -> numeric", delegate: &Kernel.**/2)
  defpostgres("float8 ^ float8 -> float8", delegate: &Kernel.**/2)
  defpostgres("|/ float8 -> float8", delegate: &:math.sqrt/1)
  defpostgres("@ *numeric_type* -> *numeric_type*", delegate: &:erlang.abs/1)
  defpostgres("*integral_type* & *integral_type* -> *integral_type*", delegate: &Bitwise.band/2)
  defpostgres("*integral_type* | *integral_type* -> *integral_type*", delegate: &Bitwise.bor/2)
  defpostgres("*integral_type* # *integral_type* -> *integral_type*", delegate: &Bitwise.bxor/2)
  defpostgres("~ *integral_type* -> *integral_type*", delegate: &Bitwise.bnot/1)

  ## String functions
  defpostgres "text || text -> text" do
    # Can't do `delegate: &Kernel.<>/2`, because it's a macro that gets converted to local function call
    def text_concat(t1, t2) when is_binary(t1) and is_binary(t2) do
      t1 <> t2
    end
  end

  def concat_text(parts) when is_list(parts) do
    parts
    |> Enum.reject(&is_nil/1)
    |> Enum.join()
  end

  defpostgres("concat(VARIADIC text) -> text",
    strict?: false,
    delegate: &__MODULE__.concat_text/1
  )

  # Postgres with a libc provider folds one character at a time through `towlower`/`towupper`, so a
  # character whose full-case mapping expands -- `ß` to `SS`, `ﬁ` to `FI` -- is left unchanged.
  # Elixir's `String.upcase`/`downcase` apply the full Unicode mapping, which turned
  # `upper(col) = 'STRASSE'` into a match on a row Postgres does not return.
  def simple_upcase(text), do: map_codepoints(text, &String.upcase/1)
  def simple_downcase(text), do: map_codepoints(text, &String.downcase/1)

  defp map_codepoints(text, fold) do
    text
    |> String.to_charlist()
    |> Enum.map_join(fn codepoint ->
      character = <<codepoint::utf8>>

      case fold.(character) do
        <<_::utf8>> = folded -> folded
        _expanded -> character
      end
    end)
  end

  defpostgres("lower(text) -> text", delegate: &__MODULE__.simple_downcase/1)
  defpostgres("upper(text) -> text", delegate: &__MODULE__.simple_upcase/1)
  defpostgres("text ~~ text -> bool", delegate: &Casting.like?/2)
  defpostgres("text ~~* text -> bool", delegate: &Casting.ilike?/2)

  defpostgres "text !~~ text -> bool" do
    def not_like?(text1, text2), do: not Casting.like?(text1, text2)
  end

  defpostgres "text !~~* text -> bool" do
    def not_ilike?(text1, text2), do: not Casting.ilike?(text1, text2)
  end

  defpostgres("like(text, text) -> bool", delegate: &Casting.like?/2)
  defpostgres("ilike(text, text) -> bool", delegate: &Casting.ilike?/2)

  ## Date functions
  defpostgres("date + int8 -> date", commutative?: true, delegate: &Date.add/2)
  defpostgres("date - date -> int8", delegate: &Date.diff/2)

  defpostgres "date - int8 -> date" do
    def date_subtract(date, int), do: Date.add(date, -int)
  end

  defpostgres("date + time -> timestamp", commutative?: true, delegate: &NaiveDateTime.new!/2)
  defpostgres("interval + interval -> interval", delegate: &Interval.add/2)

  defpostgres("date + interval -> timestamp",
    commutative?: true,
    delegate: &Interval.add_to_date/2
  )

  defpostgres("timestamp + interval -> timestamp",
    commutative?: true,
    delegate: &Interval.add_to_date/2
  )

  defpostgres("timestamptz + interval -> timestamptz",
    commutative?: true,
    delegate: &Interval.add_to_date/2
  )

  defpostgres("time + interval -> time", commutative?: true, delegate: &Interval.add_to_time/2)
  defpostgres("date - interval -> timestamp", delegate: &Interval.subtract_from_date/2)
  defpostgres("timestamp - interval -> timestamp", delegate: &Interval.subtract_from_date/2)
  defpostgres("timestamptz - interval -> timestamptz", delegate: &Interval.subtract_from_date/2)
  defpostgres("interval - interval -> interval", delegate: &Interval.subtract/2)
  defpostgres("timestamp - timestamp -> interval", delegate: &Interval.datetime_diff/2)
  defpostgres("timestamptz - timestamptz -> interval", delegate: &Interval.datetime_diff/2)
  defpostgres("interval * float8 -> interval", commutative?: true, delegate: &Interval.scale/2)

  defpostgres("justify_days(interval) -> interval", delegate: &Interval.justify_days/1)
  defpostgres("justify_hours(interval) -> interval", delegate: &Interval.justify_hours/1)
  defpostgres("justify_interval(interval) -> interval", delegate: &Interval.justify_interval/1)

  defpostgres "timezone(text, timestamp) -> timestamptz" do
    def timestamptz_from_naive(tz, datetime), do: DateTime.from_naive!(datetime, tz)
  end

  defpostgres "timezone(text, timestamptz) -> timestamp" do
    def naive_from_timestamptz(tz, datetime),
      do: datetime |> DateTime.shift_zone!(tz) |> DateTime.to_naive()
  end

  ## Enum operators
  defpostgres("anyenum = anyenum -> bool", delegate: &Kernel.==/2)
  defpostgres("anyenum <> anyenum -> bool", delegate: &Kernel.!=/2)

  ## Array functions
  defpostgres("anyarray = anyarray -> bool", delegate: &Kernel.==/2)
  defpostgres("anyarray <> anyarray -> bool", delegate: &Kernel.!=/2)

  defpostgres("anycompatiblearray || anycompatiblearray -> anycompatiblearray",
    delegate: &PgInterop.Array.concat_arrays/2,
    strict?: false
  )

  defpostgres("array_cat(anycompatiblearray, anycompatiblearray) -> anycompatiblearray",
    delegate: &PgInterop.Array.concat_arrays/2,
    strict?: false
  )

  defpostgres("anycompatible || anycompatiblearray -> anycompatiblearray",
    delegate: &PgInterop.Array.array_prepend_concat/2,
    strict?: false
  )

  defpostgres("array_prepend(anycompatible, anycompatiblearray) -> anycompatiblearray",
    delegate: &PgInterop.Array.array_prepend/2,
    strict?: false
  )

  defpostgres("anycompatiblearray || anycompatible -> anycompatiblearray",
    delegate: &PgInterop.Array.array_append_concat/2,
    strict?: false
  )

  defpostgres("array_append(anycompatiblearray, anycompatible) -> anycompatiblearray",
    delegate: &PgInterop.Array.array_append/2,
    strict?: false
  )

  defpostgres("array_ndims(anyarray) -> int4", delegate: &PgInterop.Array.get_array_dim/1)

  # `@>`, `<@` and `&&` read an array's elements and not its lower bounds.
  defpostgres "anyarray @> anyarray -> bool" do
    def left_array_contains_right?(left, right) do
      MapSet.subset?(
        List.flatten(PgArray.elements(right)) |> MapSet.new(),
        List.flatten(PgArray.elements(left)) |> Enum.reject(&is_nil/1) |> MapSet.new()
      )
    end
  end

  defpostgres "anyarray <@ anyarray -> bool" do
    def right_array_contains_left?(left, right) do
      MapSet.subset?(
        List.flatten(PgArray.elements(left)) |> MapSet.new(),
        List.flatten(PgArray.elements(right)) |> Enum.reject(&is_nil/1) |> MapSet.new()
      )
    end
  end

  defpostgres "anyarray && anyarray -> bool" do
    def arrays_overlap?(left, right)
        when is_struct(left, PgArray.Bounded) or is_struct(right, PgArray.Bounded),
        do: arrays_overlap?(PgArray.elements(left), PgArray.elements(right))

    def arrays_overlap?(left, right) when left == [] or right == [], do: false

    def arrays_overlap?(left, right) when is_list(left) and is_list(right),
      do: arrays_overlap?(MapSet.new(List.flatten(left)), MapSet.new(List.flatten(right)))

    def arrays_overlap?(%MapSet{} = left, right) do
      Enum.any?(right, fn
        elem when is_list(elem) -> arrays_overlap?(left, elem)
        nil -> false
        elem -> MapSet.member?(left, elem)
      end)
    end
  end

  # The array comparisons above as the parser builds them, given the element type (see
  # `Parser.from_concrete/2`). The definitions compare elements as Elixir terms. These first key
  # both arrays by `IndexValue.index_value/2`, under which two elements are one term exactly when
  # their type calls them equal, and which is the key the routing indexes look a row up by: the
  # evaluator and the indexes then make the same decision about a row. The key of an array keeps
  # its lower bounds, which `=` and `<>` compare and the others do not.
  def array_equal?(left, right, type), do: array_key(left, type) === array_key(right, type)
  def array_not_equal?(left, right, type), do: not array_equal?(left, right, type)

  def left_array_contains_right?(left, right, type),
    do: left_array_contains_right?(elements_key(left, type), elements_key(right, type))

  def right_array_contains_left?(left, right, type),
    do: right_array_contains_left?(elements_key(left, type), elements_key(right, type))

  def arrays_overlap?(left, right, type),
    do: arrays_overlap?(elements_key(left, type), elements_key(right, type))

  defp array_key(array, type), do: IndexValue.index_value({:array, type}, array)
  defp elements_key(array, type), do: array |> PgArray.elements() |> array_key(type)
end
