defmodule PgInterop.Array do
  defmodule Bounded do
    @moduledoc """
    An array whose lower bounds are not all 1, which Postgres writes with its bounds, as in
    `[0:1]={a,b}`: a row gets one from an assignment such as `arr[0] = x`.

    `elements` is the nested list the same array parses to without its bounds, and `lower_bounds`
    holds one bound per dimension, outermost first. Postgres's `=` and `<>` compare the bounds and
    its subscripts count from them, while `@>`, `<@`, `&&` and `ANY` read only the elements.
    """
    @enforce_keys [:lower_bounds, :elements]
    defstruct [:lower_bounds, :elements]

    @type t() :: %__MODULE__{lower_bounds: [integer(), ...], elements: [term(), ...]}
  end

  defguardp is_space(c) when c in [?\s, ?\t, ?\n, ?\r, ?\v, ?\f]

  @doc ~S"""
  Parse a Postgres string-serialized array into a list of strings, unwrapping the escapes. Parses nested arrays.
  If a casting function is provided, it will be applied to each element.

  Parsing follows SOME of the same rules as the postgres parser, in particular:
  1. at most 6 nesting levels are allowed,
  2. arrays must be of uniform dimension, i.e. all sub-arrays must have the same number of elements if at the same depth,
  3. a backslash escapes the character after it, quoted or not, which is how postgres writes every
     value containing a delimiter, a quote or a backslash, and which suppresses the `NULL` keyword.

  This implementation also breaks away from the postgres parser in that some bugs are NOT reimplemented:
  - `select '{{1},{{2}}}'::text[];` yields `{{{1}},{{2}}}` in PG, we raise an error
  - `select '{{{1}},{2}}'::text[];` yields `{}` in PG, we raise an error
  - `select '{{{1}},{2},{{3}}}::text[];` yields `{{{1}},{{NULL}},{{3}}}` in PG, we raise an error

  ## Examples

      iex> ~S|{"(\"2023-06-15 11:18:05.372698+00\",)"}| |> parse()
      [~s|("2023-06-15 11:18:05.372698+00",)|]

      iex> ~S|{"(\"2023-06-15 11:18:05.372698+00\",)","(\"2023-06-15 11:18:05.372698+00\",)"}| |> parse()
      [~s|("2023-06-15 11:18:05.372698+00",)|, ~s|("2023-06-15 11:18:05.372698+00",)|]

      iex> ~S|{hello, world, null, "null"}| |> parse()
      ["hello", "world", nil, "null"]

      iex> ~S|{"C:\\Users\\me",a\,b,\N\U\L\L}| |> parse()
      ["C:\\Users\\me", "a,b", "NULL"]

      iex> ~S|{"2023-06-15 11:18:05.372698+00",2023-06-15 11:18:05.372698+00}| |> parse(fn x -> {:ok, n, _} = DateTime.from_iso8601(x); n end)
      [~U[2023-06-15 11:18:05.372698Z], ~U[2023-06-15 11:18:05.372698Z]]

      iex> ~s|{ "1" , 3  ,   "2" ,    3 3  }| |> parse()
      ["1", "3", "2", "3 3"]

      iex> ~s|{ {{1, 1}, { "2"   , 2 }} ,{{"3", 3}, {4, 4} }, {  {5, 5},{6, 6}  }}| |> parse(&String.to_integer/1)
      [[[1, 1], [2, 2]], [[3, 3], [4, 4]], [[5, 5], [6, 6]]]

      iex> ~s|{ "1" ,   "2" ,    3 3   , , 4}| |> parse()
      ** (RuntimeError) Unexpected ',' character

      iex> ~s|{ "1" , 3,  "2" ,    3 3   , }| |> parse()
      ** (RuntimeError) Unexpected '}' character

      iex> ~s|{ {1} ,{   2 }, {3  }}  }| |> parse()
      ** (RuntimeError) Invalid array syntax

      iex> ~s|{{{1} ,{   2 }, {3  }} | |> parse()
      ** (RuntimeError) Unexpected end of input

      iex> ~s|{"}| |> parse()
      ** (RuntimeError) Unexpected end of input

      iex> ~s|{{1},2,{3}}| |> parse(&String.to_integer/1)
      ** (RuntimeError) Unexpected array element

      iex> ~s|{{{{{{{1}}}}}}}| |> parse()
      ** (RuntimeError) number of dimensions (7) exceeds maximum of 6

      iex> ~s|{ {1} ,{   {2} }, {3  }}| |> parse()
      ** (RuntimeError) Inconsistent array dimensions

      iex> ~s|{ {{1}} ,{2}, {3  }}| |> parse()
      ** (RuntimeError) Inconsistent array dimensions

  Postgres writes an array whose lower bounds are not all 1 with a `[lower:upper]=` prefix, one
  bracket per dimension, on output and so in the replication stream. Such an array parses to a
  `PgInterop.Array.Bounded`, which keeps the lower bounds; one whose lower bounds are all 1 is the
  plain list Postgres would have written without them. The prefix is read only as Postgres writes
  it, with no whitespace.

      iex> ~s|[0:1]={a,b}| |> parse()
      %PgInterop.Array.Bounded{lower_bounds: [0], elements: ["a", "b"]}

      iex> ~s|[1:2][0:0]={{1},{2}}| |> parse(&String.to_integer/1)
      %PgInterop.Array.Bounded{lower_bounds: [1, 0], elements: [[1], [2]]}

      iex> ~s|[1:2]={a,b}| |> parse()
      ["a", "b"]

      iex> ~s|[1:3]={a,b}| |> parse()
      ** (RuntimeError) Specified array dimensions do not match array contents

      iex> ~s|[2:1]={a}| |> parse()
      ** (RuntimeError) Upper bound cannot be less than lower bound

      iex> ~s|[0:1] = {a,b}| |> parse()
      ** (RuntimeError) Invalid array dimension bounds
  """
  def parse(str, casting_fun \\ & &1)

  def parse("{}", _), do: []

  def parse("[" <> _ = str, casting_fun) do
    {bounds, rest} = parse_dimension_bounds(str, [])
    elements = parse(rest, casting_fun)

    if Enum.any?(bounds, fn {lower, upper} -> upper < lower end),
      do: raise("Upper bound cannot be less than lower bound")

    if dimensions(elements) != Enum.map(bounds, fn {lower, upper} -> upper - lower + 1 end),
      do: raise("Specified array dimensions do not match array contents")

    with_lower_bounds(elements, Enum.map(bounds, &elem(&1, 0)))
  end

  def parse(str, casting_fun) do
    case parse_nested_arrays(str, casting_fun, %{cur_dim: 1}) do
      {result, "", _} ->
        result

      {result, rest, _} ->
        if String.match?(rest, ~r/^\s*$/) do
          result
        else
          raise "Invalid array syntax"
        end
    end
  end

  # `[lower:upper]`, or `[upper]` for a lower bound of 1, per dimension, then `=`.
  @dimension_bound ~r/\A\[(?:(-?\d+):)?(-?\d+)\]/

  defp parse_dimension_bounds("={" <> _ = str, [_ | _] = bounds),
    do: {Enum.reverse(bounds), binary_part(str, 1, byte_size(str) - 1)}

  defp parse_dimension_bounds(str, bounds) do
    case Regex.run(@dimension_bound, str) do
      [match, lower, upper] ->
        lower = if lower == "", do: 1, else: String.to_integer(lower)
        rest = binary_part(str, byte_size(match), byte_size(str) - byte_size(match))
        parse_dimension_bounds(rest, [{lower, String.to_integer(upper)} | bounds])

      nil ->
        raise "Invalid array dimension bounds"
    end
  end

  # The length of each dimension, outermost first: the parse has checked they are uniform.
  defp dimensions([]), do: []
  defp dimensions([first | _] = list) when is_list(first), do: [length(list) | dimensions(first)]
  defp dimensions(list) when is_list(list), do: [length(list)]

  @doc """
  The elements of an array, without its lower bounds.
  """
  @spec elements(list() | Bounded.t()) :: list()
  def elements(%Bounded{elements: elements}), do: elements
  def elements(array) when is_list(array), do: array

  @doc """
  The lower bound of each dimension of an array, outermost first.

      iex> lower_bounds([[1, 2], [3, 4]])
      [1, 1]

      iex> lower_bounds(%PgInterop.Array.Bounded{lower_bounds: [0], elements: [1, 2]})
      [0]
  """
  @spec lower_bounds(list() | Bounded.t()) :: [integer()]
  def lower_bounds(%Bounded{lower_bounds: lower_bounds}), do: lower_bounds
  def lower_bounds(array) when is_list(array), do: Enum.map(dimensions(array), fn _ -> 1 end)

  @doc """
  An array of `elements` with the given lower bounds: a plain list when they are all 1 or when it
  is empty, which in Postgres has no dimensions.
  """
  @spec with_lower_bounds(list(), [integer()]) :: list() | Bounded.t()
  def with_lower_bounds([], _lower_bounds), do: []

  def with_lower_bounds(elements, lower_bounds) do
    if Enum.all?(lower_bounds, &(&1 == 1)),
      do: elements,
      else: %Bounded{lower_bounds: lower_bounds, elements: elements}
  end

  @doc """
  The `[lower:upper]=` prefix Postgres writes before an array's elements, empty when its lower
  bounds are all 1.

      iex> bounds_prefix(%PgInterop.Array.Bounded{lower_bounds: [0, 1], elements: [[1], [2]]})
      "[0:1][1:1]="

      iex> bounds_prefix([1, 2])
      ""
  """
  @spec bounds_prefix(list() | Bounded.t()) :: String.t()
  def bounds_prefix(array) when is_list(array), do: ""

  def bounds_prefix(%Bounded{lower_bounds: lower_bounds, elements: elements}) do
    lower_bounds
    |> Enum.zip(dimensions(elements))
    |> Enum.map_join(fn {lower, length} -> "[#{lower}:#{lower + length - 1}]" end)
    |> Kernel.<>("=")
  end

  @doc """
  The array `ARRAY[...]` builds from `values`. When one of them is an array with lower bounds,
  Postgres builds an array of them that keeps their bounds and starts its outer dimension at 1,
  and refuses sub-arrays whose dimensions or lower bounds differ, or a NULL or empty one beside
  them. Any other list of values is the array itself.

      iex> construct([%PgInterop.Array.Bounded{lower_bounds: [0], elements: [1, 2]}])
      %PgInterop.Array.Bounded{lower_bounds: [1, 0], elements: [[1, 2]]}

      iex> construct([[1, 2], [3, 4]])
      [[1, 2], [3, 4]]
  """
  @spec construct(list()) :: list() | Bounded.t()
  def construct(values) do
    if Enum.any?(values, &is_struct(&1, Bounded)) do
      [first | _] = values
      shape = array_shape(first)

      unless Enum.all?(values, &(array_shape(&1) == shape)),
        do: raise("multidimensional arrays must have array expressions with matching dimensions")

      %Bounded{lower_bounds: [1 | lower_bounds(first)], elements: Enum.map(values, &elements/1)}
    else
      values
    end
  end

  defp array_shape(array) when is_list(array) or is_struct(array, Bounded),
    do: {array |> elements() |> dimensions(), lower_bounds(array)}

  defp array_shape(_), do: nil

  @doc """
  Orders two arrays as Postgres does: by their elements in row-major order, compared by
  `compare_elements` with a NULL after every other value, up to the end of the shorter one, then
  by their number of elements, their number of dimensions, the length of each and their lower
  bounds.

      iex> compare(%PgInterop.Array.Bounded{lower_bounds: [0], elements: [9]}, [1, 2], fn
      ...>   left, right when left > right -> :gt
      ...>   left, right when left < right -> :lt
      ...>   _, _ -> :eq
      ...> end)
      :gt
  """
  @spec compare(list() | Bounded.t(), list() | Bounded.t(), (term(), term() -> :lt | :eq | :gt)) ::
          :lt | :eq | :gt
  def compare(left, right, compare_elements) do
    {left_elements, right_elements} = {elements(left), elements(right)}

    case compare_in_order(walk(left_elements), walk(right_elements), compare_elements) do
      :eq -> compare_terms(size_key(left, left_elements), size_key(right, right_elements))
      order -> order
    end
  end

  defp compare_in_order(left, right, compare_elements) do
    with {left_element, left} <- next_element(left),
         {right_element, right} <- next_element(right),
         :eq <- compare_element(left_element, right_element, compare_elements) do
      compare_in_order(left, right, compare_elements)
    else
      :done -> :eq
      order -> order
    end
  end

  defp compare_element(nil, nil, _compare_elements), do: :eq
  defp compare_element(nil, _right, _compare_elements), do: :gt
  defp compare_element(_left, nil, _compare_elements), do: :lt
  defp compare_element(left, right, compare_elements), do: compare_elements.(left, right)

  # A walk over a nested list's elements in row-major order: the lists still to read, innermost
  # first, the depth of the innermost one, and the depth at which the elements sit (counted
  # without `dimensions/1`, which measures each dimension's length).
  defp walk(elements), do: {[elements], 1, get_array_dim(elements) || 0}

  defp next_element({[], _depth, _dimensions}), do: :done

  defp next_element({[[] | outer], depth, dimensions}),
    do: next_element({outer, depth - 1, dimensions})

  defp next_element({[[element | rest] | outer], dimensions, dimensions}),
    do: {element, {[rest | outer], dimensions, dimensions}}

  defp next_element({[[list | rest] | outer], depth, dimensions}),
    do: next_element({[list, rest | outer], depth + 1, dimensions})

  defp size_key(array, []), do: {0, 0, [], lower_bounds(array)}

  defp size_key(array, elements) do
    dimensions = dimensions(elements)
    {Enum.product(dimensions), length(dimensions), dimensions, lower_bounds(array)}
  end

  defp compare_terms(left, right) when left > right, do: :gt
  defp compare_terms(left, right) when left < right, do: :lt
  defp compare_terms(_left, _right), do: :eq

  defp parse_nested_arrays(<<c>> <> rest, fun, dim_info) when is_space(c),
    do: parse_nested_arrays(rest, fun, dim_info)

  defp parse_nested_arrays(_, _, %{cur_dim: dim}) when dim > 6,
    do: raise("number of dimensions (#{dim}) exceeds maximum of 6")

  defp parse_nested_arrays(_, _, %{cur_dim: dim, max_dim: max_dim}) when dim > max_dim,
    do: raise("Inconsistent array dimensions")

  defp parse_nested_arrays("{" <> rest, fun, %{cur_dim: dim} = dim_info) do
    # we're in an array, need to parse all the elements at this level
    case String.trim_leading(rest) do
      "" ->
        raise "Unexpected end of input"

      "{" <> _ = rest ->
        parse_all_nested_arrays(rest, fun, [], 0, dim_info)

      _ ->
        # If we know max dimension but see a non-array element, before that, we know it's inconsistent
        if is_map_key(dim_info, :max_dim) and dim_info.max_dim > dim do
          raise "Inconsistent array dimensions"
        end

        {result, rest, dim_size} = parse_all_elements(rest, fun)

        # If we've been at this depth, validate that new array is consistent with the previous ones,
        # if not, save it
        case Map.fetch(dim_info, dim) do
          {:ok, ^dim_size} ->
            {result, rest, dim_info}

          :error ->
            {result, rest, Map.put(dim_info, dim, dim_size)}

          {:ok, _} ->
            raise "Inconsistent array dimensions"
        end
    end
  end

  defp parse_nested_arrays(_, _, _), do: raise("Unexpected array element")

  defp parse_all_nested_arrays(str, fun, acc, dim_size, %{cur_dim: dim} = dim_info) do
    {result, rest, dim_info} = parse_nested_arrays(str, fun, %{dim_info | cur_dim: dim + 1})
    dim_info = %{dim_info | cur_dim: dim}

    # First time we reach this branch is when we followed all open braces at the start
    # of the string, so we know the maximum dimension of the array
    dim_info = Map.put_new(dim_info, :max_dim, dim + 1)

    case scan_until_next_boundary(rest) do
      # If next boundary is a comma, we're at the same depth, so keep parsing
      {?,, rest} ->
        parse_all_nested_arrays(rest, fun, [result | acc], dim_size + 1, dim_info)

      # If next boundary is a closing brace, we're done with this array, so update what we can
      {?}, rest} ->
        dim_size = dim_size + 1

        # If we've been at this depth, validate that new array is consistent with the previous ones,
        # if not, save it
        case Map.fetch(dim_info, dim) do
          {:ok, ^dim_size} ->
            {Enum.reverse([result | acc]), rest, dim_info}

          :error ->
            {Enum.reverse([result | acc]), rest, Map.put(dim_info, dim, dim_size)}

          {:ok, _} ->
            raise "Inconsistent array dimensions"
        end
    end
  end

  defp parse_all_elements(str, fun, acc \\ [], dim \\ 0)
  defp parse_all_elements("", _, _, _), do: raise("Unexpected end of input")

  defp parse_all_elements(<<c>> <> rest, fun, acc, dim) when is_space(c),
    do: parse_all_elements(rest, fun, acc, dim)

  defp parse_all_elements(str, fun, acc, dim) do
    {type, {elem, rest}} = scan_next_element(str)

    case scan_until_next_boundary(rest) do
      {?,, rest} -> parse_all_elements(rest, fun, [apply_fun(type, elem, fun) | acc], dim + 1)
      {?}, rest} -> {Enum.reverse([apply_fun(type, elem, fun) | acc]), rest, dim + 1}
    end
  end

  defp scan_next_element(<<?{>> <> _), do: raise("Unexpected '{' character")
  defp scan_next_element(<<?">> <> rest), do: {:literal, scan_until_quote(rest, "")}

  defp scan_next_element(rest) do
    case scan_until_comma_or_end(rest, "", "", false) do
      {elem, rest, true} -> {:literal, {elem, rest}}
      {elem, rest, false} -> {:keyword, {elem, rest}}
    end
  end

  defp scan_until_quote("", _), do: raise("Unexpected end of input")
  defp scan_until_quote(<<?">> <> rest, acc), do: {acc, rest}
  defp scan_until_quote(<<?\\, c>> <> str, acc), do: scan_until_quote(str, acc <> <<c>>)
  defp scan_until_quote(<<c>> <> str, acc), do: scan_until_quote(str, acc <> <<c>>)

  defp scan_until_comma_or_end("", _, _, _), do: raise("Unexpected end of input")

  defp scan_until_comma_or_end(<<?\\, c>> <> str, acc, acc_whitespace, _escaped?),
    do: scan_until_comma_or_end(str, acc <> acc_whitespace <> <<c>>, "", true)

  defp scan_until_comma_or_end(<<?">> <> _, _, _, _), do: raise("Unexpected '\"' character")

  defp scan_until_comma_or_end(<<c>> <> _, "", _, false) when c in [?,, ?}],
    do: raise("Unexpected '#{[c]}' character")

  defp scan_until_comma_or_end("}" <> _ = rest, acc, _acc_whitespace, escaped?),
    do: {acc, rest, escaped?}

  defp scan_until_comma_or_end(<<?,>> <> _ = str, acc, _acc_whitespace, escaped?),
    do: {acc, str, escaped?}

  defp scan_until_comma_or_end(<<c>> <> str, "", "", escaped?) when is_space(c),
    do: scan_until_comma_or_end(str, "", "", escaped?)

  defp scan_until_comma_or_end(<<c>> <> str, acc, acc_whitespace, escaped?) when is_space(c),
    do: scan_until_comma_or_end(str, acc, acc_whitespace <> <<c>>, escaped?)

  defp scan_until_comma_or_end(<<c>> <> str, acc, acc_whitespace, escaped?),
    do: scan_until_comma_or_end(str, acc <> acc_whitespace <> <<c>>, "", escaped?)

  defp scan_until_next_boundary(""), do: raise("Unexpected end of input")
  defp scan_until_next_boundary(<<c>> <> rest) when c in [?,, ?}], do: {c, rest}

  defp scan_until_next_boundary(<<c>> <> rest) when is_space(c),
    do: scan_until_next_boundary(rest)

  defp scan_until_next_boundary(<<c>> <> _), do: raise("Unexpected '#{[c]}' character")

  defp apply_fun(:literal, elem, fun), do: fun.(elem)

  defp apply_fun(:keyword, elem, fun) do
    if String.downcase(elem) == "null", do: nil, else: fun.(elem)
  end

  @doc ~S"""
  Serialize a list of strings into a postgres string-serialized array into a list of strings, wrapping the contents

  ## Examples

      iex> [~s|("2023-06-15 11:18:05.372698+00",)|] |> serialize()
      ~S|{"(\"2023-06-15 11:18:05.372698+00\",)"}|

      iex> [~s|("2023-06-15 11:18:05.372698+00",)|, ~s|("2023-06-15 11:18:05.372698+00",)|] |> serialize()
      ~S|{"(\"2023-06-15 11:18:05.372698+00\",)","(\"2023-06-15 11:18:05.372698+00\",)"}|

      iex> str = ~S|{"(\"2023-06-15 11:18:05.372698+00\",)","(\"2023-06-15 11:18:05.372698+00\",)"}|
      iex> str |> parse() |> serialize()
      str
  """
  def serialize(array, quote_char \\ ?") when is_list(array) do
    array
    |> Enum.map_join(",", fn
      nil ->
        "null"

      val when is_binary(val) ->
        val
        |> String.replace("\\", "\\\\")
        |> String.replace(~S|"|, ~S|\"|)
        |> enclose(<<quote_char>>)
    end)
    |> enclose("{", "}")
  end

  defp enclose(str, left, right \\ nil) do
    left <> str <> (right || left)
  end

  @doc """
  Access a slice or index of a postgres array.

  ## Examples

      iex> ~S|{1,2,3,4,5}| |> parse(&String.to_integer/1) |> slice_access([{:slice, nil, 3}])
      [1, 2, 3]

      iex> ~S|{1,2,3,4,5}| |> parse(&String.to_integer/1) |> slice_access([{:slice, 3, nil}])
      [3, 4, 5]

      iex> ~S|{1,2,3,4,5}| |> parse(&String.to_integer/1) |> slice_access([{:slice, 3, 4}])
      [3, 4]

      iex> ~S|{{1,2},{3,4}}| |> parse(&String.to_integer/1) |> slice_access([{:slice, nil, nil}, {:index, 2}])
      [[1, 2], [3, 4]]

      iex> ~S|{{1,2},{3,4}}| |> parse(&String.to_integer/1) |> slice_access([{:slice, nil, nil}, {:slice, 2, 2}])
      [[2], [4]]

      iex> ~S|{{1,2},{3,4}}| |> parse(&String.to_integer/1) |> slice_access([{:slice, nil, nil}, {:slice, -1, 1}])
      [[1], [3]]

      iex> ~S|{{1,2},{3,4}}| |> parse(&String.to_integer/1) |> slice_access([{:slice, nil, nil}, {:slice, 1, -1}])
      []

      iex> ~S|[0:3]={1,2,3,4}| |> parse(&String.to_integer/1) |> slice_access([{:slice, 1, 2}])
      [2, 3]
  """
  @spec slice_access(
          list() | Bounded.t(),
          list({:slice, nil | integer(), nil | integer()} | {:index, integer()})
        ) :: list()
  def slice_access(%Bounded{lower_bounds: lower_bounds, elements: elements}, instructions) do
    # Postgres counts a subscript from the dimension's lower bound and numbers the slice from 1. A
    # dimension written as one number is a slice from 1, as it is on an array without bounds.
    instructions
    |> Enum.with_index()
    |> Enum.map(fn {instruction, dimension} ->
      offset = Enum.at(lower_bounds, dimension, 1) - 1

      case instruction do
        {:slice, lower, upper} -> {:slice, lower && lower - offset, upper && upper - offset}
        {:index, upper} -> {:slice, 1 - offset, upper - offset}
      end
    end)
    |> then(&slice_access(elements, &1))
  end

  def slice_access(array, instructions) do
    do_slice_access(array, instructions)
  catch
    :out_of_bounds -> []
  end

  defp do_slice_access(elem, [_ | _]) when not is_list(elem), do: throw(:out_of_bounds)
  defp do_slice_access(array, []), do: array

  defp do_slice_access(array, [{:slice, nil, nil} | rest]),
    do: Enum.map(array, &do_slice_access(&1, rest))

  defp do_slice_access(_array, [{:slice, lower_idx, upper_idx} | _])
       when is_integer(lower_idx) and is_integer(upper_idx) and
              (lower_idx > upper_idx or upper_idx < 1),
       do: throw(:out_of_bounds)

  defp do_slice_access(array, [{:slice, lower_idx, upper_idx} | rest]),
    do:
      array
      |> Enum.slice((normalize_idx(lower_idx) || 0)..(normalize_idx(upper_idx) || -1)//1)
      |> Enum.map(&do_slice_access(&1, rest))

  defp do_slice_access(array, [{:index, idx} | rest]),
    do: do_slice_access(array, [{:slice, 1, idx} | rest])

  @doc """
  Access an index of a postgres array. If the index is out of bounds or array has more dimensions than the indices provided, returns `nil`.

  ## Examples

      iex> ~S|{1,2,3,4,5}| |> parse(&String.to_integer/1) |> index_access([{:index, 3}])
      3

      iex> ~S|{{1,2},{3,4}}| |> parse(&String.to_integer/1) |> index_access([{:index, 2}, {:index, 1}])
      3

      iex> ~S|{{1,2},{3,4}}| |> parse(&String.to_integer/1) |> index_access([{:index, 3}])
      nil

      iex> ~S|[0:1]={1,2}| |> parse(&String.to_integer/1) |> index_access([{:index, 0}])
      1
  """
  @spec index_access(list() | Bounded.t(), list({:index, integer()})) :: list()
  def index_access(%Bounded{lower_bounds: lower_bounds, elements: elements}, list_of_indices) do
    list_of_indices
    |> Enum.with_index()
    |> Enum.map(fn {{:index, idx}, dimension} ->
      {:index, idx - Enum.at(lower_bounds, dimension, 1) + 1}
    end)
    |> then(&index_access(elements, &1))
  end

  def index_access(array, list_of_indices) do
    Enum.reduce_while(list_of_indices, array, fn
      _, nil -> {:halt, nil}
      {:index, idx}, _acc when idx < 1 -> {:halt, nil}
      {:index, idx}, acc -> {:cont, Enum.at(acc, idx - 1)}
    end)
    |> case do
      # Postgres arrays are N-dimensional rather than nested, so a subscript expression must
      # supply one subscript per dimension; supplying fewer is not an inner array but NULL.
      # Anything still a list here is under-subscripted -- and would contradict this node's own
      # declared type, which is the array's base element type however many dimensions it has.
      result when is_list(result) -> nil
      result -> result
    end
  end

  defp normalize_idx(nil), do: nil
  defp normalize_idx(pg_index) when pg_index < 1, do: 0
  defp normalize_idx(pg_index), do: pg_index - 1

  def concat_arrays(arr1, []), do: arr1
  def concat_arrays([], arr2), do: arr2

  def concat_arrays(arr1, nil), do: arr1
  def concat_arrays(nil, arr2), do: arr2

  # Postgres keeps the lower bounds of the array whose dimensions the result takes, and refuses
  # two arrays whose other dimensions differ in length or lower bound.
  def concat_arrays(arr1, arr2) when is_struct(arr1, Bounded) or is_struct(arr2, Bounded) do
    {elements1, elements2} = {elements(arr1), elements(arr2)}
    shape1 = Enum.zip(dimensions(elements1), lower_bounds(arr1))
    shape2 = Enum.zip(dimensions(elements2), lower_bounds(arr2))

    case {shape1, shape2} do
      {[{_, lower} | inner], [_ | inner]} ->
        with_lower_bounds(elements1 ++ elements2, [lower | Enum.map(inner, &elem(&1, 1))])

      {inner, [_ | inner]} ->
        with_lower_bounds([elements1 | elements2], lower_bounds(arr2))

      {[_ | inner], inner} ->
        with_lower_bounds(elements1 ++ [elements2], lower_bounds(arr1))

      _ ->
        raise "cannot concatenate incompatible arrays"
    end
  end

  def concat_arrays(arr1, arr2) do
    case {get_array_dim(arr1), get_array_dim(arr2)} do
      {d1, d1} -> arr1 ++ arr2
      {d1, d2} when d2 - d1 == 1 -> [arr1 | arr2]
      {d1, d2} when d1 - d2 == 1 -> arr1 ++ [arr2]
      {d1, d2} -> raise "Incompatible array dimensions: #{d1} and #{d2}"
    end
  end

  @doc """
  Get the dimension of a postgres array.

  ## Examples

      iex> ~S|{}| |> parse() |> get_array_dim()
      nil

      iex> ~S|{1,2,3,4,5}| |> parse() |> get_array_dim()
      1

      iex> ~S|{{1,2},{3,4}}| |> parse() |> get_array_dim()
      2
  """
  @spec get_array_dim(list() | Bounded.t()) :: non_neg_integer()
  def get_array_dim(arr, dim \\ 0)
  def get_array_dim(%Bounded{elements: elements}, dim), do: get_array_dim(elements, dim)
  def get_array_dim([], _), do: nil
  def get_array_dim([hd | _], dim), do: get_array_dim(hd, dim + 1)
  def get_array_dim(_, dim), do: dim

  # Appending or prepending to a one-dimensional array keeps its lower bound.
  def array_prepend(elem, %Bounded{lower_bounds: [_], elements: elements} = array),
    do: %{array | elements: [elem | elements]}

  def array_prepend(elem, []), do: [elem]
  def array_prepend(elem, [hd | tl]) when not is_list(hd), do: [elem, hd | tl]

  def array_prepend_concat(elem, _) when is_binary(elem),
    do: raise("Cannot prepend a binary to an array with ||")

  def array_prepend_concat(nil, arr), do: arr
  def array_prepend_concat(elem, arr) when not is_binary(elem), do: array_prepend(elem, arr)

  def array_append(%Bounded{lower_bounds: [_], elements: elements} = array, elem),
    do: %{array | elements: elements ++ [elem]}

  def array_append([], elem), do: [elem]
  def array_append([hd | _] = list, elem) when not is_list(hd), do: list ++ [elem]

  def array_append_concat(_, elem) when is_binary(elem),
    do: raise("Cannot append a binary to an array with ||")

  def array_append_concat(arr, nil), do: arr
  def array_append_concat(arr, elem), do: array_append(arr, elem)
end
