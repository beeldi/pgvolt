defmodule Electric.Shapes.Filter.RefScan do
  @moduledoc """
  Reads the columns a where clause names off its text, without parsing it.

  `Filter` keeps a shape's evaluated clause only when the clause cannot be rebuilt from its text,
  which is when the text names a column that `used_refs` no longer holds. Settling that with a
  parse costs one parse per shape on every restore. Nearly every clause is plain enough to be
  settled by reading its text instead: columns compared to literals, joined by AND, OR, NOT, IN
  and IS NULL.

  The scan is one-sided. It answers `:known` only for a text made of string literals, whole
  numbers, operator characters, the reserved words below, type names behind a `::`, and
  identifiers that name a key of `used_refs`. Postgres builds a column reference out of an
  identifier and of nothing else, so such a text names no other column. Whatever the scan does
  not recognise - a comment, an escape, a dotted name, a word it does not list - is `:unknown`,
  and the caller parses.
  """

  # Reserved in Postgres: none can name a column unless quoted.
  @reserved ~w(and or not in is null true false array)

  # Postgres truncates a longer identifier, so the name it keeps is not the one in the text.
  @max_identifier_bytes 63

  @spec scan(String.t(), %{optional([String.t()]) => term()}) :: :known | :unknown
  def scan(query, used_refs) when is_binary(query) and is_map(used_refs),
    do: text(query, used_refs, false)

  # `type?` says the next identifier follows a `::` and names a type.
  defp text(<<>>, _refs, _type?), do: :known

  defp text(<<c, rest::binary>>, refs, type?) when c in ~c" \t\n\r",
    do: text(rest, refs, type?)

  defp text(<<"::", rest::binary>>, refs, _type?), do: text(rest, refs, true)
  defp text(<<?", rest::binary>>, refs, type?), do: quoted(rest, refs, type?)
  defp text(<<?', rest::binary>>, refs, _type?), do: string(rest, refs)
  defp text(<<"--", _::binary>>, _refs, _type?), do: :unknown
  defp text(<<"/*", _::binary>>, _refs, _type?), do: :unknown

  # `U&'..'` and `U&".."` carry Unicode escapes
  defp text(<<?&, c, _::binary>>, _refs, _type?) when c in ~c"'\"", do: :unknown

  defp text(<<c, rest::binary>>, refs, _type?) when c in ~c"()[],=<>!&|@~+-*/%^",
    do: text(rest, refs, false)

  defp text(<<c, _::binary>> = text, refs, _type?) when c in ?0..?9, do: number(text, refs)

  defp text(<<c, _::binary>> = text, refs, type?) when c in ?a..?z or c in ?A..?Z or c == ?_,
    do: word(text, text, 0, refs, type?)

  defp text(_other, _refs, _type?), do: :unknown

  defp quoted(rest, refs, type?) do
    case :binary.split(rest, "\"") do
      # a doubled quote is an escaped one: the name does not end here
      [_name, <<?", _::binary>>] ->
        :unknown

      [name, rest] when byte_size(name) >= 1 and byte_size(name) <= @max_identifier_bytes ->
        if type? or is_map_key(refs, [name]), do: text(rest, refs, false), else: :unknown

      _ ->
        :unknown
    end
  end

  # What a backslash means depends on the kind of string, which is the parser's to say.
  defp string(<<"''", rest::binary>>, refs), do: string(rest, refs)
  defp string(<<?', rest::binary>>, refs), do: text(rest, refs, false)
  defp string(<<?\\, _::binary>>, _refs), do: :unknown
  defp string(<<_, rest::binary>>, refs), do: string(rest, refs)
  defp string(<<>>, _refs), do: :unknown

  # Whole numbers only: a decimal point, an exponent or a radix prefix goes to the parser.
  defp number(<<c, rest::binary>>, refs) when c in ?0..?9, do: number(rest, refs)

  defp number(<<c, _::binary>>, _refs)
       when c in ?a..?z or c in ?A..?Z or c in ~c"_.$" or c >= 0x80,
       do: :unknown

  defp number(rest, refs), do: text(rest, refs, false)

  defp word(<<c, rest::binary>>, text, len, refs, type?)
       when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_,
       do: word(rest, text, len + 1, refs, type?)

  # `$` and the bytes past ASCII carry an identifier on, and a quote straight after a word makes
  # the word a prefix of what follows (`E'..'`, `B'..'`) instead of a name
  defp word(<<c, _::binary>>, _text, _len, _refs, _type?) when c in ~c"$'\"" or c >= 0x80,
    do: :unknown

  defp word(rest, text, len, refs, type?) do
    if type? or named?(binary_part(text, 0, len), refs),
      do: text(rest, refs, false),
      else: :unknown
  end

  # Postgres folds an unquoted identifier to lower case.
  defp named?(word, refs) when byte_size(word) <= @max_identifier_bytes do
    word = String.downcase(word, :ascii)
    word in @reserved or is_map_key(refs, [word])
  end

  defp named?(_word, _refs), do: false
end
