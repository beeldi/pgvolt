if Code.ensure_loaded?(Ecto) do
  defmodule Electric.Client.EctoAdapter.ArrayDecoder do
    alias Electric.Client.EctoAdapter

    # PostgreSQL refuses an array of more than 6 dimensions (MAXDIM), so a value nested
    # deeper did not come from PostgreSQL. Refuse it rather than recurse once per brace.
    @max_dimensions 6

    def decode!("{}", _type), do: []

    def decode!(encoded_array, type) when is_binary(encoded_array) do
      {"", [result]} =
        decode_array(encoded_array, [], {EctoAdapter.cast_to(type), type, encoded_array, 0})

      result
    end

    ##############################

    defp decode_array("", acc, _state) do
      {"", :lists.reverse(acc)}
    end

    defp decode_array(<<"{}", rest::bitstring>>, acc, state) do
      decode_array(rest, [[] | acc], state)
    end

    defp decode_array(<<"{", rest::bitstring>>, acc, state) do
      {rest, array} = decode_array(rest, [], enter_dimension(state))
      decode_array(rest, [array | acc], state)
    end

    defp decode_array(<<"}", rest::bitstring>>, acc, _state) do
      {rest, :lists.reverse(acc)}
    end

    defp decode_array(<<?,, rest::bitstring>>, acc, state) do
      decode_array(rest, acc, state)
    end

    defp decode_array(<<?", rest::bitstring>>, acc, state) do
      {rest, elem} = decode_quoted_elem(rest, [], state)
      decode_array(rest, [elem | acc], state)
    end

    defp decode_array(rest, acc, state) do
      {rest, elem} = decode_elem(rest, [], state)
      decode_array(rest, [elem | acc], state)
    end

    ##############################

    defp decode_elem(<<",", _::bitstring>> = rest, acc, state),
      do: {rest, cast(acc, state)}

    defp decode_elem(<<"}", _::bitstring>> = rest, acc, state),
      do: {rest, cast(acc, state)}

    defp decode_elem(<<c::utf8, rest::bitstring>>, acc, state),
      do: decode_elem(rest, [acc | <<c::utf8>>], state)

    defp decode_elem("", _acc, {_cast_fun, type, source, _depth}) do
      raise Ecto.CastError, type: {:array, type}, value: source
    end

    ##############################

    defp decode_quoted_elem(<<?", rest::bitstring>>, acc, state),
      do: {rest, cast_quoted(acc, state)}

    defp decode_quoted_elem(<<?\\, c::utf8, rest::bitstring>>, acc, state),
      do: decode_quoted_elem(rest, [acc | <<c::utf8>>], state)

    defp decode_quoted_elem(<<c::utf8, rest::bitstring>>, acc, state),
      do: decode_quoted_elem(rest, [acc | <<c::utf8>>], state)

    defp decode_quoted_elem("", _acc, {_cast_fun, type, source, _depth}) do
      raise Ecto.CastError, type: {:array, type}, value: source
    end

    ##############################

    defp enter_dimension({_cast_fun, type, source, @max_dimensions}) do
      raise Ecto.CastError,
        type: {:array, type},
        value: source,
        message:
          "cannot cast an array nested deeper than the #{@max_dimensions} dimensions PostgreSQL allows to #{inspect({:array, type})}"
    end

    defp enter_dimension({cast_fun, type, source, depth}),
      do: {cast_fun, type, source, depth + 1}

    defp cast(iodata, {cast_fun, _type, _source, _depth}) do
      iodata
      |> IO.iodata_to_binary()
      |> case do
        "NULL" -> nil
        value -> cast_fun.(value)
      end
    end

    defp cast_quoted(iodata, {cast_fun, _type, _source, _depth}) do
      iodata
      |> IO.iodata_to_binary()
      |> cast_fun.()
    end
  end
end
