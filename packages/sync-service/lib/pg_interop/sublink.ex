defmodule PgInterop.Sublink do
  @moduledoc """
  Membership test for `value IN (SELECT ...)` sublinks.

  A true match wins over any unknown comparison. Without a match, `IN` is
  unknown if any comparison is unknown. For composite keys, a pair of
  non-NULL fields that differ is enough to make the row comparison false.
  """

  defstruct [:keys, :null_keys, :originals]

  @type t :: %__MODULE__{keys: MapSet.t(), null_keys: MapSet.t() | nil, originals: map() | nil}

  def composite_view(%MapSet{} = keys, originals \\ nil) do
    %__MODULE__{
      keys: keys,
      null_keys: keys |> MapSet.filter(&null_key?/1) |> nonempty(),
      originals: originals
    }
  end

  def keys(%__MODULE__{keys: keys}), do: keys
  def keys(%MapSet{} = keys), do: keys

  def put(%__MODULE__{keys: keys, null_keys: null_keys} = view, value, original \\ nil) do
    null_keys =
      if null_key?(value), do: MapSet.put(null_keys || MapSet.new(), value), else: null_keys

    originals = if view.originals, do: Map.put(view.originals, value, original)
    %{view | keys: MapSet.put(keys, value), null_keys: null_keys, originals: originals}
  end

  def delete(%__MODULE__{keys: keys, null_keys: null_keys} = view, value) do
    null_keys =
      if null_keys && null_key?(value),
        do: null_keys |> MapSet.delete(value) |> nonempty(),
        else: null_keys

    originals = if view.originals, do: Map.delete(view.originals, value)
    %{view | keys: MapSet.delete(keys, value), null_keys: null_keys, originals: originals}
  end

  @spec member?(term(), [term()] | MapSet.t() | t()) :: boolean() | nil
  def member?({a, b} = value, %__MODULE__{keys: keys, null_keys: nil})
      when not is_nil(a) and not is_nil(b),
      do: MapSet.member?(keys, value)

  def member?(value, %__MODULE__{keys: keys, null_keys: null_keys}) do
    cond do
      null_key?(value) -> unknown_or_false(value, keys)
      MapSet.member?(keys, value) -> true
      is_nil(null_keys) -> false
      true -> unknown_or_false(value, null_keys)
    end
  end

  def member?(value, values) when is_tuple(value) do
    if not null_key?(value) and Enum.member?(values, value),
      do: true,
      else: unknown_or_false(value, values)
  end

  def member?(value, list) when is_list(list) do
    cond do
      not is_nil(value) and Enum.member?(list, value) -> true
      list == [] -> false
      is_nil(value) -> nil
      Enum.any?(list, &is_nil/1) -> nil
      true -> false
    end
  end

  def member?(value, %MapSet{} = set) do
    cond do
      not is_nil(value) and MapSet.member?(set, value) -> true
      MapSet.size(set) == 0 -> false
      is_nil(value) -> nil
      MapSet.member?(set, nil) -> nil
      true -> false
    end
  end

  def null_key?(value) when is_tuple(value), do: value |> Tuple.to_list() |> Enum.any?(&is_nil/1)
  def null_key?(value), do: is_nil(value)

  defp nonempty(set), do: if(MapSet.size(set) == 0, do: nil, else: set)

  defp unknown_or_false(value, values) do
    if Enum.any?(values, &row_comparison_unknown?(value, &1)), do: nil, else: false
  end

  defp row_comparison_unknown?(left, right) do
    fields = Enum.zip(Tuple.to_list(left), Tuple.to_list(right))

    Enum.any?(fields, fn {a, b} -> is_nil(a) or is_nil(b) end) and
      Enum.all?(fields, fn {a, b} -> is_nil(a) or is_nil(b) or a === b end)
  end
end
