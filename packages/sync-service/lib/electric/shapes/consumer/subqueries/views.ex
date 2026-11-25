defmodule Electric.Shapes.Consumer.Subqueries.Views do
  # Applies dependency move operations against the current subquery view map.

  alias PgInterop.Sublink

  @type ref() :: [String.t()]
  @type t() :: %{ref() => MapSet.t() | Sublink.t()}

  def new(%Sublink{} = view, _), do: view
  def new(keys, {:array, {:row, _}}), do: Sublink.composite_view(keys)
  def new(keys, _), do: keys

  @spec current(t(), ref()) :: MapSet.t()
  def current(views, subquery_ref) do
    views |> Map.get(subquery_ref, MapSet.new()) |> Sublink.keys()
  end

  @spec apply_move(t(), ref(), list(), :move_in | :move_out) :: t()
  @spec apply_move(t(), ref(), {list(), list()}, :replace) :: t()
  def apply_move(views, subquery_ref, {outs, ins}, :replace) do
    views
    |> apply_move(subquery_ref, outs, :move_out)
    |> apply_move(subquery_ref, ins, :move_in)
  end

  def apply_move(views, subquery_ref, values, :move_in) do
    Map.update!(views, subquery_ref, fn view ->
      Enum.reduce(values, view, fn {value, original}, view -> put(view, value, original) end)
    end)
  end

  def apply_move(views, subquery_ref, values, :move_out) do
    Map.update!(views, subquery_ref, fn view ->
      Enum.reduce(values, view, fn {value, _original_value}, view -> delete(view, value) end)
    end)
  end

  # Applies to the view the moves from `from_view` to `to_view` that remove rows: a positive view
  # loses the keys `to_view` lacks, a negated view gains the keys only `to_view` has.
  @spec apply_removals(
          t(),
          ref(),
          MapSet.t() | Sublink.t(),
          MapSet.t() | Sublink.t(),
          :positive | :negated
        ) :: t()
  def apply_removals(views, subquery_ref, from_view, to_view, :negated) do
    Map.update!(views, subquery_ref, fn view ->
      to_view
      |> Sublink.keys()
      |> MapSet.difference(Sublink.keys(from_view))
      |> Enum.reduce(view, &put(&2, &1, original(to_view, &1)))
    end)
  end

  def apply_removals(views, subquery_ref, from_view, to_view, :positive) do
    Map.update!(views, subquery_ref, fn view ->
      from_view
      |> Sublink.keys()
      |> MapSet.difference(Sublink.keys(to_view))
      |> Enum.reduce(view, &delete(&2, &1))
    end)
  end

  defp original(%Sublink{originals: %{} = originals}, value), do: Map.get(originals, value)
  defp original(_view, _value), do: nil

  defp put(%Sublink{} = view, value, original), do: Sublink.put(view, value, original)

  defp put(%MapSet{} = view, value, _) when is_tuple(value),
    do: Sublink.put(Sublink.composite_view(view), value)

  defp put(view, value, _), do: MapSet.put(view, value)

  defp delete(%Sublink{} = view, value), do: Sublink.delete(view, value)
  defp delete(view, value), do: MapSet.delete(view, value)
end
