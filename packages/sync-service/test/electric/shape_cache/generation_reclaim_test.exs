defmodule Electric.ShapeCache.GenerationReclaimTest do
  use ExUnit.Case, async: true

  alias Electric.ShapeCache.GenerationReclaim
  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.Shapes.Consumer.Materializer

  @stack "generation-reclaim-test"

  setup do
    test_pid = self()

    Repatch.patch(ShapeDb, :list_shapes_page_after, fn _stack, after_handle, _limit ->
      page =
        Process.get(:fixture_shapes)
        |> Enum.filter(fn {handle, _shape} -> handle > after_handle end)
        |> Enum.sort()

      last = with {handle, _shape} <- List.last(page), do: handle
      {:ok, page, last}
    end)

    Repatch.patch(ShapeCleaner, :remove_shapes, fn _stack, handles, "shape_generation_gc" ->
      send(test_pid, {:removed, handles})
      :ok
    end)

    Repatch.patch(Materializer, :whereis, fn _stack, handle ->
      if handle in Process.get(:materialized, []), do: self(), else: nil
    end)

    :ok
  end

  defp suffix(n), do: " AND 'shape-rotation:db#{n}' = 'shape-rotation:db#{n}'"
  defp shape(query), do: %{where: %{query: query}}
  defp literal(value), do: "'" <> String.replace(value, "'", "''") <> "'"

  defp reclaim(shapes, overrides \\ %{}) do
    Process.put(:fixture_shapes, shapes)

    GenerationReclaim.reclaim(
      @stack,
      Map.merge(
        %{current_generation: 5, newest_collectable_generation: 4, budget_ms: 1_000, after: ""},
        overrides
      )
    )
  end

  defp removed do
    receive do
      {:removed, handles} -> handles ++ removed()
    after
      0 -> []
    end
  end

  test "removes the shapes of collectable generations and keeps the current ones" do
    assert {:ok, %{matched: 2, removed: 2, outcome: :ok}} =
             reclaim([
               {"h1", shape("owner = 'a'" <> suffix(3))},
               {"h2", shape("owner = 'b'" <> suffix(4))},
               {"h9", shape("owner = 'z'" <> suffix(5))}
             ])

    assert removed() == ["h1", "h2"]
  end

  test "a client value carrying the current token inside a literal does not block the page" do
    # partition bounds reach the where clause as literals, quotes doubled
    poisoned =
      ~s|("id" >= #{literal("a'shape-rotation:db5'")}) AND ("id" < #{literal("shape-rotation:db5")})|

    assert {:ok, %{matched: 2, removed: 2, outcome: :ok}} =
             reclaim([
               {"h1", shape("owner = 'a'" <> suffix(3))},
               {"h2", shape(poisoned <> suffix(3))},
               {"h9", shape("owner = 'z'" <> suffix(5))}
             ])

    assert removed() == ["h1", "h2"]
  end

  test "a shape whose clause compares the current token is still screened as live" do
    # selected by its old suffix but also carrying the current generation comparison
    both = "owner = 'a'" <> suffix(5) <> suffix(3)

    assert {:error, {:live_shapes, ["h2"]}} =
             reclaim([
               {"h1", shape("owner = 'a'" <> suffix(3))},
               {"h2", shape(both)}
             ])

    assert removed() == []
  end

  test "supports a custom token_prefix and ignores shapes with other prefixes" do
    custom_suffix = fn n -> " AND 'gen:#{n}' = 'gen:#{n}'" end

    assert {:ok, %{matched: 1, removed: 1, outcome: :ok}} =
             reclaim(
               [
                 {"h1", shape("owner = 'a'" <> custom_suffix.(3))},
                 {"h2", shape("owner = 'b'" <> suffix(3))},
                 {"h3", shape("owner = 'c'" <> custom_suffix.(5))}
               ],
               %{token_prefix: "gen:"}
             )

    assert removed() == ["h1"]
  end

  test "a shape with a running materializer is still screened as live" do
    Process.put(:materialized, ["h1"])

    assert {:error, {:live_shapes, ["h1"]}} =
             reclaim([{"h1", shape("owner = 'a'" <> suffix(3))}])

    assert removed() == []
  end

  test "an unterminated literal fails closed" do
    assert {:error, {:live_shapes, ["h1"]}} =
             reclaim([{"h1", shape("owner = 'a" <> suffix(3))}])

    assert removed() == []
  end
end
