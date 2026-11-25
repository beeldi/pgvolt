# What does the shape routing index cost to hold, to build, and to GROW, at a large
# shape population?
#
# Routing speed is the win this index exists for. What it charges is paid in three places:
#
#   build s      cumulative cost of registering every shape — paid at BOOT, inside
#                `ShapeLogCollector.handle_continue(:restore_shapes)`, which calls
#                `EventRouter.add_shape/3` once per restored shape and does so for every shape
#                regardless of `ELECTRIC_ROUTER_SHARDS` (the shard workers hold additional copies,
#                they do not divide this).
#   marginal us  cost of ONE more `add_shape` at that population — paid by every new client shape
#                registration, on the collector's serialized call path. If this grows with the
#                population, boot time is the lesser problem.
#   retained MB  what the filter holds, against a memory limit.
#
# One filter is grown incrementally and sampled at checkpoints, so the whole curve costs one
# build rather than one per size.
#
# It needs MIX_ENV=test: its stub inspector lives in test/support, which only the test build
# compiles.
#
#   MIX_ENV=test mix run --no-start dev/shape_filter_cost.exs             # multi-division IN filter
#   MIX_ENV=test DIVISIONS=0 mix run --no-start dev/shape_filter_cost.exs # full-firm filter
#   MIX_ENV=test CHECKPOINTS=10000,20000,40000 mix run --no-start dev/shape_filter_cost.exs
#
# Memory is read after a forced full sweep, so it is retained memory rather than garbage the
# collector has not reached yet.
Logger.configure(level: :error)

alias Electric.Shapes.Filter
alias Electric.Shapes.Shape

inspector =
  Support.StubInspector.new(
    tables: [{1, {"public", "employees"}}],
    columns: [
      %{name: "id", type: "text", pk_position: 0},
      %{name: "firmId", type: "text"},
      %{name: "divisionId", type: "text"},
      %{name: "deletedAt", type: "timestamptz"},
      %{name: "metaJson", type: "text"},
      %{name: "divisionIds", array_type: "text"}
    ]
  )

# The bench has two shape forms: a multi-division filter with an explicit IN list, or a full-firm
# filter with none. DIVISIONS=0 measures the full-firm form, isolating whether cost is driven by
# IN-list size or shape count.
division_list_size = String.to_integer(System.get_env("DIVISIONS", "20"))
shapes_per_firm = String.to_integer(System.get_env("SHAPES_PER_FIRM", "50"))
marginal_samples = String.to_integer(System.get_env("MARGINAL_SAMPLES", "200"))

checkpoints =
  System.get_env("CHECKPOINTS", "5000,10000,20000,40000,80000")
  |> String.split(",")
  |> Enum.map(&String.to_integer/1)

# CLAUSE=or_residual swaps the plain soft-delete conjunct for a disjunctive residual filter.
# `distribute_and_over_or` refuses to distribute an IN list over an indexable OR residual, so an
# unguarded OR check drops the firm-node residual into `other_shapes` - the map read, updated, and
# written back whole on every insert (upstream #4743).
row_filter =
  case System.get_env("CLAUSE", "default") do
    "or_residual" -> ~s|("deletedAt" IS NULL OR "metaJson" IS NOT NULL)|
    _ -> ~s|"deletedAt" IS NULL|
  end

# ARRAY_ACCESS=1 measures the array-overlap filter form (`&&`) instead of an IN list.
array_access? = System.get_env("ARRAY_ACCESS", "0") == "1"

shape_for = fn shape_index ->
  firm = div(shape_index, shapes_per_firm)

  division =
    cond do
      division_list_size == 0 ->
        ~s|"divisionId" IS NOT NULL|

      array_access? ->
        divisions =
          Enum.map_join(1..division_list_size, ", ", fn n -> "'d#{shape_index}_#{n}'" end)

        ~s|"divisionIds" && ARRAY[#{divisions}]::text[]|

      true ->
        divisions =
          Enum.map_join(1..division_list_size, ", ", fn n -> "'d#{shape_index}_#{n}'" end)

        ~s|"divisionId" IN (#{divisions})|
    end

  where = ~s|"firmId" = 'f#{firm}' AND #{division} AND #{row_filter}|

  {:ok, shape} = Shape.new(~s|"employees"|, where: where, inspector: inspector)
  shape
end

settled_memory = fn ->
  :erlang.garbage_collect()
  Enum.each(:erlang.processes(), &:erlang.garbage_collect/1)
  :erlang.memory(:total)
end

# A row matching the very first shape, so the correct answer never changes as the population grows
# and a rising routing cost cannot be mistaken for more matches.
change = %Electric.Replication.Changes.NewRecord{
  relation: {"public", "employees"},
  record: %{
    "id" => "e1",
    "firmId" => "f0",
    "divisionId" => "d0_1",
    "divisionIds" => "{d0_1}",
    "deletedAt" => nil
  },
  log_offset: Electric.Replication.LogOffset.first()
}

# The shapes prebuilt here are excluded from the marginal timing on purpose: `Shape.new/2` parses the
# where clause, and that cost belongs to the request path, not to the filter.
add_range = fn filter, from, to ->
  Enum.reduce(from..to, filter, fn index, filter ->
    Filter.add_shape(filter, index, shape_for.(index))
  end)
end

measure_marginal = fn filter, from ->
  prepared = Enum.map(from..(from + marginal_samples - 1), &{&1, shape_for.(&1)})

  {add_micros, filter} =
    :timer.tc(fn ->
      Enum.reduce(prepared, filter, fn {index, shape}, filter ->
        Filter.add_shape(filter, index, shape)
      end)
    end)

  # Removal is measured on the same shapes, at the same population, because an index that makes
  # registration cheap by making deletion linear has not fixed anything — it has moved the cost onto
  # shape expiry and rotation.
  {remove_micros, filter} =
    :timer.tc(fn ->
      Enum.reduce(prepared, filter, fn {index, _shape}, filter ->
        Filter.remove_shape(filter, index)
      end)
    end)

  {filter, _} =
    Enum.reduce(prepared, {filter, nil}, fn {index, shape}, {filter, _} ->
      {Filter.add_shape(filter, index, shape), nil}
    end)

  {add_micros / marginal_samples, remove_micros / marginal_samples, filter}
end

:io.format("~-10s ~12s ~10s ~12s ~14s ~10s ~9s~n", [
  "shapes",
  "build s",
  "add us",
  "remove us",
  "retained MB",
  "route us",
  "matched"
])

baseline = settled_memory.()

# ROUTE_MEMO=1 measures what ELECTRIC_FILTER_ROUTING_MEMO costs registration rather than
# routing: `track_routing_fields` ends every add and every remove with a `match_delete` whose key is
# unbound, which is a full scan of the memo. That scan is free on an empty memo, so the memo has to be
# filled to its cap before the marginal add is timed - otherwise the flag looks free at boot and
# charges in steady state, which is the opposite of what the numbers would say.
route_memo? = System.get_env("ROUTE_MEMO", "0") == "1"

fill_memo = fn filter ->
  if route_memo? do
    Enum.each(1..8192, fn n ->
      Filter.affected_shapes(filter, %{
        change
        | record: %{change.record | "divisionId" => "fill#{n}"}
      })
    end)
  end
end

Enum.reduce(checkpoints, {Filter.new(filter_routing_memo: route_memo?), 0, 0}, fn target,
                                                                                  {filter, added,
                                                                                   build_us} ->
  {grow_us, filter} = :timer.tc(fn -> add_range.(filter, added, target - 1) end)
  build_us = build_us + grow_us

  fill_memo.(filter)

  {add_us, remove_us, filter} = measure_marginal.(filter, target)
  added = target + marginal_samples

  retained = settled_memory.() - baseline

  Filter.affected_shapes(filter, change)

  {route_us, matched} =
    :timer.tc(fn ->
      Enum.reduce(1..100, nil, fn _, _ -> Filter.affected_shapes(filter, change) end)
    end)

  :io.format("~-10w ~12.1f ~10.1f ~12.1f ~14.1f ~10.1f ~9w~n", [
    target,
    build_us / 1_000_000,
    add_us,
    remove_us,
    retained / 1_048_576,
    route_us / 100,
    MapSet.size(matched)
  ])

  {filter, added, build_us}
end)
