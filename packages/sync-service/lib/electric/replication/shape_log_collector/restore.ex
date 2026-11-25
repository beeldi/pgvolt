defmodule Electric.Replication.ShapeLogCollector.Restore do
  @moduledoc false

  # The collector's restore reads every stored shape and adds each one it keeps to its routing.
  # Alone, the collector took ~250 µs a shape to decode it and add it to the filter: minutes
  # with a large shape count, on every restart, with live sync down all along.
  #
  # Here the collector only reads. It steps the store and hands each row on still encoded, a batch
  # at a time, to one of a few processes, picked by the root table the store files the shape under.
  # Each of them decodes its rows, adds them to the filter the router holds, whose tables they all
  # write, and returns the rest of what goes in the collector's state, for the collector to merge.
  # Every row on one table reaches the same process, in the order of the read, and that is what
  # keeps the result the one the collector builds alone:
  #
  #   * a table's conditions in the filter are only ever written by one process;
  #   * the routing state names a table after the first shape read on it.
  #
  # Tables that share a name share their conditions in the filter, so the first process to add a
  # shape on a name claims it, and a shape on a name another process claimed is left to the
  # collector, which adds it once the others are done, as are a shape the store files under a table
  # that is not its root table, which the store never does, and a shape with dependency handles but
  # no dependencies, which no shape has.
  #
  # Each call into SQLite runs on a dirty scheduler, and back from it the collector waits for a turn
  # on a scheduler the processes keep busy, so the store hands the rows over a page per call
  # (`ShapeDb.Query.list_encoded_shape_stream/1`) and not a row per call: a row per call made the
  # read the bound of the restore. The bound is now the process with the most shapes to restore,
  # busy all along while the others wait for rows. Fewer processes would leave the collector more
  # turns but do less, and its priority does not shorten the wait.

  alias Electric.Replication.Eval.Env
  alias Electric.Replication.ShapeLogCollector
  alias Electric.ShapeCache.ShapeStatus
  alias Electric.Shapes.EventRouter
  alias Electric.Shapes.Filter
  alias Electric.Shapes.Partitions
  alias Electric.Shapes.RouterWorker
  alias Electric.Shapes.Shape

  # Rows handed to a process at once, and how many such batches it can have waiting: enough to
  # keep it busy while the collector reads on, few enough that the read cannot run ahead with the
  # store in memory.
  @batch_rows 100
  @batches_ahead 4

  # A process decodes thousands of shapes and keeps little of them. On the default heap it would
  # collect garbage every few shapes, and every few batches of the binaries it is handed.
  @heap_words 1024 * 1024

  @typedoc """
  What the restore returns, for the collector to fold into its state:

    * `router`, `owners`, `count`: the shapes added to the filter, as `add_shape_to_owner/3` adds
      them, and how many;
    * `independent`: those shapes, all of them shapes without dependencies;
    * `relations`: those shapes by root table, `{oid, name}`, for the partitions;
    * `subquery`: the handles of the shapes with dependencies and of their dependencies;
    * `uncastable`: the shapes added filtering on a column Electric cannot cast, and those columns;
    * `deferred`: the shapes left to the collector, each with whether it is in the routing state;
    * `routing_state`: the routing state of every shape read but the deferred ones not in it.

  `uncastable` and `deferred` hold each shape with its place in the read, and in that order.
  """
  @type result :: %{
          router: EventRouter.t(),
          owners: %{Electric.shape_handle() => non_neg_integer() | :own},
          count: non_neg_integer(),
          independent: MapSet.t(Electric.shape_handle()),
          relations: %{Electric.oid_relation() => MapSet.t(Electric.shape_handle())},
          subquery: MapSet.t(Electric.shape_handle()),
          uncastable: [{non_neg_integer(), Electric.shape_handle(), list()}],
          deferred: [{non_neg_integer(), Electric.shape_handle(), Shape.t(), boolean()}],
          routing_state: ShapeStatus.routing_state()
        }

  @spec run(map(), ShapeStatus.routing_state()) :: result()
  def run(state, routing_state) do
    ref = make_ref()
    claims = :ets.new(:restore_claims, [:set, :public, write_concurrency: true])

    context = %{
      collector: self(),
      ref: ref,
      router: state.event_router,
      router_workers: state.workers,
      partitions: state.partitions,
      worker_prepare?: state.worker_prepare?,
      env: Env.new(),
      claims: claims,
      routing_state: routing_state
    }

    count = max(System.schedulers_online(), 2)
    spawn_opts = [:link, min_heap_size: @heap_words, min_bin_vheap_size: @heap_words]

    processes =
      List.to_tuple(
        for index <- 0..(count - 1),
            do: :erlang.spawn_opt(fn -> restore(context, index) end, spawn_opts)
      )

    reader = %{
      ref: ref,
      processes: processes,
      seq: 0,
      tables: %{},
      batches: Tuple.duplicate({0, []}, count),
      ahead: Tuple.duplicate(0, count)
    }

    try do
      case ShapeStatus.reduce_encoded_shapes(state.stack_id, reader, &dispatch/2) do
        {:error, error} -> raise error
        reader -> reader |> finish_read() |> merge(state.event_router, routing_state)
      end
    after
      :ets.delete(claims)
    end
  end

  defp dispatch({handle, oid, encoded}, reader) do
    # the store files every shape under its root table, so no shape is decoded here
    {oid, encoded} =
      case oid do
        nil ->
          shape = decode(encoded)
          {shape.root_table_id, shape}

        oid ->
          {oid, encoded}
      end

    {index, reader} = process_for(reader, oid)
    {length, rows} = elem(reader.batches, index)
    rows = [{reader.seq, handle, oid, encoded} | rows]
    reader = %{reader | seq: reader.seq + 1}

    if length + 1 == @batch_rows do
      reader |> put_batch(index, {0, []}) |> send_batch(index, rows)
    else
      put_batch(reader, index, {length + 1, rows})
    end
  end

  defp decode(%Shape{} = shape), do: shape
  defp decode(encoded) when is_binary(encoded), do: :erlang.binary_to_term(encoded)

  defp process_for(%{tables: tables} = reader, oid) do
    case tables do
      %{^oid => index} ->
        {index, reader}

      _ ->
        index = rem(map_size(tables), tuple_size(reader.processes))
        {index, %{reader | tables: Map.put(tables, oid, index)}}
    end
  end

  defp put_batch(reader, index, batch),
    do: %{reader | batches: put_elem(reader.batches, index, batch)}

  defp send_batch(reader, index, rows) do
    reader = await_room(reader, index)
    send(elem(reader.processes, index), {reader.ref, :rows, Enum.reverse(rows)})
    %{reader | ahead: put_elem(reader.ahead, index, elem(reader.ahead, index) + 1)}
  end

  defp await_room(reader, index) do
    if elem(reader.ahead, index) < @batches_ahead,
      do: reader,
      else: reader |> await_credit() |> await_room(index)
  end

  defp await_credit(%{ref: ref} = reader) do
    receive do
      {^ref, :credit, index} ->
        %{reader | ahead: put_elem(reader.ahead, index, elem(reader.ahead, index) - 1)}

      {^ref, :failed, kind, reason, stacktrace} ->
        :erlang.raise(kind, reason, stacktrace)
    end
  end

  defp finish_read(reader) do
    reader =
      Enum.reduce(0..(tuple_size(reader.processes) - 1), reader, fn index, reader ->
        case elem(reader.batches, index) do
          {0, []} -> reader
          {_length, rows} -> reader |> put_batch(index, {0, []}) |> send_batch(index, rows)
        end
      end)

    for pid <- Tuple.to_list(reader.processes), do: send(pid, {reader.ref, :done})

    await_results(reader.ref, tuple_size(reader.processes), %{})
  end

  defp await_results(_ref, count, results) when map_size(results) == count,
    do: results |> Enum.sort() |> Enum.map(&elem(&1, 1))

  defp await_results(ref, count, results) do
    receive do
      {^ref, :credit, _index} ->
        await_results(ref, count, results)

      {^ref, :restored, index, result} ->
        await_results(ref, count, Map.put(results, index, result))

      {^ref, :failed, kind, reason, stacktrace} ->
        :erlang.raise(kind, reason, stacktrace)
    end
  end

  defp merge(results, router, routing_state) do
    classes =
      Map.new(
        for %{fanout: fanout} <- results,
            {class, handles} <- fanout,
            handle <- handles,
            do: {handle, class}
      )

    owners = results |> Enum.flat_map(& &1.owned) |> Map.new()

    relations =
      results
      |> Enum.flat_map(&Map.to_list(&1.relations))
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Map.new(fn {table, handles} -> {table, handles |> Enum.concat() |> MapSet.new()} end)

    %{
      router: EventRouter.put_fanout_classes(router, classes),
      owners: owners,
      count: results |> Enum.map(& &1.count) |> Enum.sum(),
      independent: MapSet.new(Map.keys(owners)),
      relations: relations,
      subquery: MapSet.new(Enum.flat_map(results, & &1.subquery)),
      uncastable: results |> Enum.flat_map(& &1.uncastable) |> Enum.sort(),
      deferred: results |> Enum.flat_map(& &1.deferred) |> Enum.sort_by(&elem(&1, 0)),
      routing_state:
        ShapeStatus.join_routing_state(routing_state, Enum.map(results, & &1.routing_state))
    }
  end

  # -- the processes ------------------------------------------------------------------------------

  defp restore(%{ref: ref} = context, index) do
    acc = %{
      routing_state: ShapeStatus.share_routing_state(context.routing_state),
      claims: %{},
      owned: [],
      relations: %{},
      fanout: %{},
      subquery: [],
      uncastable: [],
      deferred: [],
      count: 0
    }

    try do
      acc = restore_batches(context, index, acc)

      # Shapes this process cast to the shard workers must reach them before whatever the
      # collector sends them next, and only the order of one sender's messages is kept.
      if acc.count > 0 and context.router_workers != nil and context.worker_prepare?,
        do: context.router_workers |> Tuple.to_list() |> Enum.each(&RouterWorker.sync/1)

      send(context.collector, {ref, :restored, index, Map.delete(acc, :claims)})
    catch
      kind, reason -> send(context.collector, {ref, :failed, kind, reason, __STACKTRACE__})
    end
  end

  defp restore_batches(%{ref: ref} = context, index, acc) do
    receive do
      {^ref, :rows, rows} ->
        acc = Enum.reduce(rows, acc, &restore_row(context, index, &1, &2))
        send(context.collector, {ref, :credit, index})
        restore_batches(context, index, acc)

      {^ref, :done} ->
        acc
    end
  end

  defp restore_row(context, index, {seq, handle, oid, encoded}, acc) do
    case decode(encoded) do
      %Shape{root_table_id: ^oid} = shape ->
        # the analysis of the shape's clause, which takes most of a shape's restore, run once for
        # both its routing state and the filter
        analysis = Filter.analyse_shape(shape)
        indexed? = Filter.indexed_analysis?(analysis)

        acc = %{
          acc
          | routing_state:
              ShapeStatus.add_to_routing_state(acc.routing_state, handle, shape, indexed?)
        }

        restore_shape(context, index, {seq, handle, shape, analysis}, acc)

      shape ->
        defer(acc, seq, handle, shape, false)
    end
  end

  # A shape with dependencies stays out of the routing, see `ShapeLogCollector`. One with dependency
  # handles but no dependencies goes to the collector, which checks those handles.
  defp restore_shape(context, index, {seq, handle, shape, analysis}, acc) do
    cond do
      shape.shape_dependencies != [] ->
        %{acc | subquery: [handle | shape.shape_dependencies_handles] ++ acc.subquery}

      shape.shape_dependencies_handles != [] ->
        defer(acc, seq, handle, shape, true)

      true ->
        case claim(context, index, shape.root_table, shape.root_table_id, acc) do
          {true, acc} -> add_shape(context, {seq, handle, shape, analysis}, acc)
          {false, acc} -> defer(acc, seq, handle, shape, true)
        end
    end
  end

  # A name is claimed with its first shape, whose root table is then introspected for the
  # partitions the collector restores once the read is over: one at a time, as the inspector
  # introspects them, but while the read goes on.
  defp claim(context, index, name, oid, %{claims: claims} = acc) do
    case claims do
      %{^name => mine?} ->
        {mine?, acc}

      _ ->
        mine? = :ets.insert_new(context.claims, {name, index})
        if mine?, do: Partitions.preload_root_table(context.partitions, oid)
        {mine?, %{acc | claims: Map.put(claims, name, mine?)}}
    end
  end

  defp add_shape(context, {seq, handle, shape, analysis}, acc) do
    uncastable =
      case ShapeLogCollector.uncastable_refs(shape, context.env) do
        [] -> acc.uncastable
        refs -> [{seq, handle, refs} | acc.uncastable]
      end

    owner =
      case context.router_workers do
        nil -> :own
        workers -> ShapeLogCollector.shard_shape(workers, context.worker_prepare?, handle, shape)
      end

    fanout =
      case EventRouter.add_restored_shape(context.router, handle, shape, analysis) do
        nil -> acc.fanout
        class -> Map.update(acc.fanout, class, [handle], &[handle | &1])
      end

    %{
      acc
      | uncastable: uncastable,
        owned: [{handle, owner} | acc.owned],
        fanout: fanout,
        relations:
          Map.update(
            acc.relations,
            {shape.root_table_id, shape.root_table},
            [handle],
            &[handle | &1]
          ),
        count: acc.count + 1
    }
  end

  defp defer(acc, seq, handle, shape, routed?),
    do: %{acc | deferred: [{seq, handle, shape, routed?} | acc.deferred]}
end
