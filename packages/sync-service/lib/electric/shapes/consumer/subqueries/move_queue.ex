defmodule Electric.Shapes.Consumer.Subqueries.MoveQueue do
  @moduledoc """
  Multi-dependency move queue. Tracks move_in/move_out operations per dependency index,
  with deduplication and redundancy elimination scoped per dependency.

  Move-outs from any dependency are drained before move-ins from any dependency.

  Each per-dep batch also accumulates the upstream Postgres transaction ids that
  contributed to it, so move-in/move-out broadcasts can carry `txids` for client
  attribution (mirroring `Electric.LogItems.from_change/4`).
  """

  alias Electric.Postgres.Xid

  @type move_value() :: {term(), term()}
  @type txid() :: pos_integer()
  @type entry() :: {[move_value()], MapSet.t(txid()), MapSet.t(txid())}

  defstruct move_out: %{}, move_in: %{}, replacements: MapSet.new(), late_removals: MapSet.new()

  @type t() :: %__MODULE__{
          move_out: %{non_neg_integer() => entry()},
          move_in: %{non_neg_integer() => entry()},
          replacements: MapSet.t(non_neg_integer()),
          late_removals: MapSet.t(non_neg_integer())
        }

  @type batch_kind() :: :move_out | :move_in
  @type batch() :: {batch_kind(), non_neg_integer(), [move_value()], [txid()], [txid()]}
  @type replacement() ::
          {:replace, non_neg_integer(), {[move_value()], [move_value()]}, [txid()], [txid()]}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec length(t()) :: non_neg_integer()
  def length(%__MODULE__{move_out: move_out, move_in: move_in}) do
    count_values(move_out) + count_values(move_in)
  end

  defp count_values(map) do
    Enum.reduce(map, 0, fn {_, {vs, _, _}}, acc -> acc + Kernel.length(vs) end)
  end

  @doc """
  Enqueue a materializer payload for a specific dependency.
  `dep_view` is the current view for this dependency, used for redundancy elimination.

  The payload may include a `:txids` key listing the upstream xids that produced
  the moves. Those xids are unioned with any already accumulated for this dep.
  """
  @spec enqueue(t(), non_neg_integer(), map() | keyword(), MapSet.t()) :: t()
  def enqueue(%__MODULE__{} = queue, dep_index, payload, %MapSet{} = dep_view)
      when is_map(payload) or is_list(payload) do
    payload = Map.new(payload)
    new_txids = payload |> Map.get(:txids, []) |> MapSet.new()
    new_txids8 = payload |> Map.get(:txids8, []) |> MapSet.new()

    {existing_outs, existing_out_txids, existing_out_txids8} =
      Map.get(queue.move_out, dep_index, {[], MapSet.new(), MapSet.new()})

    {existing_ins, existing_in_txids, existing_in_txids8} =
      Map.get(queue.move_in, dep_index, {[], MapSet.new(), MapSet.new()})

    ops =
      Enum.map(existing_outs, &{:move_out, &1}) ++
        Enum.map(existing_ins, &{:move_in, &1}) ++
        payload_to_ops(payload)

    {new_outs, new_ins} = reduce(ops, dep_view)

    # Attribution is accumulated across queued changes. Only a fresh payload
    # from one transaction establishes that these halves arrived together.
    replacements =
      if existing_outs == [] and existing_ins == [] and new_outs != [] and new_ins != [] and
           single_transaction?(new_txids, new_txids8),
         do: MapSet.put(queue.replacements, dep_index),
         else: MapSet.delete(queue.replacements, dep_index)

    # A removal that reaches a queued addition, or shares a payload with one,
    # may come from a later commit: the view without both never existed.
    late_removals =
      cond do
        new_outs == [] or new_ins == [] or MapSet.member?(replacements, dep_index) ->
          MapSet.delete(queue.late_removals, dep_index)

        Map.get(payload, :move_out, []) != [] and
            (existing_ins != [] or Map.get(payload, :move_in, []) != []) ->
          MapSet.put(queue.late_removals, dep_index)

        true ->
          queue.late_removals
      end

    %__MODULE__{
      replacements: replacements,
      late_removals: late_removals,
      move_out:
        put_or_delete(
          queue.move_out,
          dep_index,
          new_outs,
          MapSet.union(existing_out_txids, new_txids),
          MapSet.union(existing_out_txids8, new_txids8)
        ),
      move_in:
        put_or_delete(
          queue.move_in,
          dep_index,
          new_ins,
          MapSet.union(existing_in_txids, new_txids),
          MapSet.union(existing_in_txids8, new_txids8)
        )
    }
  end

  @doc """
  Pop the next batch of operations. Returns move-out batches (any dep) before move-in batches.
  Returns `{batch, updated_queue}` or `nil` if the queue is empty.
  """
  @spec pop_next(t()) :: {batch(), t()} | nil
  def pop_next(%__MODULE__{move_out: move_out} = queue) when move_out != %{} do
    {dep_index, {values, txids, txids8}} = Enum.min_by(move_out, &elem(&1, 0))

    {{:move_out, dep_index, values, sorted_txids(txids), sorted_txids(txids8)},
     %{queue | move_out: Map.delete(move_out, dep_index)}}
  end

  def pop_next(%__MODULE__{move_out: move_out, move_in: move_in} = queue)
      when move_out == %{} and move_in != %{} do
    {dep_index, {values, txids, txids8}} = Enum.min_by(move_in, &elem(&1, 0))

    {{:move_in, dep_index, values, sorted_txids(txids), sorted_txids(txids8)},
     %{
       queue
       | move_in: Map.delete(move_in, dep_index),
         replacements: MapSet.delete(queue.replacements, dep_index),
         late_removals: MapSet.delete(queue.late_removals, dep_index)
     }}
  end

  def pop_next(%__MODULE__{}), do: nil

  # A negated dependency can apply a same-transaction replacement at one
  # boundary. Pairing across commits can leap ahead of another dependency,
  # so halves from distinct commits pair only when no other one has a move.
  @spec pair_with_move_in(batch(), t()) :: {batch() | replacement(), t()}
  def pair_with_move_in({:move_out, dep_index, outs, txids, txids8} = move, queue) do
    with {ins, in_txids, in_txids8} <- Map.get(queue.move_in, dep_index),
         true <-
           MapSet.member?(queue.replacements, dep_index) or
             not other_dependency_queued?(queue, dep_index) do
      {{:replace, dep_index, {outs, ins}, merge_txids(txids, in_txids),
        merge_txids(txids8, in_txids8)},
       %{
         queue
         | move_in: Map.delete(queue.move_in, dep_index),
           replacements: MapSet.delete(queue.replacements, dep_index),
           late_removals: MapSet.delete(queue.late_removals, dep_index)
       }}
    else
      _ -> {move, queue}
    end
  end

  def pair_with_move_in(move, queue), do: {move, queue}

  @doc """
  Whether the dependency's queued removal reached a queued addition, so that
  applying it alone could show a view that no commit held.
  """
  @spec removal_after_addition?(t(), non_neg_integer()) :: boolean()
  def removal_after_addition?(%__MODULE__{} = queue, dep_index) do
    MapSet.member?(queue.late_removals, dep_index) and Map.has_key?(queue.move_in, dep_index)
  end

  def other_dependency_in_transaction?(%__MODULE__{} = queue, dep_index, txids, txids8) do
    Enum.any?([queue.move_out, queue.move_in], fn batches ->
      Enum.any?(batches, fn
        {^dep_index, _} -> false
        {_, {_, other_txids, other_txids8}} -> overlaps?(txids, txids8, other_txids, other_txids8)
      end)
    end)
  end

  defp overlaps?(txids, [], other_txids, _other_txids8) do
    Enum.any?(txids, &MapSet.member?(other_txids, &1))
  end

  defp overlaps?(txids, txids8, other_txids, other_txids8) do
    if MapSet.size(other_txids8) == 0,
      do: overlaps?(txids, [], other_txids, other_txids8),
      else: Enum.any?(txids8, &MapSet.member?(other_txids8, &1))
  end

  defp other_dependency_queued?(queue, dep_index) do
    Enum.any?([queue.move_out, queue.move_in], fn batches ->
      Enum.any?(Map.keys(batches), &(&1 != dep_index))
    end)
  end

  defp merge_txids(txids, %MapSet{} = other),
    do: sorted_txids(MapSet.union(MapSet.new(txids), other))

  defp single_transaction?(txids, txids8) do
    case {MapSet.to_list(txids), MapSet.to_list(txids8)} do
      {[_], []} -> true
      {[], [_]} -> true
      {[xid], [xid8]} -> Xid.compare(xid, xid8) == :eq
      _ -> false
    end
  end

  defp sorted_txids(%MapSet{} = txids), do: Enum.sort(txids)

  defp payload_to_ops(payload) do
    Enum.map(Map.get(payload, :move_out, []), &{:move_out, &1}) ++
      Enum.map(Map.get(payload, :move_in, []), &{:move_in, &1})
  end

  defp reduce(ops, base_view) do
    terminal_ops =
      ops
      |> Enum.with_index()
      |> Enum.reduce(%{}, fn {{kind, move_value}, index}, acc ->
        Map.put(acc, elem(move_value, 0), %{kind: kind, move_value: move_value, index: index})
      end)
      |> Map.values()
      |> Enum.reject(&redundant?(&1, base_view))
      |> Enum.sort_by(& &1.index)

    {
      for(%{kind: :move_out, move_value: move_value} <- terminal_ops, do: move_value),
      for(%{kind: :move_in, move_value: move_value} <- terminal_ops, do: move_value)
    }
  end

  defp redundant?(%{kind: :move_in, move_value: {value, _}}, base_view) do
    MapSet.member?(base_view, value)
  end

  defp redundant?(%{kind: :move_out, move_value: {value, _}}, base_view) do
    not MapSet.member?(base_view, value)
  end

  defp put_or_delete(map, key, [], _txids, _txids8), do: Map.delete(map, key)

  defp put_or_delete(map, key, values, txids, txids8),
    do: Map.put(map, key, {values, txids, txids8})
end
