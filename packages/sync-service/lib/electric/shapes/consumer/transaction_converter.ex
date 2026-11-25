defmodule Electric.Shapes.Consumer.TransactionConverter do
  # Converts transactions into append-change effects using Shape.convert_change/3.

  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.Transaction
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Shape

  @type convert_opts() :: keyword()

  @spec transaction_to_effects(Transaction.t(), Shape.t(), convert_opts()) ::
          {:ok, [Effects.t()]} | {:error, {:truncate, Changes.xid() | nil}}
  def transaction_to_effects(%Transaction{} = txn, %Shape{} = shape, opts \\ [])
      when is_list(opts) do
    with {:ok, effects, _written} <- txn_to_effects(txn, shape, opts, nil) do
      {:ok, effects}
    end
  end

  # With `old_refs_once_written: refs`, a change to a row that an earlier change of these
  # transactions wrote reads the row it replaces against `refs` instead of the old side of
  # `extra_refs`. The row it replaces must be one written: two rows can share a key inside a
  # transaction (a deferred primary key, or identical rows of a table without one), and a change to
  # the other one reads the old side of `extra_refs`. Identical rows count apart, so each change
  # that replaces one of them reads `refs`. Refs given as a function of the row read the views it
  # returns for that row.
  @spec transactions_to_effects([Transaction.t()], Shape.t(), convert_opts()) ::
          {:ok, [Effects.t()]} | {:error, {:truncate, Changes.xid() | nil}}
  def transactions_to_effects(txns, %Shape{} = shape, opts \\ [])
      when is_list(txns) and is_list(opts) do
    with {:ok, effects, _written} <- convert_transactions(txns, shape, opts) do
      {:ok, effects}
    end
  end

  # The same, with the rows these transactions wrote. A later call given them as `written:` reads a
  # change to one of those rows as it reads one to a row of its own transactions. Given
  # `written_key?:` as well, it reads so a change to a row for which that function, given the row's
  # key and record, returns true: routing drops a change whose rows match neither side of the
  # shape, and the change after it then replaces a row that no change it received wrote.
  @spec convert_transactions([Transaction.t()], Shape.t(), convert_opts()) ::
          {:ok, [Effects.t()], %{String.t() => Changes.row()} | nil}
          | {:error, {:truncate, Changes.xid() | nil}}
  def convert_transactions(txns, %Shape{} = shape, opts) when is_list(txns) and is_list(opts) do
    written =
      if Keyword.has_key?(opts, :old_refs_once_written), do: Keyword.get(opts, :written, %{})

    Enum.reduce_while(txns, {:ok, [], written}, fn txn, {:ok, acc, written} ->
      case txn_to_effects(txn, shape, opts, written) do
        {:ok, [], written} ->
          {:cont, {:ok, acc, written}}

        {:ok, effects, written} ->
          {:cont, {:ok, [effects | acc], written}}

        {:error, {:truncate, _xid}} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, effects, written} -> {:ok, effects |> Enum.reverse() |> List.flatten(), written}
      {:error, {:truncate, _xid}} = error -> error
    end
  end

  defp txn_to_effects(%Transaction{} = txn, %Shape{} = shape, opts, written) do
    with {:ok, changes, written} <- convert_changes(txn, shape, opts, written) do
      {:ok, append_effects(txn.xid, txn.full_xid, changes), written}
    end
  end

  defp convert_changes(%Transaction{} = txn, %Shape{} = shape, opts, written)
       when is_list(opts) do
    txn.changes
    |> Enum.reduce_while({[], written}, fn change, {acc, written} ->
      case change do
        %Changes.TruncatedRelation{} ->
          {:halt, {:error, {:truncate, txn.xid}}}

        # Framed once for this shape's whole fan-out class; nothing left to decide per shape.
        %Changes.FramedBlock{} = block ->
          {:cont, {[block | acc], written}}

        _ ->
          # Accumulate in reverse, flattening each conversion as we go so the head
          # of the accumulator is always the last emitted change.
          converted = Shape.convert_change(shape, change, change_opts(opts, change, written))
          {:cont, {Enum.reduce(converted, acc, &[&1 | &2]), track_written(written, change)}}
      end
    end)
    |> case do
      {:error, {:truncate, _xid}} = error ->
        error

      # Mark the last change before reversing, avoiding a separate pass to find
      # and rebuild the tail.
      {[], written} ->
        {:ok, [], written}

      {[last | rest], written} ->
        # Marking a change as last rewrites a header, so it can no longer use an encoding prepared
        # for a whole fan-out class -- only this shape's copy is last. Unwrapping drops the
        # prepared encoding, and this one change is re-encoded downstream.
        {:ok, Enum.reverse([%{unwrap_prepared(last) | last?: true} | rest]), written}
    end
  end

  defp change_opts(opts, change, written) do
    opts =
      if written != nil and
           (replaces_written_row?(written, change) or
              replaces_written_key?(Keyword.get(opts, :written_key?), change)) do
        {_old_refs, new_refs} = Keyword.fetch!(opts, :extra_refs)
        Keyword.put(opts, :extra_refs, {Keyword.fetch!(opts, :old_refs_once_written), new_refs})
      else
        opts
      end

    case Keyword.fetch(opts, :extra_refs) do
      {:ok, {old_refs, new_refs}} when is_function(old_refs, 1) or is_function(new_refs, 1) ->
        Keyword.put(
          opts,
          :extra_refs,
          {row_refs(old_refs, replaced_record(change)),
           row_refs(new_refs, written_record(change))}
        )

      _ ->
        opts
    end
  end

  defp row_refs(refs, nil) when is_function(refs, 1), do: %{}
  defp row_refs(refs, record) when is_function(refs, 1), do: refs.(record)
  defp row_refs(refs, _record), do: refs

  defp replaced_record(%Changes.UpdatedRecord{old_record: record}), do: record
  defp replaced_record(%Changes.DeletedRecord{old_record: record}), do: record
  defp replaced_record(_change), do: nil

  defp written_record(%Changes.NewRecord{record: record}), do: record
  defp written_record(%Changes.UpdatedRecord{record: record}), do: record
  defp written_record(_change), do: nil

  defp replaces_written_row?(written, change) do
    case replaced_row(change) do
      nil -> false
      row -> Map.has_key?(written, row)
    end
  end

  defp replaces_written_key?(nil, _change), do: false

  defp replaces_written_key?(written_key?, change) do
    case replaced_row(change) do
      nil -> false
      {key, record} -> written_key?.(key, record)
    end
  end

  defp replaced_row(%Changes.UpdatedRecord{old_key: old_key, key: key, old_record: record}),
    do: {old_key || key, record}

  defp replaced_row(%Changes.DeletedRecord{key: key, old_record: record}), do: {key, record}
  defp replaced_row(_change), do: nil

  # Counts the rows the changes wrote, by key and content, while they still exist.
  defp track_written(nil, _change), do: nil

  defp track_written(written, change) do
    written =
      if replaces_written_row?(written, change),
        do: remove_written(written, replaced_row(change)),
        else: written

    case change do
      %Changes.NewRecord{key: key, record: record} -> add_written(written, {key, record})
      %Changes.UpdatedRecord{key: key, record: record} -> add_written(written, {key, record})
      _ -> written
    end
  end

  defp add_written(written, row), do: Map.update(written, row, 1, &(&1 + 1))

  defp remove_written(written, row) do
    case Map.fetch!(written, row) do
      1 -> Map.delete(written, row)
      count -> Map.put(written, row, count - 1)
    end
  end

  defp unwrap_prepared(%Changes.PreparedChange{change: change}), do: change

  defp unwrap_prepared(%Changes.FramedBlock{}) do
    raise "a framed block was the final change of a transaction — the assembler must always " <>
            "emit the trailing change of a run individually so last? marking never splits a block"
  end

  defp unwrap_prepared(change), do: change

  defp append_effects(_xid, _full_xid, []), do: []

  defp append_effects(xid, full_xid, changes) do
    [%Effects.AppendChanges{changes: changes, xid: xid, full_xid: full_xid}]
  end
end
