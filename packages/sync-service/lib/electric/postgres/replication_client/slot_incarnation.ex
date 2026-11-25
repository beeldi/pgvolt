defmodule Electric.Postgres.ReplicationClient.SlotIncarnation do
  @moduledoc """
  Names the incarnation of the replication slot that the full transaction source generation is
  bound to. PostgreSQL gives slots no identity, and a slot recreated under the same name leaves a
  WAL gap clients cannot detect, so each slot Electric creates gets a nonce persisted next to the
  timeline.

  The nonce is written as pending before CREATE_REPLICATION_SLOT and settled after its result. A
  pending nonce left by a crash, or a record that is missing, unreadable or names another slot,
  yields a fresh nonce: a spurious generation change costs clients a rebuild, a missed one keeps
  rows from the gap.

  The same write records whether creating the slot would owe a shape purge. A later attempt
  carries that decision forward if the earlier attempt never settled. The connection manager
  clears it after the purge or when the timeline check establishes that this is the first boot.
  """

  require Logger

  alias Electric.PersistentKV

  def prepare(kv, stack_id, slot_name, purge_on_create \\ nil) do
    previous = load(kv, stack_id)

    record = %{
      slot_name: slot_name,
      current: settled_current(previous, slot_name),
      pending: :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false),
      shape_purge: outstanding_purge(previous),
      purge_on_create: purge_on_create
    }

    store!(kv, stack_id, record)
    record
  end

  def settle(kv, stack_id, record, outcome) when outcome in [:created, :existing] do
    incarnation = (outcome == :existing && record.current) || record.pending
    shape_purge = if outcome == :created, do: outstanding_purge(record), else: record.shape_purge

    store!(kv, stack_id, %{
      record
      | current: incarnation,
        pending: nil,
        shape_purge: shape_purge,
        purge_on_create: nil
    })

    incarnation
  end

  # Readers waiting for the lock can act on a confirmed purge. The lock holder resolves
  # purge_on_create when it settles the attempt or carries it forward after an interruption.
  # A record that cannot be read owes `unreadable`.
  def shape_purge(kv, stack_id, unreadable \\ :required) do
    case load(kv, stack_id) do
      %{shape_purge: purge} -> purge
      :unreadable -> unreadable
      _ -> nil
    end
  end

  def clear_shape_purge(kv, stack_id) do
    case load(kv, stack_id) do
      %{pending: nil, shape_purge: purge} = record when not is_nil(purge) ->
        PersistentKV.set(kv, key(stack_id), %{record | shape_purge: nil})

      :unreadable ->
        {:error, :unreadable_slot_incarnation}

      _ ->
        :ok
    end
  rescue
    error in File.Error -> {:error, error}
  end

  defp outstanding_purge(%{shape_purge: :required}), do: :required
  defp outstanding_purge(%{purge_on_create: :required}), do: :required
  defp outstanding_purge(%{shape_purge: :if_previous_timeline}), do: :if_previous_timeline
  defp outstanding_purge(%{purge_on_create: :if_previous_timeline}), do: :if_previous_timeline
  defp outstanding_purge(:unreadable), do: :required
  defp outstanding_purge(_), do: nil

  defp settled_current(%{slot_name: slot_name, current: current, pending: nil}, slot_name)
       when is_binary(current),
       do: current

  defp settled_current(_record, _slot_name), do: nil

  defp load(nil, _stack_id), do: nil

  defp load(kv, stack_id) do
    case PersistentKV.get(kv, key(stack_id)) do
      {:ok, record} ->
        record

      {:error, :not_found} ->
        nil

      error ->
        Logger.warning(
          "Failed to load the replication slot incarnation: #{inspect(error)}. " <>
            "The full transaction source generation will change."
        )

        :unreadable
    end
  end

  defp store!(nil, _stack_id, _record), do: :ok
  defp store!(kv, stack_id, record), do: :ok = PersistentKV.set(kv, key(stack_id), record)

  defp key(stack_id), do: "replication_slot_incarnation_#{stack_id}"
end
