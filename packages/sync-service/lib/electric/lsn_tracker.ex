defmodule Electric.LsnTracker do
  alias Electric.Postgres.Lsn
  import Electric, only: [is_stack_id: 1]

  @type stack_ref :: Electric.stack_id() | atom()
  @global_lsn_updates_topic :global_lsn_updates

  # this function is idempotent to avoid problems in tests
  @spec initialize(stack_ref()) :: :ok
  def initialize(stack_ref) do
    table = table(stack_ref)

    case :ets.info(table, :id) do
      :undefined ->
        :ets.new(table, [:public, :named_table, read_concurrency: true])

      ref when is_reference(ref) ->
        :ok
    end

    # Each table counts its replay generations from its own start (`replay_generation/1`).
    :ets.insert_new(table, {:replay_generation, make_ref(), 0})
    :ok
  end

  @spec set_last_processed_lsn(stack_ref(), Lsn.t() | non_neg_integer()) :: :ok
  def set_last_processed_lsn(stack_ref, lsn) when is_struct(lsn, Lsn) do
    stack_ref
    |> table()
    |> :ets.insert({:last_processed_lsn, lsn})

    :ok
  end

  def set_last_processed_lsn(stack_ref, lsn) when is_integer(lsn) do
    set_last_processed_lsn(stack_ref, Lsn.from_integer(lsn))
  end

  @spec initialize_last_processed_lsn(stack_ref(), Lsn.t()) :: :ok
  def initialize_last_processed_lsn(stack_ref, lsn) when is_struct(lsn, Lsn) do
    stack_ref
    |> table()
    |> :ets.insert_new({:last_processed_lsn, lsn})

    :ok
  end

  def initialize_last_processed_lsn(stack_ref, lsn) when is_integer(lsn) do
    initialize_last_processed_lsn(stack_ref, Lsn.from_integer(lsn))
  end

  @doc """
  Records where the replication slot resumes, as the replication client read it on connecting.
  """
  @spec set_slot_flushed_lsn(stack_ref(), Lsn.t() | non_neg_integer()) :: :ok
  def set_slot_flushed_lsn(stack_ref, lsn) when is_integer(lsn),
    do: set_slot_flushed_lsn(stack_ref, Lsn.from_integer(lsn))

  def set_slot_flushed_lsn(stack_ref, lsn) when is_struct(lsn, Lsn) do
    stack_ref |> table() |> :ets.insert({:slot_flushed_lsn, lsn})
    :ok
  end

  @doc """
  Has the next shape log collector replay from where the replication slot resumes, as after a
  restart of the stack, instead of from the position the last one processed.

  A writer stopped with the shape subsystem can lose transactions the collector took and the
  slot was not acknowledged past. Postgres resends them from the slot, and a collector that
  started from the position its predecessor processed dropped them. The request outlives the
  connection manager that makes it, until `apply_replay_from_slot/1` takes it.

  The broadcast watermark is dropped with it: it promised that what it covers was fanned out,
  and a transaction a stopped writer lost below it is fanned out again only by the replay. The
  replay generation then moves (`replay_generation/1`), and what the watermark covered stays
  taken (`dropped_high_water/1`).
  """
  @spec request_replay_from_slot(stack_ref()) :: :ok
  def request_replay_from_slot(stack_ref) do
    table = table(stack_ref)
    :ets.insert(table, {:replay_from_slot, true})
    drop_last_broadcast_lsn(table)
    next_replay_generation(table)
  end

  @doc """
  Sets the processed position to where the replication slot resumes, as the replication client
  last read it, when `request_replay_from_slot/1` asked for it, and drops the broadcast
  watermark the stopped collector may have raised after the request. Called before a shape
  subsystem starts.
  """
  @spec apply_replay_from_slot(stack_ref()) :: :ok
  def apply_replay_from_slot(stack_ref) do
    table = table(stack_ref)

    with true <- :ets.member(table, :replay_from_slot),
         [{:slot_flushed_lsn, lsn}] <- :ets.lookup(table, :slot_flushed_lsn) do
      :ets.insert(table, {:last_processed_lsn, lsn})
      drop_last_broadcast_lsn(table)
      :ets.delete(table, :replay_from_slot)
      next_replay_generation(table)
    end

    :ok
  rescue
    # A stack with no table requested nothing.
    ArgumentError -> :ok
  end

  @doc """
  Counts the drops of the broadcast watermark, for a replay from the slot, a replay of the journal
  or a purge, in this table. A request that read the watermark, then found the shape's log holding
  what it covers, answers with it only if the generation has not moved since: the subsystem
  started again for a replay has the log at the same offset while the replay has not yet
  delivered what a stopped writer lost below the watermark.

  The generation names the table too, which goes with the shape status owner: one created again
  counts from its own start, and a request that read the one before finds another generation.
  The name and the count are one row, read in one lookup: two reads could take the name from one
  table and the count from the one created after it. With no table, or before `initialize/1` or
  a first drop wrote the row, it is `nil`, which vouches for nothing.

  Read it before the watermark: each drop removes the watermark before it moves the count.
  """
  @spec replay_generation(stack_ref()) :: {reference(), non_neg_integer()} | nil
  def replay_generation(stack_ref) do
    case :ets.lookup(table(stack_ref), :replay_generation) do
      [{:replay_generation, incarnation, count}] -> {incarnation, count}
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc """
  The highest position a drop of the broadcast watermark took away since the last purge, or `nil`.

  A promotion check bounds what Electric took from the old server's stream by the positions that
  survive, and the watermark can be the only one at or past the new server's switchpoint: a
  keepalive fanned out with no journal boundary. A replay or a journal tail dropped it before
  the check ran. The processed position a replay sets back needs no such trace: what the
  collector processed is in the journal, and the other positions it held are the slot's.
  """
  @spec dropped_high_water(stack_ref()) :: non_neg_integer() | nil
  def dropped_high_water(stack_ref) do
    case :ets.lookup(table(stack_ref), :dropped_high_water) do
      [{:dropped_high_water, lsn}] -> lsn
      [] -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @doc """
  Forgets what `dropped_high_water/1` holds, once a purge of every shape is owed and recorded:
  the old stream's positions no longer bound anything, and they can exceed the new one's.
  """
  @spec forget_dropped_high_water(stack_ref()) :: :ok
  def forget_dropped_high_water(stack_ref) do
    stack_ref |> table() |> :ets.delete(:dropped_high_water)
    :ok
  rescue
    ArgumentError -> :ok
  end

  # Records what the watermark covers before it takes it, and takes only the value it recorded: a
  # promotion check, which reads the watermark first, and a manager that dies between the two
  # steps always find one of them. A broadcast in between raises the watermark, which is then
  # recorded and taken in turn. Keepalive positions are exclusive: a commit record starting there
  # was not taken.
  defp drop_last_broadcast_lsn(table) do
    case :ets.lookup(table, :last_broadcast_lsn) do
      [{:last_broadcast_lsn, lsn} = watermark] ->
        if lsn > 0, do: raise_dropped_high_water(table, lsn - 1)

        if :ets.select_delete(table, [{watermark, [], [true]}]) == 0,
          do: drop_last_broadcast_lsn(table),
          else: :ok

      [] ->
        :ok
    end
  end

  defp raise_dropped_high_water(table, lsn) do
    raised? = fn ->
      :ets.select_replace(table, [
        {{:dropped_high_water, :"$1"}, [{:<, :"$1", lsn}], [{{:dropped_high_water, lsn}}]}
      ]) == 1
    end

    _ = raised?.() or :ets.insert_new(table, {:dropped_high_water, lsn}) or raised?.()
    :ok
  end

  defp next_replay_generation(table) do
    _ =
      :ets.update_counter(table, :replay_generation, {3, 1}, {:replay_generation, make_ref(), 0})

    :ok
  end

  @spec get_last_processed_lsn(stack_ref()) :: Lsn.t() | nil
  def get_last_processed_lsn(stack_ref) do
    case stack_ref |> table() |> :ets.lookup(:last_processed_lsn) do
      [{:last_processed_lsn, lsn}] -> lsn
      [] -> nil
    end
  end

  @spec broadcast_last_seen_lsn(stack_ref(), Lsn.t() | non_neg_integer()) :: :ok
  def broadcast_last_seen_lsn(stack_ref, lsn) when is_struct(lsn, Lsn) do
    broadcast_last_seen_lsn(stack_ref, Lsn.to_integer(lsn))
  end

  def broadcast_last_seen_lsn(stack_ref, lsn) when is_integer(lsn) do
    # Two independent processes feed this value — the collector after each commit
    # fan-out and the replication client with server keepalive positions — with no
    # ordering between their writes. The value promises subscribers that everything
    # at or below it has been fanned out, and a consumer waiting on a move-in
    # boundary reads it once at subscribe time, so it may only ever advance: a
    # regression erases the highest safe position and the move-in never splices.
    if advance_last_broadcast_lsn(stack_ref, lsn) do
      registry = Electric.StackSupervisor.registry_name(stack_ref)

      if Process.whereis(registry) do
        Registry.dispatch(registry, @global_lsn_updates_topic, fn entries ->
          for {pid, _} <- entries, do: send(pid, {:global_last_seen_lsn, lsn})
        end)
      end
    end

    :ok
  end

  # Atomically raises the stored broadcast LSN, returning false when the stored value
  # is already at or past `lsn`. A missing table (some setups never initialize one)
  # keeps the pre-existing behavior of dispatching every broadcast; any other
  # ArgumentError is a structurally broken table and must surface, because swallowing
  # it would silently stop the cell from ever being written.
  defp advance_last_broadcast_lsn(stack_ref, lsn) do
    table = table(stack_ref)

    advanced? = fn ->
      :ets.select_replace(table, [
        {{:last_broadcast_lsn, :"$1"}, [{:<, :"$1", lsn}], [{{:last_broadcast_lsn, lsn}}]}
      ]) == 1
    end

    advanced?.() or :ets.insert_new(table, {:last_broadcast_lsn, lsn}) or advanced?.()
  rescue
    error in ArgumentError ->
      if :ets.whereis(table(stack_ref)) == :undefined,
        do: true,
        else: reraise(error, __STACKTRACE__)
  end

  @doc """
  Drop the broadcast watermark.

  The watermark promises subscribers that everything at or below it was fanned out on
  the current WAL timeline, and it only ever advances. After a shape purge — timeline
  change, restored backup, recreated replication slot — real positions may be lower
  than what it records, so it must be discarded with the shapes it described.

  The replay generation moves with it (`replay_generation/1`): a collector that starts with
  a journal tail to replay drops it too, and a request that read it before has nothing to
  vouch for until the replay has run. What it covered stays taken (`dropped_high_water/1`),
  until the purge forgets it.
  """
  @spec reset_last_broadcast_lsn(stack_ref()) :: :ok
  def reset_last_broadcast_lsn(stack_ref) do
    table = table(stack_ref)
    drop_last_broadcast_lsn(table)
    next_replay_generation(table)
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Flag that journaled transactions are awaiting boot-time replay.

  While the flag is up, the last-seen feed's promise does not hold: the collector is
  fanning out transactions the replication client never dispatched, so the client must
  hold keepalive positions instead of publishing them.
  """
  @spec mark_journal_replay_pending(stack_ref()) :: :ok
  def mark_journal_replay_pending(stack_ref) do
    stack_ref |> table() |> :ets.insert({:journal_replay_pending, true})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @spec clear_journal_replay_pending(stack_ref()) :: :ok
  def clear_journal_replay_pending(stack_ref) do
    stack_ref |> table() |> :ets.delete(:journal_replay_pending)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @spec journal_replay_pending?(stack_ref()) :: boolean()
  def journal_replay_pending?(stack_ref) do
    stack_ref |> table() |> :ets.member(:journal_replay_pending)
  rescue
    ArgumentError -> false
  end

  @doc """
  Whether it is proven that no boot-time journal replay is outstanding.

  The inverse of `journal_replay_pending?/1` with the opposite failure default: a
  missing table answers false. Use this for decisions that release a durability
  pin — advancing the replication slot past WAL a pending replay has not fanned
  out must not happen just because the table was unreadable.
  """
  @spec journal_replay_settled?(stack_ref()) :: boolean()
  def journal_replay_settled?(stack_ref) do
    not (stack_ref |> table() |> :ets.member(:journal_replay_pending))
  rescue
    ArgumentError -> false
  end

  @doc """
  Returns the most recently broadcast LSN, or 0 if none has been broadcast yet.
  """
  @spec get_last_broadcast_lsn(stack_ref()) :: non_neg_integer()
  def get_last_broadcast_lsn(stack_ref) do
    :ets.lookup_element(table(stack_ref), :last_broadcast_lsn, 2)
  rescue
    ArgumentError -> 0
  end

  @doc """
  Subscribe the calling process to global LSN updates.

  On success, if an LSN has already been broadcast, a `{:global_last_seen_lsn, lsn}`
  message is sent to the calling process (`self()`) before returning. Callers are
  expected to handle that message in their `handle_info/2` just like subsequent
  broadcasts.
  """
  @spec subscribe_to_global_lsn_updates(stack_ref(), term()) :: {:ok, pid()} | {:error, term()}
  def subscribe_to_global_lsn_updates(stack_ref, value \\ []) do
    # Register BEFORE reading the stored value. Dispatch is at-most-once per position
    # (non-advancing broadcasts are suppressed), so the replay below is the only
    # backstop for a position published before this process subscribed; reading first
    # would open a window where a position is neither replayed nor ever re-dispatched.
    with {:ok, _} <-
           Registry.register(
             Electric.StackSupervisor.registry_name(stack_ref),
             @global_lsn_updates_topic,
             value
           ) do
      last_lsn = get_last_broadcast_lsn(stack_ref)

      if last_lsn > 0 do
        send(self(), {:global_last_seen_lsn, last_lsn})
      end

      {:ok, self()}
    end
  end

  @spec unsubscribe_from_global_lsn_updates(stack_ref()) :: :ok
  def unsubscribe_from_global_lsn_updates(stack_ref) do
    Registry.unregister(
      Electric.StackSupervisor.registry_name(stack_ref),
      @global_lsn_updates_topic
    )
  end

  @doc """
  Returns the ETS table name used to store LSN info for the given stack ID.
  """
  @spec stack_ref(Electric.stack_id()) :: atom()
  def stack_ref(stack_id) when is_stack_id(stack_id), do: table(stack_id)

  defp table(stack_ref) when is_stack_id(stack_ref), do: :"#{inspect(__MODULE__)}:#{stack_ref}"
  defp table(stack_ref) when is_atom(stack_ref), do: stack_ref
end
