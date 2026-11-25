defmodule Electric.AdmissionControl do
  @moduledoc """
  Simple admission control using ETS-based counters to limit concurrent requests per stack.

  This module prevents server overload by:
  - Limiting the number of concurrent requests per type of request (initial or existing) within a stack
  - Failing fast with 503 + Retry-After when at capacity
  - Using cheap ETS operations for minimal overhead

  ## Usage

      # Try to acquire a permit for a stack
      case Electric.AdmissionControl.try_acquire(stack_id, :initial, max_concurrent: 1000) do
        :ok ->
          # Request is allowed, process it
          # Don't forget to call release/1 when done!

        {:error, :overloaded} ->
          # Too many concurrent requests, return 503
      end

      # Always release the permit when done
      Electric.AdmissionControl.release(stack_id, :initial)

  ## Owned permits

  A process that can be stopped from outside, such as a Bandit HTTP/2
  stream process when its connection closes, never reaches its own release
  code. `try_acquire_owned/3`, `try_swap_owned/4` and `release_owned/3`
  record each permit against the calling process, and the GenServer
  monitors that process: when it exits still holding permits, they are
  released for it.

  The record is written before a counter goes up and cleared after it goes
  down. An owner stopped half-way through a step can only make a counter
  read one low (counters stop at zero, so this clears once the stack goes
  idle), never leave a permit counted with no holder.

  ## Configuration

  The max_concurrent limit can be configured in your config files:

      config :electric, :max_concurrent_requests, %{initial: 300, existing: 10_000}

  """

  use GenServer
  require Logger

  @table_name :electric_admission_control

  @doc """
  Start the admission control GenServer.

  ## Options

    * `:table_name` - Custom ETS table name (default: `:electric_admission_control`)
    * `:name` - GenServer name (default: `__MODULE__`)

  """
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @allowed_kinds ~w|initial existing|a
  for {kind, pos} <- Enum.with_index(@allowed_kinds, 2) do
    defp tuple_pos(unquote(kind)), do: unquote(pos)
  end

  @doc """
  Try to acquire a permit for the given stack_id.

  Returns `:ok` if permit granted, `{:error, :overloaded}` if at capacity.

  ## Options

    * `:max_concurrent` - Maximum concurrent requests allowed (default: 1000)
    * `:table_name` - ETS table name (default: `:electric_admission_control`)

  ## Examples

      iex> Electric.AdmissionControl.try_acquire("stack-123", :initial, max_concurrent: 1000)
      :ok

      iex> Electric.AdmissionControl.try_acquire("stack-123", :initial, max_concurrent: 1)
      {:error, :overloaded}

  """
  def try_acquire(stack_id, kind, opts \\ []) when kind in @allowed_kinds do
    table_name = Keyword.get(opts, :table_name, @table_name)

    max_concurrent =
      Keyword.get_lazy(opts, :max_concurrent, fn ->
        Electric.Config.get_env(:max_concurrent_requests)
        |> Map.fetch!(kind)
      end)

    current = incr(table_name, stack_id, kind)

    if current > max_concurrent do
      # At or over capacity, decrement back and reject
      decr(table_name, stack_id, kind)

      # Emit telemetry event
      :telemetry.execute(
        [:electric, :admission_control, :reject],
        %{count: 1, limit: max_concurrent},
        %{
          stack_id: stack_id,
          reason: :overloaded,
          kind: kind,
          current: current
        }
      )

      {:error, :overloaded}
    else
      # Successfully acquired permit
      # Emit telemetry for current concurrency level
      :telemetry.execute(
        [:electric, :admission_control, :acquire],
        %{count: 1, current: current, limit: max_concurrent},
        %{stack_id: stack_id, kind: kind}
      )

      :ok
    end
  end

  @doc """
  Release a permit for the given stack_id.

  Always call this after processing a request, even if it errors.
  Consider using a try/after or Plug's `register_before_send/2` callback.

  ## Options

    * `:table_name` - ETS table name (default: `:electric_admission_control`)

  ## Examples

      iex> Electric.AdmissionControl.release("stack-123", :initial)
      :ok

  """
  def release(stack_id, kind, opts \\ []) when kind in @allowed_kinds do
    table_name = Keyword.get(opts, :table_name, @table_name)
    decr(table_name, stack_id, kind)
    :ok
  end

  @doc """
  Move an in-flight permit from `from_kind` to `to_kind` for a stack.

  The caller must already hold a `from_kind` permit (acquired via
  `try_acquire/3`). Calling `try_swap/4` without an outstanding
  `from_kind` permit will silently grant a phantom `to_kind` permit.

  ## Options

    * `:max_concurrent` — required. Cap for `to_kind`.
    * `:table_name` — ETS table (default: `:electric_admission_control`).

  """
  @spec try_swap(String.t(), atom(), atom(), keyword()) :: :ok | {:error, :overloaded}
  def try_swap(stack_id, from_kind, to_kind, opts)
      when from_kind in @allowed_kinds and to_kind in @allowed_kinds do
    table_name = Keyword.get(opts, :table_name, @table_name)
    cap = Keyword.fetch!(opts, :max_concurrent)

    # Plain increment, no threshold clamp. Returns the post-increment value.
    new_to = incr(table_name, stack_id, to_kind)

    if new_to > cap do
      # Rollback the claimed permit since the bucket is already at capacity.
      decr(table_name, stack_id, to_kind)

      :telemetry.execute(
        [:electric, :admission_control, :swap_rejected],
        %{count: 1, current: new_to, limit: cap},
        %{stack_id: stack_id, from: from_kind, to: to_kind}
      )

      {:error, :overloaded}
    else
      # Success: drop the from_kind permit. Clamp at 0 to be defensive
      # against callers who lied about holding a from_kind permit.
      decr(table_name, stack_id, from_kind)

      :telemetry.execute(
        [:electric, :admission_control, :swap],
        %{count: 1, current: new_to, limit: cap},
        %{stack_id: stack_id, from: from_kind, to: to_kind}
      )

      :ok
    end
  end

  @doc """
  Get the current number of in-flight requests for a stack.

  Returns a map with `:initial` and `:existing` counts.

  Useful for monitoring and debugging.

  ## Options

    * `:table_name` - ETS table name (default: `:electric_admission_control`)

  ## Examples

      iex> Electric.AdmissionControl.get_current("stack-123")
      %{initial: 5, existing: 10}

  """
  def get_current(stack_id, opts \\ []) do
    table_name = Keyword.get(opts, :table_name, @table_name)

    case :ets.lookup(table_name, stack_id) do
      [{^stack_id, initial, existing}] -> %{initial: initial, existing: existing}
      [] -> %{initial: 0, existing: 0}
    end
  end

  @doc """
  Like `try_acquire/3`, but the permit belongs to the calling process and is
  released for it if it exits before calling `release_owned/3`.
  """
  @spec try_acquire_owned(String.t(), atom(), keyword()) :: :ok | {:error, :overloaded}
  def try_acquire_owned(stack_id, kind, opts \\ []) when kind in @allowed_kinds do
    table_name = Keyword.get(opts, :table_name, @table_name)

    watch_owner(table_name, stack_id)
    add_owned(table_name, stack_id, kind, 1)

    case try_acquire(stack_id, kind, opts) do
      :ok ->
        :ok

      {:error, :overloaded} = error ->
        drop_owned(table_name, stack_id, kind)
        error
    end
  end

  @doc """
  Like `try_swap/4`, for a permit taken with `try_acquire_owned/3`.
  """
  @spec try_swap_owned(String.t(), atom(), atom(), keyword()) :: :ok | {:error, :overloaded}
  def try_swap_owned(stack_id, from_kind, to_kind, opts)
      when from_kind in @allowed_kinds and to_kind in @allowed_kinds do
    table_name = Keyword.get(opts, :table_name, @table_name)

    add_owned(table_name, stack_id, to_kind, 1)

    case try_swap(stack_id, from_kind, to_kind, opts) do
      :ok ->
        drop_owned(table_name, stack_id, from_kind)
        :ok

      {:error, :overloaded} = error ->
        drop_owned(table_name, stack_id, to_kind)
        error
    end
  end

  @doc """
  Release a permit taken with `try_acquire_owned/3` or moved with
  `try_swap_owned/4`.
  """
  @spec release_owned(String.t(), atom(), keyword()) :: :ok
  def release_owned(stack_id, kind, opts \\ []) when kind in @allowed_kinds do
    table_name = Keyword.get(opts, :table_name, @table_name)

    release(stack_id, kind, opts)
    drop_owned(table_name, stack_id, kind)
  end

  # Owned permits live in the counters table as `{{:owned, pid, stack_id},
  # initial, existing}`, next to the `{stack_id, initial, existing}` counters.
  defp owned_key(stack_id), do: {:owned, self(), stack_id}

  defp add_owned(table_name, stack_id, kind, n) do
    key = owned_key(stack_id)
    :ets.update_counter(table_name, key, {tuple_pos(kind), n}, {key, 0, 0})
  end

  defp drop_owned(table_name, stack_id, kind) do
    key = owned_key(stack_id)
    :ets.update_counter(table_name, key, {tuple_pos(kind), -1, 0, 0}, {key, 0, 0})
    # Only the owner writes its row while it is alive, so this cannot race.
    :ets.delete_object(table_name, {key, 0, 0})
    :ok
  end

  # The table belongs to the GenServer, so its owner is the process to tell.
  defp watch_owner(table_name, stack_id) do
    GenServer.cast(:ets.info(table_name, :owner), {:watch, self(), stack_id})
  end

  @impl true
  def init(opts) do
    table_name = Keyword.get(opts, :table_name, @table_name)

    :ets.new(table_name, [
      :named_table,
      :public,
      :set,
      write_concurrency: true,
      read_concurrency: true
    ])

    Logger.notice("Admission control initialized with table: #{table_name}")
    {:ok, %{table_name: table_name, owners: %{}}}
  end

  @impl true
  def handle_cast({:watch, pid, stack_id}, %{owners: owners} = state) do
    owners =
      case owners do
        %{^pid => stack_ids} ->
          Map.put(owners, pid, MapSet.put(stack_ids, stack_id))

        %{} ->
          # A pid that is already gone gets an immediate :DOWN.
          Process.monitor(pid)
          Map.put(owners, pid, MapSet.new([stack_id]))
      end

    {:noreply, %{state | owners: owners}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %{owners: owners} = state) do
    {stack_ids, owners} = Map.pop(owners, pid, MapSet.new())

    for stack_id <- stack_ids do
      case :ets.take(state.table_name, {:owned, pid, stack_id}) do
        [{_key, initial, existing}] when initial > 0 or existing > 0 ->
          if initial > 0, do: decr(state.table_name, stack_id, :initial, initial)
          if existing > 0, do: decr(state.table_name, stack_id, :existing, existing)

          :telemetry.execute(
            [:electric, :admission_control, :reclaim],
            %{initial: initial, existing: existing},
            %{stack_id: stack_id}
          )

        _ ->
          :ok
      end
    end

    {:noreply, %{state | owners: owners}}
  end

  defp incr(table_name, stack_id, kind) do
    :ets.update_counter(table_name, stack_id, {tuple_pos(kind), 1}, {stack_id, 0, 0})
  end

  # `n` must be positive: with a step of 0 the threshold reads as an upper
  # bound and resets any positive count to 0.
  defp decr(table_name, stack_id, kind, n \\ 1) when n > 0 do
    :ets.update_counter(table_name, stack_id, {tuple_pos(kind), -n, 0, 0}, {stack_id, 0, 0})
  end
end
