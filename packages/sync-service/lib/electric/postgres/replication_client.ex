defmodule Electric.Postgres.ReplicationClient do
  @moduledoc """
  A client module for Postgres logical replication.
  """
  use Electric.Postgres.ReplicationConnection

  alias Electric.Postgres.LogicalReplication.Decoder
  alias Electric.Postgres.Lsn
  alias Electric.Postgres.FullTransactionId
  alias Electric.Postgres.LogicalReplication.Messages.Begin
  alias Electric.Postgres.LogicalReplication.Messages.Message
  alias Electric.LsnTracker
  alias Electric.Postgres.ReplicationClient.MessageConverter
  alias Electric.Postgres.ReplicationClient.ConnectionSetup
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.Replication.Changes.Relation
  alias Electric.Telemetry.OpenTelemetry
  alias Electric.Telemetry.Sampler

  require Logger
  require MessageConverter

  @type step ::
          :disconnected
          | :connected
          | :identify_system
          | :query_timeline_history
          | :query_pg_info
          | :acquire_lock
          | :create_publication
          | :check_if_publication_exists
          | :repair_publication
          | :drop_slot
          | :create_slot
          | :query_slot_flushed_lsn
          | :set_display_setting
          | :ready_to_stream
          | :start_streaming
          | :streaming

  defmodule State do
    @enforce_keys [:handle_event, :publication_name]
    defstruct [
      :stack_id,
      :connection_manager,
      :handle_event,
      :publication_name,
      :lock_acquired?,
      :try_creating_publication?,
      :recreate_slot?,
      :start_streaming?,
      :pg_version,
      :backend_pid,
      :system_identifier,
      :timeline_id,
      :database,
      :database_oid,
      :persistent_kv,
      :slot_incarnation,
      :slot_name,
      :slot_temporary?,
      :display_settings,
      :message_converter,
      :publication_owner?,
      :replication_idle_timeout,
      repair_publication?: false,
      manual_table_publishing?: false,
      replication_heartbeat_interval: 0,
      wal_sender_timeout: 60_000,
      step: :disconnected,
      wait_for_active_ref: nil,
      in_flight: :queue.new(),
      in_flight_count: 0,
      replay: nil,
      stale_refs: MapSet.new(),
      pipeline_healthy?: false,
      socket_paused?: false,
      paused_since: nil,
      pipeline_window: 1,
      received_wal: 0,
      flushed_wal: 0,
      last_confirmed_flush_lsn: 0,
      full_xid_source_positions?: false,
      provenance_anchor: nil,
      pending_provenance: nil,
      provenance_task: nil,
      provenance_deadline: nil,
      provenance_request: nil,
      replay_before_lsn: 0,
      last_seen_txn_lsn: Lsn.from_integer(0),
      last_seen_txn_timestamp: nil,
      flush_up_to_date?: true,
      ack_after_journal?: false,
      keep_shapes_on_promotion?: false,
      ingest_priority_high?: false,
      journal: nil,
      journal_base_path: nil,
      journal_backlog: :queue.new(),
      journal_backlog_rows: 0,
      pending_seen_wal: nil,
      retry_log_after: nil,
      idle_drain: nil
    ]

    @type t() :: %__MODULE__{
            stack_id: String.t(),
            connection_manager: pid(),
            handle_event: {module(), atom(), [term()]},
            publication_name: String.t(),
            try_creating_publication?: boolean(),
            manual_table_publishing?: boolean(),
            recreate_slot?: boolean(),
            start_streaming?: boolean(),
            pg_version: non_neg_integer(),
            slot_name: String.t(),
            slot_temporary?: boolean(),
            display_settings: [String.t()],
            message_converter: MessageConverter.t(),
            publication_owner?: boolean(),
            replication_idle_timeout: non_neg_integer(),
            replication_heartbeat_interval: non_neg_integer(),
            wal_sender_timeout: non_neg_integer(),
            step: Electric.Postgres.ReplicationClient.step(),
            wait_for_active_ref: {reference(), term()} | nil,
            in_flight: :queue.queue({reference(), term(), non_neg_integer(), integer()}),
            in_flight_count: non_neg_integer(),
            replay: :queue.queue({term(), non_neg_integer()}) | nil,
            stale_refs: MapSet.t(reference()),
            pipeline_healthy?: boolean(),
            socket_paused?: boolean(),
            paused_since: integer() | nil,
            pipeline_window: pos_integer(),
            received_wal: non_neg_integer(),
            flushed_wal: non_neg_integer(),
            last_confirmed_flush_lsn: non_neg_integer(),
            last_seen_txn_lsn: Lsn.t(),
            last_seen_txn_timestamp: integer(),
            flush_up_to_date?: boolean(),
            retry_log_after: integer() | nil,
            idle_drain: {reference(), boolean()} | nil
          }

    @opts_schema NimbleOptions.new!(
                   stack_id: [required: true, type: :string],
                   connection_manager: [required: true, type: :pid],
                   handle_event: [required: true, type: :mfa],
                   publication_name: [required: true, type: :string],
                   try_creating_publication?: [required: true, type: :boolean],
                   # In manual mode the operator owns the publication: Electric only checks it.
                   manual_table_publishing?: [type: :boolean, default: false],
                   start_streaming?: [type: :boolean, default: true],
                   slot_name: [required: true, type: :string],
                   slot_temporary?: [type: :boolean, default: false],
                   replication_idle_timeout: [type: :non_neg_integer, default: 0],
                   replication_heartbeat_interval: [type: :non_neg_integer, default: 0],
                   # Set a reasonable limit for the maximum size of a transaction that
                   # we can handle, above which we would exit as we run the risk of running
                   # out of memmory.
                   # TODO: stream out transactions and collect on disk to avoid this
                   max_txn_size: [type: {:or, [:non_neg_integer, nil]}, default: nil],
                   # Maximum number of changes to buffer before flushing a transaction fragment.
                   # Smaller values result in more message passing overhead but lower memory usage.
                   # The minimum allowed value is 2.
                   max_batch_size: [type: :non_neg_integer, default: 100],
                   pipeline_window: [type: :pos_integer, required: false],
                   ack_after_journal: [type: :boolean, required: false],
                   keep_shapes_on_promotion: [type: :boolean, required: false],
                   ingest_priority_high: [type: :boolean, required: false],
                   persistent_kv: [type: :any, default: nil]
                 )

    @spec new(Access.t()) :: t()
    def new(opts) do
      opts = NimbleOptions.validate!(opts, @opts_schema)
      settings = [display_settings: Electric.Postgres.display_settings()]
      opts = settings ++ opts

      {max_txn_size, opts} = Keyword.pop!(opts, :max_txn_size)
      {max_batch_size, opts} = Keyword.pop!(opts, :max_batch_size)
      {pipeline_window, opts} = Keyword.pop(opts, :pipeline_window)
      {ack_after_journal, opts} = Keyword.pop(opts, :ack_after_journal)
      {keep_shapes_on_promotion, opts} = Keyword.pop(opts, :keep_shapes_on_promotion)
      {ingest_priority_high, opts} = Keyword.pop(opts, :ingest_priority_high)

      # Assert the implicit requirement
      true = max_batch_size >= 2

      pipeline_window = Electric.Config.get_env(:replication_pipeline_window, pipeline_window)

      # NimbleOptions only validates the key when it is passed in opts, and the value is
      # popped before validation anyway — so an env-provided window reaches here unchecked.
      # A window below 1 wedges the socket silently: nothing ever reopens it.
      (is_integer(pipeline_window) and pipeline_window >= 1) ||
        raise ArgumentError,
              "replication pipeline window must be a positive integer, got: #{inspect(pipeline_window)}"

      ack_after_journal? = Electric.Config.get_env(:ack_after_journal, ack_after_journal)

      keep_shapes_on_promotion? =
        Electric.Config.get_env(:keep_shapes_on_promotion, keep_shapes_on_promotion)

      ingest_priority_high? =
        Electric.Config.get_env(:ingest_priority_high, ingest_priority_high)

      struct!(
        __MODULE__,
        opts ++
          [
            pipeline_window: pipeline_window,
            ack_after_journal?: ack_after_journal?,
            keep_shapes_on_promotion?: keep_shapes_on_promotion?,
            ingest_priority_high?: ingest_priority_high?,
            message_converter:
              MessageConverter.new(
                max_tx_size: max_txn_size,
                max_batch_size: max_batch_size
              )
          ]
      )
    end
  end

  # @type state :: State.t()

  alias Electric.Replication.TxnJournal

  # Rows the ack-after-journal backlog may hold in memory before pausing the socket. The
  # journal on disk is the durable copy; this only bounds the in-RAM dispatch queue.
  @journal_backlog_max_rows 200_000

  @repl_msg_x_log_data ?w
  @repl_msg_primary_keepalive ?k
  @repl_msg_standby_status_update ?r

  @default_connect_timeout 30_000
  @idle_check_interval Electric.Config.min_replication_idle_timeout()

  # Maximum keepalive interval. Caps the derived interval (wal_sender_timeout/3)
  # so we stay responsive even if wal_sender_timeout is set very high or changes
  # on the source PG after we've connected.
  @max_keepalive_interval 15_000

  # Delay before retrying a failed event dispatch.
  @event_retry_delay 50
  @provenance_coalescing_delay 5
  @provenance_timeout 30_000

  # Maximum time to spend retrying a crashed event handler before giving up.
  @max_event_retry_time 10 * 60_000

  # Cadence of the wedge check. Well under the threshold it tests, and slow enough that a real
  # wedge reports once every few minutes rather than filling the log for as long as it lasts.
  @wedge_check_interval 5 * 60_000

  # A handler that stays down logs the same failure on every one of those retries: at 50ms against a
  # ten-minute budget that is twelve thousand identical error lines, each carrying the crash reason,
  # into the log pipeline and the error tracker for a single incident. Log the first one immediately
  # and at most one per second after that; the sequence resets as soon as an event is acked.
  @retry_log_interval 1_000

  @spec start_link(Keyword.t()) :: :gen_statem.start_ret()
  def start_link(opts) do
    config = Map.new(opts)

    # Disable the reconnection logic in Postgex.ReplicationConnection to force it to exit with
    # the connection error. Without this, we may observe undesirable restarts in tests between
    # one test process exiting and the next one starting.
    start_opts =
      [
        name: name(config.stack_id),
        timeout: Access.get(opts, :timeout, @default_connect_timeout),
        auto_reconnect: false,
        sync_connect: false
      ] ++ Electric.Utils.deobfuscate_password(config.replication_opts[:connection_opts])

    Electric.Postgres.ReplicationConnection.start_link(
      __MODULE__,
      Keyword.delete(config.replication_opts, :connection_opts),
      start_opts
    )
  end

  def name(stack_id) do
    Electric.ProcessRegistry.name(stack_id, __MODULE__)
  end

  # This is a send() and not a call() to prevent the caller (the Connection.Manager process) from
  # getting blocked when the replication connection is blocked some replication slot condition
  # that doesn't let it start streaming immediately.
  def start_streaming(client) do
    send(client, :start_streaming)
  end

  def stop(client, reason) do
    Electric.Postgres.ReplicationConnection.call(client, {:stop, reason})
  end

  # The `Postgrex.ReplicationConnection` behaviour does not follow the gen server conventions and
  # establishes its own instead. Unless the `sync_connect: false` option is passed to `start_link()`, the
  # connection process will try opening a replication connection to Postgres before returning
  # from its `init()` callback.
  #
  # The callbacks `init()`, `handle_connect()` and `handle_result()` defined in this module
  # would all be invoked inside the connection process' `init()` callback in that case. Once
  # any of the callbacks return `{:stream, ...}`, the connection process finishes its
  # initialization and switches into the logical streaming mode to start receiving logical
  # messages from Postgres, invoking the `handle_data()` callback for each one.
  #
  # TODO(alco): this needs additional info about :noreply and :query return tuples.
  @impl true
  def init(replication_opts) do
    state = State.new(replication_opts)

    full_xid? =
      "full_xid_source_positions" in Electric.StackConfig.lookup(
        state.stack_id,
        :feature_flags,
        []
      )

    state = %{state | full_xid_source_positions?: full_xid?}
    Electric.StackConfig.put(state.stack_id, :full_xid_source_positions_ready, false)
    Electric.StackConfig.put(state.stack_id, :full_xid_source_generation, nil)

    Process.set_label({:replication_client, state.stack_id})

    # Under heavy live-serving load the ingest chain (socket read → decode →
    # journal → slot ack) is latency-bound by run-queue waits, not CPU: its
    # serial work is seconds while measured parity under load is tens of
    # seconds. Elevated priority lets it preempt serving work. What bounds the
    # starvation risk is the backlog cap and the journal fsyncs, which yield to
    # dirty IO — not socket pacing, which is exactly what journal mode removes.
    if state.ingest_priority_high? do
      Process.flag(:priority, :high)
      Logger.notice("Replication client running at high scheduler priority")
    end

    Logger.metadata(stack_id: state.stack_id, is_connection_process?: true)
    Electric.Telemetry.Sentry.set_tags_context(stack_id: state.stack_id)

    {:ok, state}
  end

  # `Postgrex.ReplicationConnection` opens a new replication connection to Postgres and then
  # gives us a chance to execute one or more queries before switching into the logical
  # streaming mode. It doesn't give us the connection socket but instead takes the query returned
  # by one of our `handle_connect/1`, `handle_result/2` or `handle_info/2` callbacks, executes
  # it, invokes the `handle_result/2` callback on the result which may return another query to
  # execute, executes that, and so it goes on and on, recursively, until a callback returns
  # `{:noreply, ...}` or `{:streaming, ...}`.
  #
  # To execute a series of queries one after the other, we define an ad-hoc state
  # machine that starts from the :connected state in `handle_connect/1`, then transitions to
  # the next step and returns the appropriate query to `Postgrex.ReplicationConnection` for execution,
  # This is all implemented in a separate module named `Electric.Postgres.ReplicationClient.ConnectionSetup`
  # to separate the connection setup logic from logical streaming.

  @impl true
  def handle_connect(state) do
    %{state | step: :connected}
    |> notify_connection_opened()
    |> ConnectionSetup.start()
  end

  @impl true
  def handle_disconnect(state) do
    if state.provenance_task, do: Task.shutdown(state.provenance_task, :brutal_kill)
    # The drain finishes its bounded call: the write buffer runs it whether anyone waits or not.
    with {ref, _seen_data?} <- state.idle_drain, do: Process.demonitor(ref, [:flush])
    Electric.StackConfig.put(state.stack_id, :full_xid_source_positions_ready, false)
    Electric.StackConfig.put(state.stack_id, :full_xid_source_generation, nil)

    {:noreply,
     %{
       state
       | provenance_anchor: nil,
         provenance_task: nil,
         pending_provenance: nil,
         provenance_deadline: nil,
         provenance_request: nil,
         idle_drain: nil
     }}
  end

  @impl true
  def handle_result(result_list_or_error, state) do
    {current_step, next_step, extra_info, updated_state, return_val} =
      ConnectionSetup.process_query_result(result_list_or_error, state)

    if current_step == :identify_system,
      do: notify_system_identified(state, extra_info)

    if current_step == :query_timeline_history,
      do: notify_timeline_history_obtained(updated_state, extra_info)

    if current_step == :query_pg_info,
      do: notify_pg_info_obtained(state, extra_info)

    if current_step == :acquire_lock do
      case extra_info do
        :lock_acquired -> notify_lock_acquired(state)
        {:lock_acquisition_failed, error} -> notify_lock_acquisition_error(state, error)
      end
    end

    # for new slots, always reset the last processed LSN
    if current_step == :create_slot and extra_info == :created_new_slot do
      Electric.LsnTracker.set_last_processed_lsn(state.stack_id, updated_state.flushed_wal)
      notify_created_new_slot(state)
    end

    existing_slot? = current_step == :query_slot_flushed_lsn and next_step != :create_slot

    # for existing slots, populate the last processed LSN if not present
    if existing_slot?,
      do:
        Electric.LsnTracker.initialize_last_processed_lsn(
          state.stack_id,
          updated_state.flushed_wal
        )

    # Where the slot resumes, for a shape subsystem the connection manager starts again
    # (`Electric.LsnTracker.request_replay_from_slot/1`). The slot's own position, not the
    # journal boundary journal mode acks at: a purge removes the journal, and on a database
    # whose slot is behind that boundary a collector started there dropped what the slot resends.
    if existing_slot? or (current_step == :create_slot and extra_info == :created_new_slot) do
      Electric.LsnTracker.set_slot_flushed_lsn(
        state.stack_id,
        max(updated_state.replay_before_lsn - 1, 0)
      )
    end

    if next_step == :ready_to_stream,
      do: notify_ready_to_stream(state)

    return_val
  end

  @impl true
  def handle_call({:stop, reason}, from, _state) do
    Logger.notice(
      "Replication client #{inspect(self())} is stopping after receiving stop request from #{inspect(elem(from, 0))} with reason #{inspect(reason)}"
    )

    {:disconnect, reason}
  end

  @impl true
  def handle_info({:flush_boundary_updated, lsn}, state) do
    state =
      %{state | last_confirmed_flush_lsn: max(lsn, state.last_confirmed_flush_lsn)}
      |> update_flush_up_to_date()

    state =
      if state.flush_up_to_date? do
        # In journal mode the journal boundary is the only thing that may move flushed_wal;
        # everywhere else this is the original `flushed_wal: received_wal`, clamped with max
        # so a boundary already reported durable is never walked back.
        %{
          state
          | flushed_wal: max(state.flushed_wal, journal_free_wal(state)),
            received_wal: max(lsn, state.received_wal)
        }
      else
        %{state | flushed_wal: max(lsn, state.flushed_wal)}
      end

    {:noreply, [encode_standby_status_update(state)], state}
  end

  # Consumer-durable boundary (journal mode): generations the fan-out has fully drained
  # past become garbage.
  @impl true
  def handle_info({:journal_consumer_boundary, lsn}, %State{} = state) do
    case state.journal do
      %TxnJournal.Writer{} = journal ->
        {:noreply, %{state | journal: TxnJournal.gc!(journal, lsn)}}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(:start_streaming, %State{step: :ready_to_stream} = state) do
    ConnectionSetup.start_streaming(state)
  end

  def handle_info(:start_streaming, %State{step: step} = state) do
    Logger.debug("Replication client requested to start streaming while step=#{step}")
    {:noreply, state}
  end

  # Nothing else can see this stall. The socket is paused because downstream has not acked, and
  # the keepalive timer above keeps Postgres's `reply_time` fresh, so `wal_sender_timeout` never
  # fires and the slot stays pinned for as long as the wedge lasts. `replication_idle_timeout`
  # cannot cover it either: it measures time since the last transaction, which is also what a
  # quiet database looks like, and that is why it ships disabled. A pause has no such twin: an
  # idle stack pauses nothing, and every pause is meant to end at the next ack.
  def handle_info(:check_if_wedged, %State{paused_since: paused_since} = state)
      when is_integer(paused_since) do
    age = System.monotonic_time(:millisecond) - paused_since

    if age >= @max_event_retry_time do
      Logger.error(
        "Replication is wedged: the socket has been paused for #{age}ms waiting for downstream, " <>
          "with #{state.in_flight_count} event(s) in flight. The slot cannot advance past " <>
          "#{state.flushed_wal}, and Postgres cannot detect this because keepalives keep the " <>
          "connection alive."
      )
    end

    {:noreply, state}
  end

  def handle_info(:check_if_wedged, state), do: {:noreply, state}

  def handle_info(:check_if_idle, %State{idle_drain: {_ref, _seen_data?}} = state),
    do: {:noreply, state}

  def handle_info(:check_if_idle, %State{} = state) do
    if idle_time(state) >= state.replication_idle_timeout do
      {:noreply, start_idle_drain(state)}
    else
      {:noreply, state}
    end
  end

  # Replication data that arrived during the drain keeps the connection open, and starts a new
  # idle window: its transaction can still wait for the collector's acknowledgement, which is
  # what moves the idle clock.
  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %State{idle_drain: {ref, seen_data?}} = state
      ) do
    state = %{state | idle_drain: nil}
    time_diff = idle_time(state)

    cond do
      seen_data? ->
        {:noreply, %{state | last_seen_txn_timestamp: System.monotonic_time()}}

      time_diff >= state.replication_idle_timeout ->
        {:disconnect, {:shutdown, {:connection_idle, time_diff}}}

      true ->
        {:noreply, state}
    end
  end

  # A quiet database and a stream that stopped reading its socket look the same to the idle
  # check: no transaction for a while. The heartbeat tells them apart. Electric writes a logical
  # decoding message through its admin pool and reads it back on its own slot, so a stream that
  # still reads never looks idle, while one that stopped reading still trips the idle timeout.
  # The write runs in a task: the replication connection cannot run queries while streaming.
  def handle_info(:send_heartbeat, %State{step: :streaming} = state) do
    supervisor = Electric.ProcessRegistry.name(state.stack_id, Electric.StackTaskSupervisor)
    pool = Electric.Connection.Manager.admin_pool(state.stack_id)
    query = heartbeat_query(state.pg_version)

    Task.Supervisor.start_child(supervisor, fn -> emit_heartbeat(pool, query) end)
    {:noreply, state}
  catch
    :exit, reason ->
      Logger.warning("Failed to start a replication heartbeat: #{inspect(reason)}")
      {:noreply, state}
  end

  def handle_info(:send_heartbeat, state), do: {:noreply, state}

  # Periodic keepalive: send a StandbyStatusUpdate to prevent wal_sender_timeout.
  # This fires every @keepalive_interval ms regardless of whether the socket is
  # paused. Sending the same LSN without advancement is safe — it only resets
  # PostgreSQL's last_reply_timestamp.
  def handle_info(:send_keepalive, %State{step: :streaming} = state) do
    {:noreply, [encode_standby_status_update(state)], state}
  end

  def handle_info(:send_keepalive, state) do
    {:noreply, state}
  end

  # Event processing messages — retry path only; see dispatch_event/2 and start_event/3
  # below. The socket is always paused when a retry is scheduled, so both outcomes
  # return a plain :noreply.
  def handle_info({:process_event, event, time_remaining}, state) do
    case start_event(event, time_remaining, state) do
      {:dispatched, state} -> {:noreply, state}
      {:retry_scheduled, state} -> {:noreply, state}
    end
  end

  def handle_info(
        {:fetch_provenance_anchor, reference},
        %State{provenance_task: nil, provenance_request: reference} = state
      )
      when not is_nil(reference) do
    source =
      Map.take(state, [
        :slot_name,
        :backend_pid,
        :system_identifier,
        :timeline_id,
        :database,
        :database_oid
      ])

    supervisor = Electric.ProcessRegistry.name(state.stack_id, Electric.StackTaskSupervisor)

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        FullTransactionId.fetch_anchor!(
          Electric.Connection.Manager.admin_pool(state.stack_id),
          source
        )
      end)

    {:noreply, %{state | provenance_task: task}}
  end

  def handle_info({:fetch_provenance_anchor, _reference}, state), do: {:noreply, state}

  def handle_info({ref, anchor}, %State{provenance_task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    pending = state.pending_provenance

    state = %{
      state
      | provenance_anchor: anchor,
        provenance_task: nil,
        pending_provenance: nil,
        provenance_request: nil
    }

    result =
      case pending do
        {:first_message, data} ->
          Electric.StackConfig.put(
            state.stack_id,
            :full_xid_source_generation,
            source_generation!(state)
          )

          Electric.StackConfig.put(state.stack_id, :full_xid_source_positions_ready, true)
          notify_seen_first_message(state)
          handle_data(data, %{state | provenance_deadline: nil})

        %Begin{} = begin ->
          convert_begin(begin, state)
      end

    case result do
      {:noreply, state} -> resume_if_paused([], state)
      {:noreply, messages, state} -> resume_if_paused(messages, state)
      {:noreply_and_pause, messages, state} -> {:noreply, messages, state}
      result -> result
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{provenance_task: %Task{ref: ref}}) do
    {:disconnect, {:full_transaction_provenance_unavailable, reason}}
  end

  # StatusMonitor notification: stack became active — retry the pending event.
  # The delay prevents spinning if the handler returns :not_ready again despite
  # StatusMonitor reporting :active (a brief race during startup).
  def handle_info(
        {{Electric.StatusMonitor, ref}, {:ok, :active}},
        %State{wait_for_active_ref: {ref, event}} = state
      ) do
    Process.demonitor(ref, [:flush])
    Logger.info("Stack reported active — retrying the parked replication event")
    Process.send_after(self(), {:process_event, event, @max_event_retry_time}, @event_retry_delay)
    {:noreply, %{state | wait_for_active_ref: nil}}
  end

  # The wait ended without the stack becoming active: no status monitor to ask, or it died while
  # the event was parked. Retry anyway. If the stack is still not ready the handler answers
  # :not_ready again and the event re-parks against the restarted monitor, which is a bounded loop;
  # staying parked is a socket paused and a slot pinned until the process is restarted by hand.
  def handle_info(
        {{Electric.StatusMonitor, ref}, {:error, :not_running}},
        %State{wait_for_active_ref: {ref, event}} = state
      ) do
    Logger.warning("No status monitor to wait on — retrying the parked replication event")
    Process.send_after(self(), {:process_event, event, @max_event_retry_time}, @event_retry_delay)
    {:noreply, %{state | wait_for_active_ref: nil}}
  end

  def handle_info(
        {{Electric.StatusMonitor, :down}, ref, :process, _pid, _reason},
        %State{wait_for_active_ref: {ref, event}} = state
      ) do
    Logger.warning("Status monitor went down — retrying the parked replication event")
    Process.send_after(self(), {:process_event, event, @max_event_retry_time}, @event_retry_delay)
    {:noreply, %{state | wait_for_active_ref: nil}}
  end

  # Stale or unexpected StatusMonitor notification (e.g. after a retry already
  # succeeded and cleared wait_ref). Discard silently.
  def handle_info({{Electric.StatusMonitor, ref}, _result}, state) do
    Process.demonitor(ref, [:flush])
    {:noreply, state}
  end

  def handle_info({{Electric.StatusMonitor, :down}, _ref, :process, _pid, _reason}, state) do
    {:noreply, state}
  end

  # Async event handler replied :ok. Acks are strictly FIFO — the collector is a single
  # GenServer, so replies arrive in dispatch order. A reply for anything but the head of
  # the in-flight queue means that assumption broke, and the only safe move is to crash
  # and re-stream from the slot's confirmed position.
  def handle_info({ref, :ok}, %State{} = state) when is_reference(ref) do
    cond do
      MapSet.member?(state.stale_refs, ref) ->
        # A deferred ack for an event the pipeline already collapsed and replayed —
        # the collector outlived the failure that triggered the collapse. Drop it.
        {:noreply, %{state | stale_refs: MapSet.delete(state.stale_refs, ref)}}

      match?({{:value, {^ref, _, _, _}}, _}, :queue.out(state.in_flight)) ->
        {{:value, {^ref, event, _time_remaining, _start_time}}, rest} =
          :queue.out(state.in_flight)

        Process.demonitor(ref, [:flush])

        state = %{
          state
          | in_flight: rest,
            in_flight_count: state.in_flight_count - 1,
            pipeline_healthy?: true,
            retry_log_after: nil
        }

        {acks, state} = acknowledge_transaction(event, state)
        state = update_flush_up_to_date(state)
        continue_after_ack(acks, state)

      true ->
        exit({:pipeline_ordering_violation, ref})
    end
  end

  # Async event handler replied with a recoverable error — collapse the pipeline to
  # sequential replay and retry once the stack is active. A live collector that has
  # processed an event never becomes :not_ready again, so this only fires before the
  # first successful ack, while dispatch is still sequential.
  def handle_info({ref, {:error, error}}, %State{} = state)
      when is_reference(ref) and error in [:not_ready, :connection_not_available] do
    if MapSet.member?(state.stale_refs, ref) do
      {:noreply, %{state | stale_refs: MapSet.delete(state.stale_refs, ref)}}
    else
      true = in_flight_member?(state, ref)

      Logger.warning(
        "Replication event rejected by the event handler (#{inspect(error)}); " <>
          "collapsing the pipeline and parking until the stack reports active. " <>
          "The socket stays paused for the duration."
      )

      {{event, remaining}, state} = collapse_in_flight(state, ref)
      wait_for_active_and_retry(event, remaining, state)
    end
  end

  # Async event handler crashed — collapse the pipeline to sequential replay and retry
  # the head with its remaining budget. Later in-flight events were queued in the dead
  # collector's mailbox and are lost with it; they follow from the replay queue in WAL
  # order once the head recovers.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = state) do
    true = in_flight_member?(state, ref)
    {{event, remaining}, state} = collapse_in_flight(state, ref)

    if remaining > 0 do
      state =
        log_retry_error(state, fn ->
          "Error processing replication event (#{remaining}ms retry budget left): " <>
            inspect(reason)
        end)

      Process.send_after(self(), {:process_event, event, remaining}, @event_retry_delay)
      {:noreply, state}
    else
      Logger.error("Exhausted retry budget processing replication event: " <> inspect(reason))

      exit(reason)
    end
  end

  # This callback is invoked when the connection process receives a shutdown signal.
  def handle_info({:EXIT, _pid, :shutdown}, _state) do
    Logger.debug("Replication client #{inspect(self())} received shutdown signal, stopping")
    {:disconnect, :shutdown}
  end

  # Some other exit reason we're not expecting: disconnect and shut down.
  def handle_info({:EXIT, _pid, reason}, _state) do
    {:disconnect, reason}
  end

  # The implementation of Postgrex.ReplicationConnection doesn't give us a convenient way to
  # check whether the START_REPLICATION_SLOT statement succeeded before switching the
  # connection into streaming mode. Returning {:query, "START_REPLICATION_SLOT ...", state}
  # works fine when the query result is an error: it is then passed to the handle_result()
  # callback. But if streaming starts without issues, a function clause error is encountered
  # inside Postgrex.ReplicationConnection because it expects the connection to already have
  # been switched into streaming mode by returning {:stream, "START_REPLICATION_SLOT ...", [], state}.
  #
  # Hence this function clause of `handle_data()` that notifies the connection manager about
  # successful streaming start as soon as it receives the first replication message from
  # Postgres.
  @impl true
  @spec handle_data(binary(), State.t()) ::
          {:noreply, State.t()}
          | {:noreply, list(binary()), State.t()}
          | {:noreply_and_pause, list(binary()), State.t()}
          | {:disconnect, term()}
  def handle_data(data, %State{step: :start_streaming} = state) do
    # Modify the state as if we've just seen a transaction so that in the future we have a
    # starting point to check how long the stream has been idle for.
    state = %{state | step: :streaming, last_seen_txn_timestamp: System.monotonic_time()}

    if state.replication_idle_timeout > 0 do
      :timer.send_interval(@idle_check_interval, :check_if_idle)
    end

    if state.replication_heartbeat_interval > 0 do
      :timer.send_interval(state.replication_heartbeat_interval, :send_heartbeat)
    end

    # Start a periodic keepalive timer. This sends StandbyStatusUpdate messages
    # to PostgreSQL at regular intervals, preventing wal_sender_timeout from
    # firing even when the socket is paused for backpressure.
    #
    # The interval is derived from PostgreSQL's wal_sender_timeout (queried during
    # connection setup): timeout/3 provides a safe margin, matching the heuristic
    # used by pg_recvlogical and other replication clients.
    keepalive_interval = keepalive_interval(state.wal_sender_timeout)
    :timer.send_interval(keepalive_interval, :send_keepalive)

    :timer.send_interval(@wedge_check_interval, :check_if_wedged)

    Logger.debug(
      "Keepalive interval set to #{keepalive_interval}ms (wal_sender_timeout=#{state.wal_sender_timeout}ms)"
    )

    if state.full_xid_source_positions? do
      refresh_provenance({:first_message, data}, state)
    else
      notify_seen_first_message(state)
      handle_data(data, state)
    end
  end

  def handle_data(<<@repl_msg_primary_keepalive, wal_end::64, _clock::64, reply>>, state) do
    Logger.debug(fn ->
      "Primary Keepalive: wal_end=#{wal_end} (#{Lsn.from_integer(wal_end)}) reply=#{reply}"
    end)

    in_transaction? = MessageConverter.in_transaction?(state.message_converter)

    # Publish the server's latest WAL position to consumers with materializer
    # dependencies. This covers silent-postgres periods where no transactions
    # arrive but buffered move-in results may be ready to splice.
    state = if in_transaction?, do: state, else: note_seen_wal(state, wal_end)

    case reply do
      1 when in_transaction? ->
        {:noreply, [encode_standby_status_update(state)], state}

      # if we are not in a transaction, advance the replication slot
      # with keepalives to avoid it getting filled with irrelevant changes, like
      # heartbeats from the database provider
      1 ->
        # PostgreSQL keepalives are exclusive boundaries; status encoding adds one.
        state = update_stored_wals(state, max(wal_end - 1, 0))
        {:noreply, [encode_standby_status_update(state)], state}

      0 when in_transaction? ->
        {:noreply, [], state}

      0 ->
        state = update_stored_wals(state, max(wal_end - 1, 0))
        {:noreply, [], state}
    end
  end

  def handle_data(
        <<@repl_msg_x_log_data, _::binary>> = data,
        %State{idle_drain: {ref, false}} = state
      ),
      do: handle_data(data, %{state | idle_drain: {ref, true}})

  def handle_data(
        <<@repl_msg_x_log_data, _wal_start::64, _server_wal_end::64, _clock::64, data::binary>>,
        %State{} = state
      ) do
    case Decoder.decode(data) do
      # Logical decoding messages only arrive when heartbeats are on. Any of them, ours or
      # another producer's, proves the stream is still being read; none carries row changes.
      %Message{} -> {:noreply, %{state | last_seen_txn_timestamp: System.monotonic_time()}}
      %Begin{} = begin when state.full_xid_source_positions? -> convert_begin(begin, state)
      message -> convert_message(message, state)
    end
  end

  # Disconnecting releases the replication lock, and the next holder validates the shape catalog as
  # it finds it, so the queued catalog writes are drained first. Another process drains them: this
  # one goes on answering Postgres meanwhile, or `wal_sender_timeout` could end the session, and
  # release the lock, while the drain waits out another process's lock on the catalog. It belongs
  # to no supervisor, so a restart of the shape processes cannot end it while the write buffer,
  # which they do not include, still commits: the disconnect waits for the buffer's answer, or for
  # the drain's own bounded wait to run out.
  defp start_idle_drain(state) do
    stack_id = state.stack_id

    {_pid, ref} =
      spawn_monitor(fn ->
        Logger.metadata(stack_id: stack_id)
        Electric.Connection.Manager.drain_shape_catalog(stack_id)
      end)

    %{state | idle_drain: {ref, false}}
  end

  defp idle_time(%State{last_seen_txn_timestamp: txn_ts}),
    do: System.convert_time_unit(System.monotonic_time() - txn_ts, :native, :millisecond)

  # Postgres 17 added the flush argument. Without it a non-transactional message waits for the
  # WAL writer, which flushes within `wal_writer_delay`, far inside any idle window.
  defp heartbeat_query(pg_version) when is_integer(pg_version) and pg_version >= 170_000,
    do: "SELECT pg_logical_emit_message(false, 'electric_heartbeat', '', true)"

  defp heartbeat_query(_pg_version),
    do: "SELECT pg_logical_emit_message(false, 'electric_heartbeat', '')"

  defp emit_heartbeat(pool, query) do
    case Postgrex.query(pool, query, [], timeout: 5_000) do
      {:ok, _result} ->
        :ok

      {:error, error} ->
        Logger.warning("Failed to emit a replication heartbeat: #{inspect(error)}")
    end
  catch
    kind, error ->
      Logger.warning(
        "Failed to emit a replication heartbeat: #{Exception.format(kind, error, __STACKTRACE__)}"
      )
  end

  defp convert_begin(begin, state) do
    transaction_lsn = Lsn.to_integer(begin.final_lsn)

    if transaction_lsn < state.replay_before_lsn do
      convert_message(begin, state)
    else
      case FullTransactionId.resolve(begin.xid, transaction_lsn, state.provenance_anchor) do
        {:ok, full_xid} ->
          convert_message(%{begin | full_xid: full_xid}, %{state | provenance_deadline: nil})

        :refresh ->
          refresh_provenance(begin, state)
      end
    end
  end

  defp refresh_provenance(pending, state) do
    now = System.monotonic_time(:millisecond)
    deadline = state.provenance_deadline || now + @provenance_timeout

    if now < deadline do
      reference = make_ref()

      Process.send_after(
        self(),
        {:fetch_provenance_anchor, reference},
        @provenance_coalescing_delay
      )

      {:noreply_and_pause, [],
       pause_socket(%{
         state
         | pending_provenance: pending,
           provenance_deadline: deadline,
           provenance_request: reference
       })}
    else
      {:disconnect, {:full_transaction_provenance_unavailable, :retained_interval_not_caught_up}}
    end
  end

  defp convert_message(msg, state) do
    case MessageConverter.convert(msg, state.message_converter) do
      {:error, reason} ->
        {:disconnect, {:irrecoverable_slot, reason}}

      {:buffering, converter} ->
        {:noreply, %{state | message_converter: converter}}

      {:ok, event, converter} ->
        state = %{state | message_converter: converter}
        dispatch_event(event, state)
    end
  end

  # Dispatch event processing asynchronously, keeping up to pipeline_window events in
  # flight before pausing the socket. With a window of 1 this is the classic
  # stop-and-wait: pause on every dispatch, resume on ack. With a larger window the
  # socket stays open while the collector works, so decode of the next fragment
  # overlaps fan-out of the current one. The gen_statem remains responsive to
  # handle_info messages (keepalive timer, flush_boundary_updated, EXIT signals)
  # throughout.
  #
  # The window only opens after the first successful ack (pipeline_healthy?): a
  # collector that has never acked can still reply :not_ready, and :mark_as_ready
  # interleaving between two windowed dispatches could get a later event processed
  # while an earlier one was rejected — reordering the WAL. Sequential-until-healthy
  # confines rejection to a lone in-flight event.
  #
  # acknowledge_transaction and the flush_up_to_date? recompute are intentionally
  # deferred to the ack path, preserving the original semantics where they only ran
  # after handle_event succeeded.
  defp dispatch_event(event, %State{replay: replay} = state) when replay != nil do
    # A collapse happened while buffered copies were still being delivered after a
    # resume: queue behind the replay so WAL order is preserved, and stay paused. In
    # journal mode the event is journaled (and a commit acked) before it queues.
    {acks, state} = journal_ingest(event, state)
    state = %{state | replay: :queue.in({event, @max_event_retry_time}, replay)}
    {:noreply_and_pause, acks, pause_socket(state)}
  end

  # Ack-after-journal: journal + ack at decode time, then dispatch within the window or
  # queue behind it. The window stops pausing the socket — ingest runs ahead of fan-out
  # and the slot advances at journal speed; only the in-RAM backlog cap pauses. When the
  # storage cannot host a journal the mode disables itself wholesale: half-journal flow
  # control without journal durability would advance nothing and change semantics.
  defp dispatch_event(event, %State{ack_after_journal?: true} = state) do
    {acks, state} = journal_ingest(event, state)

    cond do
      :queue.is_empty(state.journal_backlog) and window_open?(state) ->
        case start_event(event, @max_event_retry_time, state) do
          {:dispatched, state} ->
            journal_flow_control(acks, state)

          {:retry_scheduled, state} ->
            {:noreply_and_pause, acks, pause_socket(state)}
        end

      true ->
        state = %{
          state
          | journal_backlog: :queue.in(event, state.journal_backlog),
            journal_backlog_rows: state.journal_backlog_rows + event_rows(event)
        }

        journal_flow_control(acks, state)
    end
  end

  defp dispatch_event(event, state), do: dispatch_event_legacy(event, state)

  # A window of 1 is the pre-pipeline configuration and must stay behaviourally identical
  # to it: the handler runs from handle_info, so everything already in this process's
  # mailbox — the keepalive tick, a flush-boundary notification — is handled first.
  # Dispatching inline here would reorder the event ahead of them.
  defp dispatch_event_legacy(event, %State{pipeline_window: 1} = state) do
    send(self(), {:process_event, event, @max_event_retry_time})
    {:noreply_and_pause, [], pause_socket(state)}
  end

  defp dispatch_event_legacy(event, state) do
    state = mark_pending_flush(state, event)

    case start_event(event, @max_event_retry_time, state) do
      {:dispatched, state} ->
        if state.pipeline_healthy? and state.in_flight_count < state.pipeline_window do
          {:noreply, [], state}
        else
          {:noreply_and_pause, [], pause_socket(state)}
        end

      {:retry_scheduled, state} ->
        {:noreply_and_pause, [], pause_socket(state)}
    end
  end

  defp window_open?(%State{} = state) do
    state.in_flight_count == 0 or
      (state.pipeline_healthy? and state.in_flight_count < state.pipeline_window)
  end

  defp journal_flow_control(acks, %State{journal_backlog_rows: rows} = state)
       when rows >= @journal_backlog_max_rows,
       do: {:noreply_and_pause, acks, pause_socket(state)}

  defp journal_flow_control(acks, state), do: {:noreply, acks, state}

  defp journal_ingest(_event, %State{ack_after_journal?: false} = state), do: {[], state}

  defp journal_ingest(event, state) do
    state = ensure_journal(state)
    journal = TxnJournal.append_event!(state.journal, event)

    case event do
      %TransactionFragment{commit: commit, last_log_offset: last} when commit != nil ->
        journal = TxnJournal.sync!(journal, last.tx_offset)

        # The journal is the write boundary as well as the flush boundary here.
        # Reporting a flush position ahead of the written one makes this connection's
        # pg_stat_replication write/flush lag nonsensical — the telemetry an operator
        # reads to judge this very feature.
        state = %{
          state
          | journal: journal,
            flushed_wal: max(state.flushed_wal, last.tx_offset),
            received_wal: max(state.received_wal, last.tx_offset)
        }

        {[encode_standby_status_update(state)], state}

      %Relation{} ->
        # Synced so a crash finds it in stream order relative to acked commits.
        {[], %{state | journal: TxnJournal.sync!(journal)}}

      _ ->
        {[], %{state | journal: journal}}
    end
  end

  # Whether the storage can host a journal is fixed for the life of this process, so it is
  # settled here rather than at the first dispatched event. Deciding it lazily left a window --
  # the whole of a boot over a quiet publication -- in which every WAL guard keyed off `journal`
  # took the non-journal path and a keepalive acked the slot to the server's wal_end, past a
  # boundary the next boot then measured itself against.
  defp ensure_journal(%State{journal: journal} = state) when journal != nil, do: state

  defp ensure_journal(%State{journal_base_path: base_path} = state) do
    journal =
      TxnJournal.open_writer!(base_path)
      |> TxnJournal.adopt_generations!(base_path)

    %{state | journal: journal}
  end

  defp event_rows(%TransactionFragment{change_count: count}), do: count
  defp event_rows(_event), do: 0

  defp drain_journal_backlog(%State{} = state) do
    with true <- window_open?(state),
         {{:value, event}, rest} <- :queue.out(state.journal_backlog) do
      state = %{
        state
        | journal_backlog: rest,
          journal_backlog_rows: state.journal_backlog_rows - event_rows(event)
      }

      case start_event(event, @max_event_retry_time, state) do
        {:dispatched, state} -> drain_journal_backlog(state)
        {:retry_scheduled, state} -> state
      end
    else
      _ -> state
    end
  end

  # With the socket open, a primary keepalive can arrive while transactions are still
  # in flight, and update_stored_wals would report them flushed off a stale
  # flush_up_to_date? before their shapes have durably stored them. Drop the flag on
  # dispatch of a commit; the flush_boundary_updated path re-raises it once consumers
  # catch up. Scoped to windows > 1 so the default configuration keeps today's
  # semantics bit-exact.
  # Journal mode never drops flush_up_to_date?: a keepalive advancing flushed_wal between
  # transactions is always safe there — everything behind it is journal-durable and
  # replayable — so its dispatch path skips mark_pending_flush entirely.
  defp mark_pending_flush(%State{pipeline_window: 1} = state, _event), do: state

  defp mark_pending_flush(state, %TransactionFragment{commit: commit}) when commit != nil,
    do: %{state | flush_up_to_date?: false}

  defp mark_pending_flush(state, _event), do: state

  # Dispatch the event handler as a non-blocking $gen_call. The MFA returns a
  # monitor ref; the gen_statem returns immediately and handles the reply (or
  # :DOWN on crash) in handle_info. This keeps the gen_statem responsive to
  # keepalive timers while the handler processes the event.
  defp log_retry_error(%State{} = state, build_message) do
    now = System.monotonic_time(:millisecond)

    if is_nil(state.retry_log_after) or now >= state.retry_log_after do
      Logger.error(build_message.())
      %{state | retry_log_after: now + @retry_log_interval}
    else
      state
    end
  end

  defp start_event(event, time_remaining, state) do
    {m, f, args} = state.handle_event
    start_time = System.monotonic_time(:millisecond)

    try do
      ref = apply(m, f, [event | args])

      {:dispatched,
       %{
         state
         | in_flight: :queue.in({ref, event, time_remaining, start_time}, state.in_flight),
           in_flight_count: state.in_flight_count + 1
       }}
    catch
      kind, reason ->
        remaining = time_remaining - (System.monotonic_time(:millisecond) - start_time)

        if remaining > 0 do
          stacktrace = __STACKTRACE__

          state =
            log_retry_error(state, fn ->
              "Error dispatching replication event (#{remaining}ms retry budget left): " <>
                Exception.format(kind, reason, stacktrace)
            end)

          # The dispatch itself failed, so anything already in flight points at a dead
          # or unreachable collector: collapse it in front of this event and retry the
          # head — this event when the pipeline was empty. The budget goes in undecremented
          # with its original start, because collapse_in_flight/2 subtracts the elapsed time.
          state = %{
            state
            | in_flight: :queue.in({nil, event, time_remaining, start_time}, state.in_flight),
              in_flight_count: state.in_flight_count + 1
          }

          {{head_event, head_remaining}, state} = collapse_in_flight(state)

          Process.send_after(
            self(),
            {:process_event, head_event, head_remaining},
            @event_retry_delay
          )

          {:retry_scheduled, state}
        else
          Logger.error(
            "Exhausted retry budget dispatching replication event: " <>
              Exception.format(kind, reason, __STACKTRACE__)
          )

          :erlang.raise(kind, reason, __STACKTRACE__)
        end
    end
  end

  # Tear the pipeline down to sequential replay. Returns the head event with its
  # remaining budget; every later in-flight event moves to the replay queue, ahead of
  # anything already queued there, and is re-dispatched in WAL order as acks come back.
  # The collector deduplicates redelivered fragments on last_processed_offset, so
  # events that did complete before the failure are dropped harmlessly on replay.
  # `answered` names the refs that can no longer produce a reply: one whose message is
  # being handled right now. A `:DOWN` proves only its own collector gone: the collector
  # is resolved by name on every dispatch, so a window wider than one can hold refs
  # pointing at a process that outlives the crash and still answers. Forgetting those
  # trades a bounded set for an ordering exit that drops the replication connection.
  defp collapse_in_flight(state, answered \\ nil)

  defp collapse_in_flight(%State{} = state, answered) do
    entries = :queue.to_list(state.in_flight)

    Enum.each(entries, fn {ref, _event, _time_remaining, _start_time} ->
      if is_reference(ref), do: Process.demonitor(ref, [:flush])
    end)

    # demonitor flushes :DOWNs but not replies: a live collector finalizing deferred
    # work can still ack these refs later. Remember them so those acks are dropped
    # instead of tripping the ordering check.
    stale_refs =
      Enum.reduce(entries, state.stale_refs, fn {ref, _event, _time_remaining, _start_time},
                                                acc ->
        if is_reference(ref) and ref != answered, do: MapSet.put(acc, ref), else: acc
      end)

    state = %{state | stale_refs: stale_refs}

    [{_ref, head_event, head_budget, head_start} | tail] = entries
    head_remaining = head_budget - (System.monotonic_time(:millisecond) - head_start)

    replay_tail =
      :queue.from_list(
        Enum.map(tail, fn {_ref, event, _time_remaining, _start_time} ->
          {event, @max_event_retry_time}
        end)
      )

    # The journal backlog moves into the replay queue so a single queue owns WAL order.
    # Leaving it behind would strand it: once replay is armed, every newly decoded event
    # queues there, and the replay drains to exhaustion before the backlog is touched —
    # delivering newer transactions first, which the collector then drops as duplicates
    # while the slot and the journal GC have already advanced past them. In-flight is
    # always older than the backlog, and the backlog older than anything already queued
    # for replay (its dispatch clause is unreachable while replay != nil).
    journal_backlog =
      :queue.from_list(
        Enum.map(:queue.to_list(state.journal_backlog), &{&1, @max_event_retry_time})
      )

    replay =
      :queue.join(replay_tail, :queue.join(journal_backlog, state.replay || :queue.new()))

    {{head_event, head_remaining},
     %{
       state
       | in_flight: :queue.new(),
         in_flight_count: 0,
         pipeline_healthy?: false,
         journal_backlog: :queue.new(),
         journal_backlog_rows: 0,
         replay: replay
     }}
  end

  defp in_flight_member?(%State{} = state, ref) do
    state.in_flight
    |> :queue.to_list()
    |> Enum.any?(fn {r, _event, _time_remaining, _start_time} -> r == ref end)
  end

  # After a successful ack: drain the replay queue one event at a time while it exists,
  # and only reopen the socket once the replay is done and the window has room.
  defp continue_after_ack(acks, %State{replay: replay} = state) when replay != nil do
    case :queue.out(replay) do
      {{:value, {event, time_remaining}}, rest} ->
        case start_event(event, time_remaining, %{state | replay: rest}) do
          {:dispatched, state} -> {:noreply, acks, state}
          {:retry_scheduled, state} -> {:noreply, acks, state}
        end

      {:empty, _} ->
        resume_if_paused(acks, %{state | replay: nil})
    end
  end

  defp continue_after_ack(acks, state), do: resume_if_paused(acks, state)

  # Pausing is how the client applies backpressure, and the socket reopens only when downstream
  # acks -- so the pause outlasts anything that can observe it if downstream stops. Stamping when
  # it began is what makes an unbounded pause measurable, whatever holds it: an event in flight,
  # the journal backlog, or a boot replay.
  defp pause_socket(%State{socket_paused?: true} = state), do: state

  defp pause_socket(%State{} = state),
    do: %{state | socket_paused?: true, paused_since: System.monotonic_time(:millisecond)}

  defp resume_socket(%State{} = state),
    do: %{state | socket_paused?: false, paused_since: nil}

  defp resume_if_paused(acks, %State{pending_provenance: pending} = state)
       when not is_nil(pending),
       do: {:noreply, acks, state}

  defp resume_if_paused(acks, %State{} = state)
       when state.replay != nil or state.wait_for_active_ref != nil,
       do: {:noreply, acks, state}

  defp resume_if_paused(acks, %State{ack_after_journal?: true} = state) do
    state = state |> drain_journal_backlog() |> maybe_broadcast_pending_seen_wal()

    if state.socket_paused? and state.replay == nil and
         state.journal_backlog_rows < @journal_backlog_max_rows do
      {:noreply_and_resume, acks, resume_socket(state)}
    else
      {:noreply, acks, state}
    end
  end

  defp resume_if_paused(acks, %State{socket_paused?: true} = state)
       when state.in_flight_count < state.pipeline_window do
    state = maybe_broadcast_pending_seen_wal(state)
    {:noreply_and_resume, acks, resume_socket(state)}
  end

  defp resume_if_paused(acks, state),
    do: {:noreply, acks, maybe_broadcast_pending_seen_wal(state)}

  # Downstream returned :not_ready — subscribe to StatusMonitor for notification
  # when the stack becomes active, then retry. This replaces the old blocking
  # wait_until_active(timeout: :infinity) call with an async notification.
  # The keepalive timer prevents wal_sender_timeout during the wait.
  # The remaining budget is intentionally discarded — a fresh @max_event_retry_time
  # is used after the stack becomes active, matching the old apply_with_retries
  # behavior which reset the retry timer after wait_until_active returned.
  defp wait_for_active_and_retry(event, _remaining, state) do
    ref = Electric.StatusMonitor.wait_until_async(state.stack_id, :active)
    {:noreply, %{state | wait_for_active_ref: {ref, event}}}
  end

  defp acknowledge_transaction(%TransactionFragment{commit: nil}, state), do: {[], state}

  defp acknowledge_transaction(%TransactionFragment{lsn: lsn, commit: commit}, state) do
    if Sampler.sample_metrics?() do
      alias Electric.Replication.Changes.Commit

      OpenTelemetry.execute(
        [:electric, :postgres, :replication, :transaction_received],
        %{
          monotonic_time: System.monotonic_time(),
          receive_lag: Commit.calculate_final_receive_lag(commit, System.monotonic_time()),
          bytes: commit.transaction_size,
          count: 1,
          operations: commit.txn_change_count
        },
        %{stack_id: state.stack_id}
      )
    end

    state =
      %{
        state
        | last_seen_txn_lsn: lsn,
          last_seen_txn_timestamp: System.monotonic_time()
      }
      |> update_received_wal(Lsn.to_integer(lsn))

    {[encode_standby_status_update(state)], state}
  end

  defp acknowledge_transaction(%Relation{}, state), do: {[], state}

  # flush_up_to_date? is derived state: the client is up to speed exactly when
  # the flush tracker has confirmed the last seen transaction. Recomputed
  # whenever either side moves — after acknowledge_transaction/2 advances
  # last_seen_txn_lsn and when a :flush_boundary_updated notification advances
  # last_confirmed_flush_lsn.
  #
  # Both call sites matter: for a transaction that affects no shapes, the tracker's
  # confirmation is emitted during event processing and reaches this process before the
  # handler's reply that updates last_seen_txn_lsn.
  defp update_flush_up_to_date(state) do
    confirmed? = Lsn.from_integer(state.last_confirmed_flush_lsn) == state.last_seen_txn_lsn
    %{state | flush_up_to_date?: confirmed? and not commit_in_flight?(state)}
  end

  # last_seen_txn_lsn is the last ACKED commit, so with the socket left open by a window
  # larger than 1 the recompute would re-arm the flag while a later dispatched commit is
  # still unacked — and a primary keepalive would then ack WAL no shape has stored.
  # in_flight is not the whole of what is undelivered: a collapse moves the tail into
  # `replay` and parks the head against the status monitor, and neither of the paths that
  # re-dispatch from there passes through mark_pending_flush/2. The predicate has to name
  # every place a decoded commit can sit. Window 1 pauses the socket on every dispatch and
  # journal mode acks off the journal boundary, so both keep their short-circuits.
  defp commit_in_flight?(%State{pipeline_window: 1}), do: false

  defp commit_in_flight?(%State{ack_after_journal?: true}), do: false

  defp commit_in_flight?(state) do
    state.in_flight_count > 0 or state.replay != nil or state.wait_for_active_ref != nil
  end

  defp encode_standby_status_update(state) do
    # Postgres reads write >= flush >= apply from a standby status update and derives its
    # write/flush lag columns from the gap. A flush boundary can advance flushed_wal past
    # received_wal (a keepalive moved the journal boundary without receiving new WAL), so the
    # reported write position is clamped up to the flush position rather than emit flush > write
    # and make the very lag metric an operator reads to judge the journal feature incoherent.
    write_wal = max(state.received_wal, state.flushed_wal)

    Logger.debug(fn ->
      "Standby status update: received_wal=#{Lsn.from_integer(write_wal)}, flushed_wal=#{Lsn.from_integer(state.flushed_wal)}"
    end)

    <<
      @repl_msg_standby_status_update,
      write_wal + 1::64,
      state.flushed_wal + 1::64,
      state.flushed_wal + 1::64,
      current_time()::64,
      0
    >>
  end

  # Derive keepalive interval from PostgreSQL's wal_sender_timeout.
  # Uses min(timeout/3, 15s): timeout/3 provides a safe margin for low timeouts,
  # while the 15s cap ensures responsiveness even if wal_sender_timeout is very
  # high or changes on the source PG after we've connected.
  defp keepalive_interval(0), do: @max_keepalive_interval

  defp keepalive_interval(wal_sender_timeout_ms),
    do: min(div(wal_sender_timeout_ms, 3), @max_keepalive_interval)

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time(), do: System.os_time(:microsecond) - @epoch

  # The last-seen broadcast carries a promise: every transaction at or below the
  # position has already been fanned out to shape consumers — exactly the barrier a
  # buffered move-in splices against. With a window of 1 the socket pauses for every
  # event, so by the time a keepalive is decoded fan-out has caught up and the promise
  # holds for free. The pipelined modes decode ahead of fan-out; publishing a keepalive
  # position at decode time would both release move-in splices ahead of commits their
  # snapshot already covers and let the collector's later commit broadcasts bury the
  # position, so it waits here until the pipeline drains.
  #
  # The collector's boot-time journal replay breaks the same promise from the other
  # side: it fans out transactions this process never dispatched, in every mode — a
  # leftover journal replays even with the flag now off. The collector flags that
  # replay in the LsnTracker table before this process can decode a keepalive and
  # clears it when the replay completes, and positions are held here while it is up.
  # A boot with no journal never sets the flag, keeping the default path exact.
  defp note_seen_wal(%State{} = state, wal_end) do
    wal_end = max(state.pending_seen_wal || 0, wal_end)

    if publishable_seen_wal?(state) do
      LsnTracker.broadcast_last_seen_lsn(state.stack_id, wal_end)
      %{state | pending_seen_wal: nil}
    else
      %{state | pending_seen_wal: wal_end}
    end
  end

  defp publishable_seen_wal?(%State{} = state) do
    (not pipelined_ingest?(state) or pipeline_drained?(state)) and
      not LsnTracker.journal_replay_pending?(state.stack_id)
  end

  defp pipelined_ingest?(%State{ack_after_journal?: true}), do: true
  defp pipelined_ingest?(%State{pipeline_window: window}), do: window > 1

  defp pipeline_drained?(%State{} = state) do
    state.in_flight_count == 0 and state.replay == nil and
      state.wait_for_active_ref == nil and
      :queue.is_empty(state.journal_backlog)
  end

  defp maybe_broadcast_pending_seen_wal(%State{pending_seen_wal: nil} = state), do: state

  defp maybe_broadcast_pending_seen_wal(%State{} = state) do
    if publishable_seen_wal?(state) do
      LsnTracker.broadcast_last_seen_lsn(state.stack_id, state.pending_seen_wal)
      %{state | pending_seen_wal: nil}
    else
      state
    end
  end

  # In journal mode the slot must never be acked past what the journal covers, even over WAL
  # that carried nothing for us: the boot guard's only way to tell a lost journal from an
  # idle one is that the two positions line up. Idle keepalives may still advance the slot,
  # but only by first widening the journal's coverage with a durable idle-boundary marker
  # (maybe_advance_idle_boundary below), so the two positions keep lining up at boot.
  defp journal_free_wal(%State{ack_after_journal?: true} = state), do: state.flushed_wal

  defp journal_free_wal(%State{} = state), do: state.received_wal

  defp update_stored_wals(%State{ack_after_journal?: true} = state, wal) do
    %{state | received_wal: max(state.received_wal, wal)}
    |> maybe_advance_idle_boundary(wal)
  end

  defp update_stored_wals(
         %{
           received_wal: received_wal,
           flushed_wal: flushed_wal,
           flush_up_to_date?: flush_up_to_date?
         } = state,
         wal
       ) do
    received_wal = max(received_wal, wal)
    flushed_wal = if flush_up_to_date?, do: max(flushed_wal, wal), else: flushed_wal

    %{state | received_wal: received_wal, flushed_wal: flushed_wal}
  end

  # WAL that carries nothing for the publication — other databases on the instance,
  # unpublished-table churn — never produces a journal commit, so the boundary and the
  # slot ack pinned to it would sit still while retained WAL grows without limit on a
  # quiet stack.
  # Once the pipeline is fully drained there is nothing the journal still
  # owes anyone below wal_end, so record wal_end in the journal as a durable idle
  # boundary and ack to it. Keepalives arrive on the wal_sender_timeout cadence, so the
  # extra datasync is rare.
  defp maybe_advance_idle_boundary(%State{} = state, wal) do
    if wal > state.flushed_wal and pipeline_drained?(state) and
         (state.journal != nil or is_binary(state.journal_base_path)) and
         LsnTracker.journal_replay_settled?(state.stack_id) do
      state = ensure_journal(state)

      journal =
        state.journal
        |> TxnJournal.append_event!({:idle_slot_boundary, wal})
        |> TxnJournal.sync!(wal)

      %{state | journal: journal, flushed_wal: wal}
    else
      state
    end
  end

  defp update_received_wal(state, wal) when is_number(wal) and wal >= state.received_wal,
    do: %{state | received_wal: wal}

  defp update_received_wal(state, wal) when is_number(wal), do: state

  # A promotion the connection manager read as a continuation keeps the stream, so the
  # generation names the timeline the stream started on rather than the connected one.
  defp source_generation!(
         %State{database_oid: database_oid, slot_incarnation: slot_incarnation} = state
       )
       when is_binary(database_oid) and is_binary(slot_incarnation) do
    [
      state.system_identifier,
      stream_timeline_id(state),
      state.database,
      database_oid,
      slot_incarnation
    ]
    |> Jason.encode!()
    |> Base.url_encode64(padding: false)
  end

  defp stream_timeline_id(%State{persistent_kv: nil} = state), do: state.timeline_id

  defp stream_timeline_id(%State{} = state) do
    Electric.Timeline.stream_timeline_id(
      [persistent_kv: state.persistent_kv, stack_id: state.stack_id],
      {state.system_identifier, state.timeline_id}
    )
  end

  defp notify_connection_opened(%State{connection_manager: manager} = state) do
    :ok = Electric.Connection.Manager.replication_client_started(manager)
    state
  end

  defp notify_system_identified(%State{connection_manager: manager} = state, info) do
    :ok = Electric.Connection.Manager.pg_system_identified(manager, info)
    state
  end

  defp notify_timeline_history_obtained(%State{connection_manager: manager} = state, history) do
    :ok =
      Electric.Connection.Manager.pg_timeline_history_obtained(manager, %{
        history: history,
        journal_base_path: state.journal_base_path
      })

    state
  end

  defp notify_pg_info_obtained(%State{connection_manager: manager} = state, pg_info) do
    :ok = Electric.Connection.Manager.pg_info_obtained(manager, pg_info)
    state
  end

  defp notify_lock_acquisition_error(%State{connection_manager: manager} = state, error) do
    :ok = Electric.Connection.Manager.replication_client_lock_acquisition_failed(manager, error)
    state
  end

  defp notify_lock_acquired(%State{connection_manager: manager} = state) do
    :ok = Electric.Connection.Manager.replication_client_lock_acquired(manager)
    state
  end

  defp notify_created_new_slot(%State{connection_manager: manager} = state) do
    :ok = Electric.Connection.Manager.replication_client_created_new_slot(manager)
    state
  end

  defp notify_ready_to_stream(%State{connection_manager: manager} = state) do
    :ok = Electric.Connection.Manager.replication_client_ready_to_stream(manager)
    state
  end

  defp notify_seen_first_message(%State{connection_manager: manager} = state) do
    :ok = Electric.Connection.Manager.replication_client_streamed_first_message(manager)
    state
  end
end
