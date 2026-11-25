defmodule Electric.Postgres.ReplicationClient.ConnectionSetup do
  @moduledoc """
  This module encapsulates the initial setup of a replication connection opened by
  `Electric.Postgres.ReplicationClient`.

  A state machine is implemented to run a series of SQL queries prior to switching the
  connection into the logical streaming mode. This helps keep the main `ReplicationClient`
  module focused on the handling of logical messages.
  """
  alias Electric.Replication.TxnJournal
  alias Electric.Utils
  alias Electric.Postgres.ReplicationClient.State
  alias Electric.Postgres.ReplicationClient.SlotIncarnation
  alias Electric.Postgres.Lsn
  alias Electric.StatusMonitor
  alias Electric.Timeline

  require Logger

  @type state :: Electric.Postgres.ReplicationClient.State.t()
  @type step :: Electric.Postgres.ReplicationClient.step()
  @type extra_info :: term
  @type callback_return ::
          {:noreply, state}
          | {:query, iodata, state}
          | {:stream, iodata, Electric.Postgres.ReplicationConnection.stream_opts(), state}
  @type query_result :: [Postgrex.Result.t()] | Postgrex.Error.t()

  # The entrypoint to the connection setup that picks the first step to run and returns the
  # `{:query, ...}` tuple for it.
  @spec start(state) :: callback_return
  def start(%{step: :connected} = state) do
    state = resolve_journal_support(state)
    next_step = next_step(state)
    query_for_step(next_step, %{state | step: next_step})
  end

  # Whether the storage can host a journal is fixed for the life of a connection, so it is
  # settled once, here, before anything can arrive on it. Deciding it at the first dispatched
  # event left a window -- the whole of a boot over a quiet publication -- in which every WAL
  # guard keyed off the journal took the non-journal path, and a keepalive acked the slot to
  # the server's wal_end, past a boundary the next boot then measured itself against. Process
  # init is too early for it: the stack's storage config is not registered yet.
  defp resolve_journal_support(%State{ack_after_journal?: false} = state), do: state

  defp resolve_journal_support(%State{} = state) do
    case storage_config(state.stack_id) do
      {Electric.ShapeCache.PureFileStorage, %{base_path: base_path}} ->
        Logger.notice("Acking the replication slot at the transaction journal boundary")
        %{state | journal_base_path: base_path}

      other ->
        Logger.warning(
          "Ack-after-journal requested but storage does not support it: #{inspect(other)}"
        )

        %{state | ack_after_journal?: false}
    end
  end

  # A client started without a stack around it is indistinguishable here from a stack whose
  # storage cannot host a journal: either way the mode disables itself.
  defp storage_config(stack_id) do
    Electric.StackConfig.lookup(stack_id, Electric.ShapeCache.Storage)
  rescue
    ArgumentError -> nil
  end

  # Process the result of executing the query, pick the next step and return the `{:query, ...}`
  # tuple for it.
  @spec process_query_result(query_result, state) ::
          {step, step, extra_info, state, callback_return}
  def process_query_result(result, %{step: step} = state) do
    {extra_info, state} =
      case dispatch_query_result(step, result, state) do
        {extra_info, %State{} = state} -> {extra_info, state}
        %State{} = state -> {nil, state}
      end

    next_step = next_step(state)
    {step, next_step, extra_info, state, query_for_step(next_step, %{state | step: next_step})}
  end

  # Instruct `Postgrex.ReplicationConnection` to switch the connection into the logical
  # streaming mode.
  @spec start_streaming(state) :: callback_return
  def start_streaming(%{step: :ready_to_stream} = state) do
    Logger.debug("ReplicationClient step: start_streaming")
    query_for_step(:start_streaming, %{state | step: :start_streaming})
  end

  ###

  defp identify_system_query(state) do
    Logger.debug("ReplicationClient step: identify_system_query")
    query = "IDENTIFY_SYSTEM"
    {:query, query, state}
  end

  def identify_system_result([%Postgrex.Result{command: :identify} = result], state) do
    # [db] postgres:postgres=> IDENTIFY_SYSTEM;
    #       systemid       │ timeline │  xlogpos  │  dbname
    # ─────────────────────┼──────────┼───────────┼──────────
    #  7506979529870965272 │        1 │ 0/220AE10 │ postgres
    # (1 row)
    [[systemid, timeline, xlogpos, dbname]] = result.rows

    {%{
       system_identifier: systemid,
       timeline_id: timeline,
       current_wal_flush_lsn: xlogpos
     }, %{state | system_identifier: systemid, timeline_id: timeline, database: dbname}}
  end

  ###

  # A promotion starts a new timeline whose history file names each ancestor timeline and where
  # it ended. The connection manager reads it to tell a promotion that kept everything Electric
  # took from the old timeline from one that rewound it. Any failure only costs that reading:
  # without a history the manager treats the change as a rewind, as it always has.
  defp timeline_history_query(state) do
    Logger.debug("ReplicationClient step: query_timeline_history")
    {:query, "TIMELINE_HISTORY #{history_timeline_id(state)}", state}
  end

  defp timeline_history_result([%Postgrex.Result{rows: [[_filename, content]]}], state)
       when is_binary(content),
       do: {Electric.Timeline.parse_history(content), state}

  defp timeline_history_result([%Postgrex.Result{} = result], state),
    do: {{:error, {:unexpected_result, result.rows}}, state}

  defp timeline_history_result(%Postgrex.Error{} = error, state),
    do: {{:error, Exception.message(error)}, state}

  # Timeline 1 has no history file: nothing can be promoted onto it.
  defp history_timeline_id(%State{keep_shapes_on_promotion?: true, timeline_id: timeline_id}) do
    case Integer.parse(to_string(timeline_id)) do
      {timeline_id, ""} when timeline_id > 1 -> timeline_id
      _ -> nil
    end
  end

  defp history_timeline_id(%State{}), do: nil

  ###

  defp pg_info_query(state) do
    Logger.debug("ReplicationClient step: pg_info_query")

    # pg_settings.setting returns the value in base units (milliseconds for
    # wal_sender_timeout) as a plain integer string, regardless of how the
    # user configured it (e.g. "60s", "1min"). This avoids interval parsing.
    #
    # The database OID tells a dropped and recreated database apart from the one it replaced,
    # which IDENTIFY_SYSTEM cannot: both report the same name.
    query = """
    SELECT
      current_setting('server_version_num') server_version_num,
      pg_backend_pid() pg_backend_pid,
      (SELECT setting FROM pg_settings WHERE name = 'wal_sender_timeout') wal_sender_timeout_ms,
      (SELECT oid FROM pg_database WHERE datname = current_database())::text database_oid
    """

    {:query, query, state}
  end

  defp pg_info_result([%Postgrex.Result{} = result], state) do
    %{rows: [[version_str, backend_pid_str, wal_sender_timeout_str, database_oid]]} = result
    version_num = String.to_integer(version_str)
    backend_pid_num = String.to_integer(backend_pid_str)
    wal_sender_timeout_ms = String.to_integer(wal_sender_timeout_str)

    {%{server_version_num: version_num, pg_backend_pid: backend_pid_num},
     %{
       state
       | pg_version: version_num,
         backend_pid: backend_pid_num,
         wal_sender_timeout: wal_sender_timeout_ms,
         database_oid: database_oid
     }}
  end

  ###

  # Try to acquire the connection lock based on the replication slot
  # before configuring it and subsequently starting replication, to ensure
  # a single active sync service is connected to Postgres per slot.
  defp acquire_lock_query(%State{slot_name: lock_name} = state) do
    Logger.debug("ReplicationClient step: acquire_lock")
    Logger.notice("Acquiring lock from postgres with name #{lock_name}")
    query = "SELECT pg_advisory_lock(hashtext('#{lock_name}'))"
    {:query, query, state}
  end

  defp acquire_lock_result([%Postgrex.Result{}], state) do
    Logger.notice("Lock acquired from postgres with name #{state.slot_name}")
    {:lock_acquired, %{state | lock_acquired?: true}}
  end

  # Statement timeouts can be retried: databases with a low statement timeout hit them while
  # waiting for the lock. A cancel request shares the code (messages are translated into
  # `lc_messages`, so only codes are matched in this module) and is retried too. A database
  # shutdown has codes of its own and still fails loudly.
  defp acquire_lock_result(
         %Postgrex.Error{postgres: %{code: :query_canceled, pg_code: "57014"}} = error,
         state
       ) do
    Logger.warning("Retrying lock acquisition for #{state.slot_name} due to #{inspect(error)}.")
    {{:lock_acquisition_failed, error}, %{state | lock_acquired?: false}}
  end

  defp acquire_lock_result(%Postgrex.Error{} = error, _state) do
    # Unexpected error, fail loudly.
    raise error
  end

  ###

  @pg18_version 180_000
  defp create_publication_query(state) do
    Logger.debug("ReplicationClient step: create_publication_query")
    # We're creating an "empty" publication here because synced tables are added to it
    # elsewhere. See `Electric.Replication.PublicationManager`.
    query = "CREATE PUBLICATION #{Utils.quote_name(state.publication_name)}"

    query =
      if state.pg_version >= @pg18_version,
        do: query <> " WITH (publish_generated_columns = stored)",
        else: query

    {:query, query, state}
  end

  # Successfully created the publication.
  defp create_publication_result([%Postgrex.Result{}], state) do
    # At this point, even if there is a replication slot already, we have to drop it and create
    # a new one, while also invalidating existing shapes. See
    # https://github.com/electric-sql/electric/issues/2692 for details.
    %{state | publication_owner?: true, recreate_slot?: true}
  end

  defp create_publication_result(%Postgrex.Error{} = error, state) do
    case error.postgres do
      %{code: :duplicate_object, pg_code: "42710"} ->
        # Publication already exists. It is checked before use: someone may have changed it.
        %{state | publication_owner?: true}

      %{code: :insufficient_privilege, pg_code: "42501"} ->
        {:insufficient_privilege, %{state | publication_owner?: false}}

      _ ->
        # Unexpected error, fail loudly.
        raise error
    end
  end

  ###

  defp check_if_publication_exists_query(state) do
    query =
      "SELECT * FROM pg_publication WHERE pubname = #{Utils.quote_string(state.publication_name)}"

    {:query, query, state}
  end

  defp check_if_publication_exists_result([%Postgrex.Result{} = result], state) do
    publication =
      case result do
        %{num_rows: 1, columns: cols, rows: [row]} ->
          Enum.zip(cols, row) |> Map.new()

        _ ->
          raise Electric.DbConfigurationError.publication_missing(state.publication_name)
      end

    case publication do
      %{"pubinsert" => "t", "pubupdate" => "t", "pubdelete" => "t", "pubtruncate" => "t"} ->
        state

      # In manual publishing mode the operator owns the publication, so it is reported instead.
      _ when state.publication_owner? and not state.manual_table_publishing? ->
        Logger.warning(
          "Publication #{inspect(state.publication_name)} does not publish all of INSERT, UPDATE, " <>
            "DELETE and TRUNCATE. Restoring them and recreating the replication slot, " <>
            "which invalidates all shapes"
        )

        %{state | repair_publication?: true}

      _ ->
        raise Electric.DbConfigurationError.publication_missing_operations(state.publication_name)
    end
  end

  defp check_if_publication_exists_result(%Postgrex.Error{} = error, _state) do
    # Unrecoverable error.
    raise error
  end

  ###

  # The publication dropped changes of the missing operations, so existing shapes may lack them.
  # The slot is dropped first and created again after the repair, which invalidates all shapes
  # as a new publication does. In that order, stopping in between still leaves no old slot to
  # reuse.
  defp repair_publication_query(state) do
    Logger.debug("ReplicationClient step: repair_publication")

    query =
      "ALTER PUBLICATION #{Utils.quote_name(state.publication_name)} " <>
        "SET (publish = 'insert, update, delete, truncate')"

    {:query, query, state}
  end

  defp repair_publication_result([%Postgrex.Result{}], state) do
    %{state | repair_publication?: false}
  end

  defp repair_publication_result(%Postgrex.Error{} = error, state) do
    case error.postgres do
      %{code: :insufficient_privilege} ->
        raise Electric.DbConfigurationError.publication_missing_operations(state.publication_name)

      _ ->
        # Unexpected error, fail loudly.
        raise error
    end
  end

  ###

  defp drop_slot_query(%State{slot_name: slot_name} = state) do
    Logger.debug("ReplicationClient step: drop_slot")
    query = "SELECT pg_drop_replication_slot('#{slot_name}')"
    {:query, query, state}
  end

  defp drop_slot_result([%Postgrex.Result{}], state) do
    state
  end

  defp drop_slot_result(%Postgrex.Error{} = error, state) do
    case error.postgres do
      %{code: :undefined_object, pg_code: "42704"} ->
        # No slot with such name exists, proceed to the next step.
        state

      _ ->
        # Unexpected error, fail loudly.
        raise error
    end
  end

  ###

  @slot_options "LOGICAL pgoutput NOEXPORT_SNAPSHOT"
  @temp_slot_options "TEMPORARY #{@slot_options}"

  defp create_slot_query(%State{slot_name: slot_name, slot_temporary?: true} = state) do
    query = "CREATE_REPLICATION_SLOT #{Utils.quote_name(slot_name)} #{@temp_slot_options}"
    {:query, query, prepare_slot_incarnation(state)}
  end

  defp create_slot_query(%State{slot_name: slot_name} = state) do
    Logger.debug("ReplicationClient step: create_slot")
    query = "CREATE_REPLICATION_SLOT #{Utils.quote_name(slot_name)} #{@slot_options}"
    {:query, query, prepare_slot_incarnation(state)}
  end

  # Arm the purge before CREATE_REPLICATION_SLOT. A probe that found an existing slot takes
  # the reuse path and never sends that command, so its nonce write cannot owe a new purge.
  defp prepare_slot_incarnation(%State{} = state, creating? \\ true) do
    purge_on_create =
      if creating? and state.persistent_kv do
        if Timeline.load_timeline(stack_id: state.stack_id, persistent_kv: state.persistent_kv) do
          :required
        else
          :if_previous_timeline
        end
      end

    %{
      state
      | slot_incarnation:
          SlotIncarnation.prepare(
            state.persistent_kv,
            state.stack_id,
            state.slot_name,
            purge_on_create
          )
    }
  end

  defp settle_slot_incarnation(%State{slot_incarnation: prepared} = state, outcome) do
    %{
      state
      | slot_incarnation:
          SlotIncarnation.settle(state.persistent_kv, state.stack_id, prepared, outcome)
    }
  end

  # Sucessfully created the replication slot.
  defp create_slot_result([%Postgrex.Result{} = result], state) do
    %Postgrex.Result{
      command: :create,
      columns: ["slot_name", "consistent_point", "snapshot_name", "output_plugin"],
      rows: [[_, lsn_str, nil, _]],
      num_rows: 1
    } = result

    Logger.debug("Created new slot at lsn=#{lsn_str}")
    lsn = lsn_str |> Lsn.from_string() |> Lsn.to_integer()
    state = settle_slot_incarnation(state, :created)
    prepare_journal_serving(state)

    {:created_new_slot, %{state | flushed_wal: max(lsn - 1, 0), replay_before_lsn: lsn}}
  end

  defp create_slot_result(%Postgrex.Error{} = error, state) do
    case error.postgres do
      %{code: :duplicate_object, pg_code: "42710"} ->
        # Slot already exists, proceed to the next step.
        Logger.debug("Found existing replication slot")
        settle_slot_incarnation(state, :existing)

      _ ->
        # Unexpected error, fail loudly.
        raise error
    end
  end

  ###

  defp query_slot_flushed_lsn_query(%State{slot_name: slot_name} = state) do
    Logger.debug("ReplicationClient step: query_slot_flushed_lsn")

    query = """
      SELECT confirmed_flush_lsn
      FROM pg_replication_slots
      WHERE slot_name = #{Utils.quote_string(slot_name)}
    """

    {:query, query, state}
  end

  defp query_slot_flushed_lsn_result([%Postgrex.Result{rows: []}], state) do
    %{state | slot_incarnation: nil}
  end

  defp query_slot_flushed_lsn_result([%Postgrex.Result{} = result], state) do
    %{rows: [[lsn_str]]} = result
    Logger.debug("Queried existing slot flushed lsn=#{lsn_str}")
    lsn = lsn_str |> Lsn.from_string() |> Lsn.to_integer()
    {base_path, generations} = prepare_journal_serving(state)

    state =
      if is_nil(state.slot_incarnation) do
        state
        |> prepare_slot_incarnation(false)
        |> settle_slot_incarnation(:existing)
      else
        state
      end

    %{
      state
      | flushed_wal: journal_boundary(state, base_path, generations) || max(lsn - 1, 0),
        replay_before_lsn: lsn
    }
  end

  defp query_slot_flushed_lsn_result(%Postgrex.Error{} = error, _state) do
    # Unexpected error, fail loudly.
    raise error
  end

  # The previous lock holder can write generations after this stack loads its metadata.
  # Check under the lock and before the slot position becomes visible to shape requests.
  defp prepare_journal_serving(%State{stack_id: stack_id, journal_base_path: base_path})
       when is_binary(base_path) do
    generations = TxnJournal.list_generations(base_path)
    if stack_id, do: :ok = StatusMonitor.prepare_journal_serving(stack_id, generations != [])
    {base_path, generations}
  end

  defp prepare_journal_serving(%State{stack_id: stack_id}) do
    with {Electric.ShapeCache.PureFileStorage, %{base_path: base_path}} <-
           storage_config(stack_id),
         [_ | _] = generations <- TxnJournal.list_generations(base_path) do
      :ok = StatusMonitor.prepare_journal_serving(stack_id, true)
      {base_path, generations}
    else
      _ -> {nil, []}
    end
  end

  # The slot reports the next byte it expects, one past what we last acked. Journal mode pins the
  # ack to the journal boundary, so seeding it from the slot pins it a byte further on every boot;
  # a couple of quiet restarts later the two no longer line up and the boot guard refuses to
  # start, reporting lost shape data that was never lost. The journal's own last commit is the
  # boundary it is meant to hold, and that does not drift.
  defp journal_boundary(%State{ack_after_journal?: true}, base_path, generations)
       when is_binary(base_path) do
    TxnJournal.last_commit_tx_offset(base_path, generations)
  end

  defp journal_boundary(%State{}, _base_path, _generations), do: nil

  ###

  defp set_display_setting_query(%{display_settings: [query | rest]} = state) do
    Logger.debug("ReplicationClient step: set_display_setting")
    {:query, query, %{state | display_settings: rest}}
  end

  defp set_display_setting_result([%Postgrex.Result{}], state) do
    state
  end

  defp set_display_setting_result(%Postgrex.Error{} = error, _state) do
    # Unexpected error, fail loudly.
    raise error
  end

  ###

  # This is one of the two terminal states of our state machine that puts the process into an
  # idling state until it receives a `:start_streaming` message.
  #
  # The other terminal state is implemented in `start_replication_slot_query/1`.
  defp ready_to_stream(state) do
    {:noreply, state}
  end

  ###

  # After Postgres executes the `START_REPLICATION` command, it will switch to the logical
  # streaming mode. That's why this function must return a `{:stream, ...}` and no queries
  # will be executed after this.
  defp start_replication_slot_query(state) do
    Logger.debug("ReplicationClient step: start_replication_slot")

    messages = if state.replication_heartbeat_interval > 0, do: ", messages 'true'", else: ""

    query =
      "START_REPLICATION SLOT #{Utils.quote_name(state.slot_name)} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication_name}'#{messages})"

    {:stream, query, [], state}
  end

  ### Below you'll find the boilerplate needed to put the state machine together.

  # This function defines the transition table for our ad-hoc state machine that determines which
  # step leads to which next one.
  #
  # This is how we order the queries to be executed prior to switching into the logical streaming mode.
  @spec next_step(state) :: step

  defp next_step(%{step: :connected}),
    do: :identify_system

  defp next_step(%{step: :identify_system} = state) do
    if history_timeline_id(state), do: :query_timeline_history, else: :query_pg_info
  end

  defp next_step(%{step: :query_timeline_history}),
    do: :query_pg_info

  defp next_step(%{step: :query_pg_info}),
    do: :acquire_lock

  defp next_step(%{step: :acquire_lock, lock_acquired?: false}),
    do: :acquire_lock

  defp next_step(%{step: :acquire_lock, lock_acquired?: true, try_creating_publication?: true}),
    do: :create_publication

  defp next_step(%{step: :acquire_lock, lock_acquired?: true}),
    do: :query_slot_flushed_lsn

  defp next_step(%{step: :create_publication, publication_owner?: true, recreate_slot?: true}),
    do: :drop_slot

  defp next_step(%{step: :create_publication}),
    do: :check_if_publication_exists

  defp next_step(%{step: :check_if_publication_exists, repair_publication?: true}),
    do: :drop_slot

  defp next_step(%{step: :check_if_publication_exists}),
    do: :query_slot_flushed_lsn

  defp next_step(%{step: :drop_slot, repair_publication?: true}),
    do: :repair_publication

  defp next_step(%{step: :drop_slot}),
    do: :create_slot

  defp next_step(%{step: :repair_publication}),
    do: :create_slot

  defp next_step(%{step: :create_slot, flushed_wal: 0}),
    do: :query_slot_flushed_lsn

  defp next_step(%{step: :create_slot}),
    do: :set_display_setting

  defp next_step(%{step: :query_slot_flushed_lsn, slot_incarnation: nil}),
    do: :create_slot

  defp next_step(%{step: :query_slot_flushed_lsn}),
    do: :set_display_setting

  defp next_step(%{step: :set_display_setting, display_settings: queries}) when queries != [],
    do: :set_display_setting

  defp next_step(%{step: :set_display_setting, start_streaming?: true}),
    do: :start_streaming

  defp next_step(%{step: :set_display_setting}),
    do: :ready_to_stream

  ###

  # Helper function that dispatches each step to a function specific to it. This is done so
  # that query and result processing functions for the same step can be grouped together in
  # this module.
  @spec query_for_step(step, state) :: callback_return

  defp query_for_step(:identify_system, state), do: identify_system_query(state)
  defp query_for_step(:query_timeline_history, state), do: timeline_history_query(state)
  defp query_for_step(:query_pg_info, state), do: pg_info_query(state)
  defp query_for_step(:acquire_lock, state), do: acquire_lock_query(state)
  defp query_for_step(:create_publication, state), do: create_publication_query(state)

  defp query_for_step(:check_if_publication_exists, state),
    do: check_if_publication_exists_query(state)

  defp query_for_step(:repair_publication, state), do: repair_publication_query(state)

  defp query_for_step(:drop_slot, state), do: drop_slot_query(state)
  defp query_for_step(:create_slot, state), do: create_slot_query(state)
  defp query_for_step(:query_slot_flushed_lsn, state), do: query_slot_flushed_lsn_query(state)
  defp query_for_step(:set_display_setting, state), do: set_display_setting_query(state)
  defp query_for_step(:ready_to_stream, state), do: ready_to_stream(state)
  defp query_for_step(:start_streaming, state), do: start_replication_slot_query(state)

  ###

  # Helper function that dispatches processing of a query result to a function specific to
  # that query's step. This is again done to facilitate grouping functions for the same step.
  @spec dispatch_query_result(step, query_result, state) ::
          state | {extra_info, state} | no_return

  defp dispatch_query_result(:identify_system, result, state),
    do: identify_system_result(result, state)

  defp dispatch_query_result(:query_timeline_history, result, state),
    do: timeline_history_result(result, state)

  defp dispatch_query_result(:query_pg_info, result, state),
    do: pg_info_result(result, state)

  defp dispatch_query_result(:acquire_lock, result, state),
    do: acquire_lock_result(result, state)

  defp dispatch_query_result(:create_publication, result, state),
    do: create_publication_result(result, state)

  defp dispatch_query_result(:check_if_publication_exists, result, state),
    do: check_if_publication_exists_result(result, state)

  defp dispatch_query_result(:repair_publication, result, state),
    do: repair_publication_result(result, state)

  defp dispatch_query_result(:drop_slot, result, state),
    do: drop_slot_result(result, state)

  defp dispatch_query_result(:create_slot, result, state),
    do: create_slot_result(result, state)

  defp dispatch_query_result(:query_slot_flushed_lsn, result, state),
    do: query_slot_flushed_lsn_result(result, state)

  defp dispatch_query_result(:set_display_setting, result, state),
    do: set_display_setting_result(result, state)
end
