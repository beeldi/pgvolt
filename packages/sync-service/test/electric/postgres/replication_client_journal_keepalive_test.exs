defmodule Electric.Postgres.ReplicationClientJournalKeepaliveTest do
  # Journal mode acks the slot at the journal boundary, so WAL that carries nothing for
  # the publication (other databases, unpublished churn) used to pile up behind a pinned
  # ack on any published-write-quiet stack. A keepalive over a fully drained pipeline now
  # advances the slot — but only by first writing a durable idle-boundary marker into the
  # journal, so the next boot still finds the slot and the journal's coverage lined up.
  # While anything is in flight, or a boot replay is pending, the pin holds: acking the
  # server's wal_end past undelivered work is exactly the bug this file always guarded.
  use ExUnit.Case, async: true

  import Support.ComponentSetup,
    only: [with_registry: 1, with_stack_id_from_test: 1, with_lsn_tracker: 1]

  alias Electric.LsnTracker
  alias Electric.Postgres.ReplicationClient
  alias Electric.Postgres.ReplicationClient.MessageConverter
  alias Electric.Postgres.ReplicationClient.State
  alias Electric.Replication.Changes.Relation
  alias Electric.Replication.TxnJournal

  @journal_boundary 1_000
  @server_wal_end 9_000_000

  @moduletag :tmp_dir

  setup [:with_stack_id_from_test, :with_registry, :with_lsn_tracker]

  defp journal_mode_state(ctx, overrides) do
    struct!(
      %State{
        handle_event: {__MODULE__, :unused, []},
        publication_name: "test_pub",
        stack_id: ctx.stack_id,
        step: :streaming,
        ack_after_journal?: true,
        journal_base_path: ctx.tmp_dir,
        flushed_wal: @journal_boundary,
        received_wal: @journal_boundary,
        message_converter: MessageConverter.new(max_tx_size: nil, max_batch_size: 100)
      },
      overrides
    )
  end

  defp with_relation_in_flight(state) do
    ref = Process.monitor(spawn(fn -> :ok end))

    %{
      state
      | in_flight:
          :queue.in(
            {ref, %Relation{}, 60_000, System.monotonic_time(:millisecond)},
            state.in_flight
          ),
        in_flight_count: state.in_flight_count + 1
    }
  end

  defp primary_keepalive(wal_end, reply),
    do: <<?k, wal_end::64, 0::64, reply>>

  defp journal_coverage(base_path),
    do: TxnJournal.last_commit_tx_offset(base_path, TxnJournal.list_generations(base_path))

  test "a keepalive over a drained pipeline advances the slot behind a durable marker", ctx do
    state = journal_mode_state(ctx, [])

    # outside the publication).
    {:noreply, [status_update], state} =
      ReplicationClient.handle_data(primary_keepalive(@server_wal_end, 1), state)

    <<?r, _received::64, flush_lsn::64, _apply::64, _clock::64, 0>> = status_update

    # with it — the next boot's guard reads the same position from disk.
    assert state.flushed_wal == @server_wal_end - 1
    assert flush_lsn == @server_wal_end
    assert journal_coverage(ctx.tmp_dir) == @server_wal_end - 1
  end

  test "a keepalive holds the journal boundary while events are in flight", ctx do
    state = journal_mode_state(ctx, []) |> with_relation_in_flight()

    {:noreply, [status_update], state} =
      ReplicationClient.handle_data(primary_keepalive(@server_wal_end, 1), state)

    <<?r, _received::64, flush_lsn::64, _apply::64, _clock::64, 0>> = status_update

    assert state.flushed_wal == @journal_boundary
    assert flush_lsn == @journal_boundary + 1
    assert journal_coverage(ctx.tmp_dir) == nil
  end

  test "a keepalive holds the journal boundary while a boot replay is pending", ctx do
    LsnTracker.mark_journal_replay_pending(ctx.stack_id)
    state = journal_mode_state(ctx, [])

    {:noreply, [status_update], state} =
      ReplicationClient.handle_data(primary_keepalive(@server_wal_end, 1), state)

    <<?r, _received::64, flush_lsn::64, _apply::64, _clock::64, 0>> = status_update

    assert state.flushed_wal == @journal_boundary
    assert flush_lsn == @journal_boundary + 1
    assert journal_coverage(ctx.tmp_dir) == nil
  end

  test "a stale keepalive at or below the boundary journals nothing", ctx do
    state = journal_mode_state(ctx, [])

    {:noreply, [_status_update], state} =
      ReplicationClient.handle_data(primary_keepalive(@journal_boundary, 1), state)

    assert state.flushed_wal == @journal_boundary
    assert journal_coverage(ctx.tmp_dir) == nil
  end

  test "the standby status update never reports flush ahead of write", ctx do
    # one branch that raises flush without moving received.
    state = journal_mode_state(ctx, received_wal: 100, flushed_wal: 200)

    {:noreply, [status_update], _state} =
      ReplicationClient.handle_info(:send_keepalive, state)

    <<?r, write_lsn::64, flush_lsn::64, apply_lsn::64, _clock::64, 0>> = status_update

    # rather than emitting the incoherent flush > write that scrambles its lag columns.
    assert write_lsn >= flush_lsn
    assert write_lsn == 201
    assert flush_lsn == 201
    assert apply_lsn == 201
  end

  # A purge removes the journal, and a slot behind the journal boundary, on a database the stack
  # was pointed at since, resends what a collector started at that boundary dropped. The shape
  # subsystem started again replays from the slot's own position.
  test "a replay from the slot starts at the slot's position, not the journal boundary", ctx do
    journal =
      TxnJournal.open_writer!(ctx.tmp_dir)
      |> TxnJournal.append_event!({:idle_slot_boundary, 300})
      |> TxnJournal.sync!(300)

    File.close(journal.file)

    state =
      journal_mode_state(ctx,
        step: :query_slot_flushed_lsn,
        slot_name: "test_slot",
        display_settings: ["SET dummy = 'test'"],
        flushed_wal: 0
      )

    result = [
      %Postgrex.Result{
        command: :select,
        columns: ["confirmed_flush_lsn"],
        rows: [["0/C9"]],
        num_rows: 1
      }
    ]

    {:query, _query, state} = ReplicationClient.handle_result(result, state)
    assert state.flushed_wal == 300

    :ok = LsnTracker.request_replay_from_slot(ctx.stack_id)
    :ok = LsnTracker.apply_replay_from_slot(ctx.stack_id)

    assert LsnTracker.get_last_processed_lsn(ctx.stack_id) ==
             Electric.Postgres.Lsn.from_integer(200)
  end
end
