defmodule Electric.Postgres.ReplicationClientSeenWalBroadcastTest do
  # The global last-seen LSN feed promises subscribers that every transaction at or
  # below the broadcast position has already been fanned out — buffered move-ins
  # splice against exactly that barrier. The pipelined ingest modes (journal mode, or
  # a replication window above one) decode keepalives while events are still in
  # flight, so their positions must wait for the pipeline to drain. Publishing at
  # decode time raced the position ahead of fan-out and the collector's older commit
  # broadcasts then buried it, leaving move-ins waiting on a boundary that had
  # already been seen.
  use ExUnit.Case, async: true

  import Support.ComponentSetup,
    only: [with_registry: 1, with_stack_id_from_test: 1, with_lsn_tracker: 1]

  alias Electric.LsnTracker
  alias Electric.Postgres.ReplicationClient
  alias Electric.Postgres.ReplicationClient.MessageConverter
  alias Electric.Postgres.ReplicationClient.State
  alias Electric.Replication.Changes.Relation

  @wal_end 9_000_000

  setup [:with_stack_id_from_test, :with_registry, :with_lsn_tracker]

  setup ctx do
    LsnTracker.subscribe_to_global_lsn_updates(ctx.stack_id)
    :ok
  end

  defp streaming_state(stack_id, overrides) do
    struct!(
      %State{
        handle_event: {__MODULE__, :unused, []},
        publication_name: "test_pub",
        stack_id: stack_id,
        step: :streaming,
        message_converter: MessageConverter.new(max_tx_size: nil, max_batch_size: 100)
      },
      overrides
    )
  end

  defp with_relation_in_flight(state) do
    ref = Process.monitor(spawn(fn -> :ok end))

    {ref,
     %{
       state
       | in_flight:
           :queue.in(
             {ref, %Relation{}, 60_000, System.monotonic_time(:millisecond)},
             state.in_flight
           ),
         in_flight_count: state.in_flight_count + 1
     }}
  end

  defp primary_keepalive(wal_end), do: <<?k, wal_end::64, 0::64, 0>>

  test "journal mode holds a keepalive position until the pipeline drains", ctx do
    # Arrange
    {ref, state} =
      ctx.stack_id
      |> streaming_state(ack_after_journal?: true)
      |> with_relation_in_flight()

    # Act: the keepalive is decoded while the event is still in flight.
    {:noreply, [], state} = ReplicationClient.handle_data(primary_keepalive(@wal_end), state)

    # Assert: the position is held, not published.
    assert state.pending_seen_wal == @wal_end
    refute_received {:global_last_seen_lsn, _}

    # Act: the in-flight event acks and the pipeline drains.
    {:noreply, [], state} = ReplicationClient.handle_info({ref, :ok}, state)

    # Assert: the held position is published exactly once.
    assert state.pending_seen_wal == nil
    assert_received {:global_last_seen_lsn, @wal_end}
    refute_received {:global_last_seen_lsn, _}
  end

  test "a window above one holds a keepalive position until the pipeline drains", ctx do
    # Arrange
    {ref, state} =
      ctx.stack_id
      |> streaming_state(pipeline_window: 2)
      |> with_relation_in_flight()

    # Act
    {:noreply, [], state} = ReplicationClient.handle_data(primary_keepalive(@wal_end), state)

    # Assert
    assert state.pending_seen_wal == @wal_end
    refute_received {:global_last_seen_lsn, _}

    # Act
    {:noreply, [], state} = ReplicationClient.handle_info({ref, :ok}, state)

    # Assert
    assert state.pending_seen_wal == nil
    assert_received {:global_last_seen_lsn, @wal_end}
  end

  test "journal mode publishes at decode time when the pipeline is idle", ctx do
    # Arrange
    state = streaming_state(ctx.stack_id, ack_after_journal?: true)

    # Act
    {:noreply, [], state} = ReplicationClient.handle_data(primary_keepalive(@wal_end), state)

    # Assert
    assert state.pending_seen_wal == nil
    assert_received {:global_last_seen_lsn, @wal_end}
  end

  test "a window of one publishes at decode time", ctx do
    # Arrange
    state = streaming_state(ctx.stack_id, [])

    # Act
    {:noreply, [], state} = ReplicationClient.handle_data(primary_keepalive(@wal_end), state)

    # Assert
    assert state.pending_seen_wal == nil
    assert_received {:global_last_seen_lsn, @wal_end}
  end

  test "a pending journal replay holds keepalive positions even with an idle pipeline", ctx do
    # Arrange: the collector flagged journaled transactions awaiting boot replay. The
    # client has dispatched nothing, so only the marker stands between a boot keepalive
    # and an over-promise past the entire journal.
    LsnTracker.mark_journal_replay_pending(ctx.stack_id)
    state = streaming_state(ctx.stack_id, ack_after_journal?: true)

    # Act
    {:noreply, [], state} = ReplicationClient.handle_data(primary_keepalive(@wal_end), state)

    # Assert: held, not published — the collector's own replay broadcasts stay the
    # only source of positions until the replay completes.
    assert state.pending_seen_wal == @wal_end
    refute_received {:global_last_seen_lsn, _}

    # Act: the replay completes and the next keepalive arrives.
    LsnTracker.clear_journal_replay_pending(ctx.stack_id)

    {:noreply, [], state} =
      ReplicationClient.handle_data(primary_keepalive(@wal_end - 1), state)

    # Assert: the held maximum is published, not the lower late keepalive.
    assert state.pending_seen_wal == nil
    assert_received {:global_last_seen_lsn, @wal_end}
  end

  test "a boot keepalive does not out-promise an in-progress journal replay", ctx do
    # Arrange: a leftover journal replays even in the default mode, so the marker must
    # hold positions there too.
    LsnTracker.mark_journal_replay_pending(ctx.stack_id)
    state = streaming_state(ctx.stack_id, [])

    # Act: the boot keepalive is decoded, then the collector's replay fans out and
    # broadcasts three journaled commits.
    {:noreply, [], state} = ReplicationClient.handle_data(primary_keepalive(@wal_end), state)
    Enum.each([1_000, 2_000, 3_000], &LsnTracker.broadcast_last_seen_lsn(ctx.stack_id, &1))

    # Assert: a consumer starting a move-in mid-replay reads the collector's real
    # fan-out boundary, not the server's wal_end.
    assert state.pending_seen_wal == @wal_end
    assert LsnTracker.get_last_broadcast_lsn(ctx.stack_id) == 3_000
  end
end
