defmodule Electric.Postgres.ReplicationClientWedgeTest do
  @moduledoc """
  A stall of this kind can last indefinitely without anything noticing. The socket
  was paused waiting for an ack downstream never sent, and the keepalive timer kept Postgres's
  `reply_time` fresh the whole time, so `wal_sender_timeout` never fired and the slot stayed
  pinned. These cases pin the one condition that separates that state from every healthy one.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Support.ComponentSetup, only: [with_stack_id_from_test: 1, with_registry: 1]

  alias Electric.Postgres.ReplicationClient
  alias Electric.Postgres.ReplicationClient.MessageConverter
  alias Electric.Postgres.ReplicationClient.State
  alias Electric.Replication.Changes.Relation

  @threshold 10 * 60_000
  @flushed_wal 9_000_000

  setup [:with_stack_id_from_test, :with_registry]

  defp state(ctx, overrides) do
    struct!(
      %State{
        handle_event: {__MODULE__, :unused, []},
        publication_name: "test_pub",
        stack_id: ctx.stack_id,
        step: :streaming,
        flushed_wal: @flushed_wal,
        message_converter: MessageConverter.new(max_tx_size: nil, max_batch_size: 100)
      },
      overrides
    )
  end

  defp paused_ago(state, age_ms) do
    %{
      state
      | socket_paused?: true,
        paused_since: System.monotonic_time(:millisecond) - age_ms
    }
  end

  defp with_event_in_flight(state, ref \\ make_ref()) do
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

  defp check(state) do
    capture_log(fn ->
      {:noreply, ^state} = ReplicationClient.handle_info(:check_if_wedged, state)
    end)
  end

  test "a socket paused past the downstream budget reports the wedge", ctx do
    state = state(ctx, []) |> paused_ago(@threshold + 1_000) |> with_event_in_flight()

    log = check(state)

    # downstream's, and how far the slot is stuck.
    assert log =~ "Replication is wedged"
    assert log =~ "1 event(s) in flight"
    assert log =~ "cannot advance past #{@flushed_wal}"
  end

  test "a socket paused within the budget is ordinary backpressure and stays quiet", ctx do
    state = state(ctx, []) |> paused_ago(@threshold - 1_000) |> with_event_in_flight()

    log = check(state)

    refute log =~ "wedged"
  end

  test "a pause held with nothing in flight is reported too", ctx do
    # flight, so keying the check off in-flight work would have missed them entirely.
    state = state(ctx, []) |> paused_ago(@threshold + 1_000)

    log = check(state)

    assert log =~ "Replication is wedged"
    assert log =~ "0 event(s) in flight"
  end

  test "an open socket is never a wedge, however long an event has been outstanding", ctx do
    # here means downstream is slow, not stopped.
    state = state(ctx, socket_paused?: false) |> with_event_in_flight()

    log = check(state)

    refute log =~ "wedged"
  end

  test "a quiet stack is not a wedge", ctx do
    # reason this check keys off a pause rather than elapsed silence.
    refute check(state(ctx, [])) =~ "wedged"
  end

  describe "the stamp on the real resume path" do
    test "an ack that reopens the socket clears the stamp", ctx do
      ref = make_ref()

      state =
        state(ctx, pipeline_healthy?: true, pipeline_window: 1)
        |> with_event_in_flight(ref)
        |> paused_ago(@threshold + 1_000)

      {:noreply_and_resume, _acks, resumed} = ReplicationClient.handle_info({ref, :ok}, state)

      # began, which is worse than not reporting at all.
      refute resumed.socket_paused?
      assert resumed.paused_since == nil
      refute check(resumed) =~ "wedged"
    end
  end

  describe "pipeline collapse and undelivered-work accounting" do
    test "a collector DOWN keeps the other in-flight refs stale so a survivor's ack is dropped",
         ctx do
      down_ref = make_ref()
      survivor_ref = make_ref()

      state =
        state(ctx, pipeline_window: 4)
        |> with_event_in_flight(down_ref)
        |> with_event_in_flight(survivor_ref)

      {result, _log} =
        with_log(fn ->
          ReplicationClient.handle_info({:DOWN, down_ref, :process, self(), :killed}, state)
        end)

      {:noreply, collapsed} = result

      # check and exit the replication connection.
      assert MapSet.member?(collapsed.stale_refs, survivor_ref)
      refute MapSet.member?(collapsed.stale_refs, down_ref)
    end

    test "a window > 1 does not report flush up-to-date while a parked replay is undelivered",
         ctx do
      # parked undelivered transactions in the replay queue.
      lsn = Electric.Postgres.Lsn.from_integer(@flushed_wal)

      state =
        state(ctx,
          pipeline_window: 4,
          in_flight_count: 0,
          replay: :queue.from_list([{%Relation{}, 60_000}]),
          last_seen_txn_lsn: lsn,
          last_confirmed_flush_lsn: @flushed_wal
        )

      {:noreply, _status, updated} =
        ReplicationClient.handle_info({:flush_boundary_updated, @flushed_wal}, state)

      refute updated.flush_up_to_date?
    end
  end
end
