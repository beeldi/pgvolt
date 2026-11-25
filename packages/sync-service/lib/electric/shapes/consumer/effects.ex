defmodule Electric.Shapes.Consumer.Effects do
  @moduledoc false
  # These are the runtime effects emitted by event handlers after functional
  # event processing. Consumer bootstrapping and other imperative setup steps
  # live in SetupEffects rather than being mixed into this runtime effect layer.

  alias Electric.Connection.Manager
  alias Electric.Postgres.SnapshotQuery
  alias Electric.Shapes.Filter.Indexes.SubqueryIndex
  alias Electric.ShapeCache.Storage
  alias Electric.LogItems
  alias Electric.Replication.Changes
  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache
  alias Electric.Shapes.Consumer.Subqueries.MoveBroadcast
  alias Electric.Shapes.Querying
  alias Electric.Shapes.Shape
  alias Electric.Telemetry.OpenTelemetry

  defmodule AppendChanges do
    @moduledoc false
    defstruct changes: [], xid: nil, full_xid: nil
  end

  defmodule AppendControl do
    @moduledoc false
    defstruct message: nil
  end

  defmodule AppendMoveInSnapshot do
    @moduledoc false
    # `skip_rows`: nil, or `{any_row, written_row, keys}` naming the snapshot rows that stay out of
    # the log: those `any_row` takes out, and among the rows of those keys, those `written_row`
    # takes out. Each is nil, `:all`, or the patterns of removals for
    # `MoveBroadcast.removed_by?/3`.
    defstruct [:snapshot_name, :snapshot, :skip_rows, row_count: 0, row_bytes: 0]
  end

  defmodule NotifyFlushed do
    @moduledoc false
    defstruct [:log_offset]
  end

  defmodule StartMoveInQuery do
    @moduledoc false
    defstruct [
      :dnf_plan,
      :trigger_dep_index,
      :values,
      :views_before_move,
      :views_after_move,
      kind: :move_in,
      query_patterns?: false
    ]

    @type t() :: %__MODULE__{
            dnf_plan: Electric.Shapes.DnfPlan.t(),
            trigger_dep_index: non_neg_integer(),
            values: list() | {list(), list()},
            views_before_move: Electric.Shapes.Consumer.Subqueries.Views.t(),
            views_after_move: Electric.Shapes.Consumer.Subqueries.Views.t(),
            kind: :move_in | :move_out | :replace,
            query_patterns?: boolean()
          }
  end

  defmodule SubscribeGlobalLsn do
    @moduledoc false
    defstruct []
  end

  defmodule UnsubscribeGlobalLsn do
    @moduledoc false
    defstruct []
  end

  defmodule AddToSubqueryIndex do
    @moduledoc false
    defstruct [:dep_index, :subquery_ref, :values]
  end

  defmodule RemoveFromSubqueryIndex do
    @moduledoc false
    defstruct [:dep_index, :subquery_ref, :values]
  end

  @type t() ::
          %AppendChanges{}
          | %AppendControl{}
          | %AppendMoveInSnapshot{}
          | %NotifyFlushed{}
          | %StartMoveInQuery{}
          | %SubscribeGlobalLsn{}
          | %UnsubscribeGlobalLsn{}
          | %AddToSubqueryIndex{}
          | %RemoveFromSubqueryIndex{}

  @type execution_result() :: %{
          state: term(),
          num_changes: non_neg_integer(),
          total_size: non_neg_integer(),
          pending_written_offset: LogOffset.t() | nil
        }

  @spec execute([t()], term(), keyword()) :: execution_result()
  def execute(effects, state, _opts \\ []) when is_list(effects) do
    Enum.reduce(
      effects,
      %{state: state, num_changes: 0, total_size: 0, pending_written_offset: nil},
      fn effect, acc ->
        execute_effect(effect, acc)
      end
    )
  end

  defp execute_effect(%AppendChanges{changes: [], xid: _}, acc), do: acc

  defp execute_effect(%AppendChanges{changes: changes, xid: xid, full_xid: full_xid}, acc) do
    state = acc.state

    {lines, total_size} = LogItems.encode_changes(changes, xid, full_xid, state.shape)
    last_offset = lines |> List.last() |> line_offset()
    writer = ShapeCache.Storage.append_to_log!(lines, state.writer)
    state = %{state | writer: writer, latest_offset: last_offset}

    %{
      acc
      | state: state,
        num_changes: acc.num_changes + line_count(lines),
        total_size: acc.total_size + total_size,
        pending_written_offset: state.latest_offset
    }
  end

  defp execute_effect(%AppendControl{message: message}, acc) do
    state = acc.state
    encoded = Jason.encode!(message)

    {{_, offset}, writer} =
      ShapeCache.Storage.append_control_message!(encoded, state.writer)

    state = %{state | writer: writer, latest_offset: offset}

    %{
      acc
      | state: state,
        num_changes: acc.num_changes + 1,
        total_size: acc.total_size + byte_size(encoded),
        pending_written_offset: state.latest_offset
    }
  end

  defp execute_effect(
         %AppendMoveInSnapshot{
           snapshot_name: snapshot_name,
           row_count: row_count,
           row_bytes: row_bytes,
           snapshot: snapshot,
           skip_rows: skip_rows
         },
         acc
       ) do
    state = acc.state

    {{_, inserted_offset}, writer} =
      ShapeCache.Storage.append_move_in_snapshot_to_log!(
        snapshot_name,
        state.writer,
        skip_row_fun(skip_rows)
      )

    state = %{state | writer: writer, latest_offset: inserted_offset}

    # Every move-in query ends with a snapshot-end, even one that found no rows: it is
    # how a client learns the move-in it saw announced is complete.
    snapshot_end =
      snapshot
      |> snapshot_end_message()
      |> Jason.encode!()

    {{_, offset}, writer} =
      ShapeCache.Storage.append_control_message!(snapshot_end, state.writer)

    state = %{state | writer: writer, latest_offset: offset}

    %{
      acc
      | state: state,
        num_changes: acc.num_changes + row_count + 1,
        total_size: acc.total_size + row_bytes + byte_size(snapshot_end),
        pending_written_offset: state.latest_offset
    }
  end

  defp execute_effect(%NotifyFlushed{log_offset: log_offset}, acc) do
    state = acc.state

    state =
      if acc.pending_written_offset do
        %{
          state
          | txn_offset_mapping:
              state.txn_offset_mapping ++ [{acc.pending_written_offset, log_offset}]
        }
      else
        consider_flushed(state, log_offset)
      end

    %{acc | state: state, pending_written_offset: nil}
  end

  defp execute_effect(%StartMoveInQuery{} = effect, acc) do
    state = acc.state
    supervisor = Electric.ProcessRegistry.name(state.stack_id, Electric.StackTaskSupervisor)
    query_move_in_async(supervisor, state, effect, self())
    acc
  end

  defp execute_effect(%SubscribeGlobalLsn{}, acc) do
    {:ok, _} = Electric.LsnTracker.subscribe_to_global_lsn_updates(acc.state.stack_id)

    acc
  end

  defp execute_effect(%UnsubscribeGlobalLsn{}, acc) do
    :ok = Electric.LsnTracker.unsubscribe_from_global_lsn_updates(acc.state.stack_id)
    acc
  end

  defp execute_effect(%AddToSubqueryIndex{} = effect, acc) do
    update_subquery_index(acc, effect.dep_index, effect.subquery_ref, effect.values, :add)
  end

  defp execute_effect(%RemoveFromSubqueryIndex{} = effect, acc) do
    update_subquery_index(acc, effect.dep_index, effect.subquery_ref, effect.values, :remove)
  end

  defp update_subquery_index(acc, dep_index, subquery_ref, values, op) do
    state = acc.state
    index = SubqueryIndex.for_stack(state.stack_id)
    fun = if op == :add, do: &SubqueryIndex.add_value/5, else: &SubqueryIndex.remove_value/5

    for {value, _original} <- values do
      fun.(index, state.shape_handle, subquery_ref, dep_index, value)
    end

    acc
  end

  @spec query_move_in_async(pid() | atom(), map(), StartMoveInQuery.t(), pid()) :: :ok
  def query_move_in_async(supervisor, consumer_state, %StartMoveInQuery{} = request, consumer_pid) do
    %{stack_id: stack_id, shape_handle: shape_handle, shape: shape, storage: storage} =
      consumer_state

    query_fn = fn conn, _pg_snapshot, lsn ->
      patterns =
        if request.query_patterns? do
          Querying.move_patterns(
            conn,
            stack_id,
            shape_handle,
            shape,
            request.dnf_plan,
            request.trigger_dep_index,
            request.views_before_move,
            request.views_after_move,
            request.kind
          )
        end

      {snapshot_name, row_count, row_bytes} =
        if request.kind != :move_out do
          write_move_in_snapshot(conn, stack_id, shape_handle, shape, storage, request)
        else
          {nil, 0, 0}
        end

      send(
        consumer_pid,
        {:query_move_in_complete, snapshot_name, row_count, row_bytes, lsn, patterns}
      )
    end

    :telemetry.execute([:electric, :subqueries, :move_in_triggered], %{count: 1}, %{
      stack_id: stack_id
    })

    # Propagate OTel context so spans created inside the task are linked to the
    # caller's trace. OTel context is per-process, so without this any
    # `with_child_span` calls in the task would be silently dropped.
    trace_context = OpenTelemetry.get_current_context()

    Task.Supervisor.start_child(supervisor, fn ->
      try do
        OpenTelemetry.set_current_context(trace_context)

        OpenTelemetry.with_span(
          "shape_snapshot.move_in_task",
          Shape.otel_attrs(shape_handle, shape, query_reason: "move_in_query"),
          stack_id,
          fn -> execute_move_in_query(stack_id, shape_handle, shape, consumer_pid, query_fn) end
        )
      rescue
        error -> send(consumer_pid, {:query_move_in_error, error, __STACKTRACE__})
      catch
        # A snapshot-pool checkout timeout is an exit, which a rescue does not catch, and this task is
        # started unlinked and unmonitored -- anything the consumer is not told about leaves it waiting
        # for a move-in result that nothing will ever send.
        kind, reason ->
          send(
            consumer_pid,
            {:query_move_in_error, RuntimeError.exception(Exception.format_banner(kind, reason)),
             __STACKTRACE__}
          )
      end
    end)

    :ok
  end

  defp write_move_in_snapshot(conn, stack_id, shape_handle, shape, storage, request) do
    where =
      Querying.move_in_where_clause(
        request.dnf_plan,
        request.trigger_dep_index,
        request.views_before_move,
        request.views_after_move,
        shape.where.used_refs
      )

    snapshot_name = Electric.Utils.uuid4()
    task_pid = self()

    Querying.query_move_in(conn, stack_id, shape_handle, shape, where,
      dnf_plan: request.dnf_plan,
      views: request.views_after_move
    )
    |> Stream.transform(
      fn -> {0, 0} end,
      fn [_, _, json] = row, {row_count, row_bytes} ->
        {[row], {row_count + 1, row_bytes + IO.iodata_length(json)}}
      end,
      fn {row_count, row_bytes} ->
        send(task_pid, {:move_in_snapshot_stats, row_count, row_bytes})
      end
    )
    |> Storage.write_move_in_snapshot!(snapshot_name, storage)

    receive do
      {:move_in_snapshot_stats, row_count, row_bytes} -> {snapshot_name, row_count, row_bytes}
    end
  end

  # Synchronous core of query_move_in_async(). Expects the current OTel context to already be set up by the caller.
  defp execute_move_in_query(stack_id, shape_handle, shape, consumer_pid, query_fn) do
    pool = Manager.pool_name(stack_id, :snapshot)

    SnapshotQuery.execute_for_shape(pool, shape_handle, shape,
      stack_id: stack_id,
      query_reason: "move_in_query",
      snapshot_info_fn: fn _, pg_snapshot, _lsn ->
        send(consumer_pid, {:pg_snapshot_known, pg_snapshot})
      end,
      query_fn: query_fn
    )
  end

  defp consider_flushed(state, log_offset) do
    alias Electric.Replication.ShapeLogCollector

    if state.txn_offset_mapping == [] do
      ShapeLogCollector.notify_flushed(state.stack_id, state.shape_handle, log_offset)
      state
    else
      new_boundary = log_offset

      {head, tail} =
        Enum.split_while(
          state.txn_offset_mapping,
          &(LogOffset.compare(elem(&1, 1), new_boundary) == :lt)
        )

      case Enum.reverse(head) do
        [] ->
          state

        [{offset, _} | rest] ->
          %{state | txn_offset_mapping: Enum.reverse([{offset, new_boundary} | rest], tail)}
      end
    end
  end

  defp line_count(lines) do
    Enum.reduce(lines, 0, fn
      %Changes.FramedBlock{count: count}, acc -> acc + count
      _line, acc -> acc + 1
    end)
  end

  defp line_offset(%Changes.FramedBlock{last_offset: offset}), do: offset
  defp line_offset(line), do: elem(line, 0)

  defp skip_row_fun(nil), do: fn _key, _tags, _json -> false end

  defp skip_row_fun({any_row, written_row, keys}) do
    fn key, tags, json ->
      removed_by?(any_row, tags, json) or
        (MapSet.member?(keys, key) and removed_by?(written_row, tags, json))
    end
  end

  defp removed_by?(nil, _tags, _json), do: false
  defp removed_by?(:all, _tags, _json), do: true

  # A row's headers are read only when those patterns reach its tags.
  defp removed_by?(patterns, tags, json),
    do: MoveBroadcast.matched_by?(tags, patterns) and conditions_removed_by?(tags, json, patterns)

  # A row whose headers carry no active conditions stays in the log.
  defp conditions_removed_by?(tags, json, patterns) do
    case Jason.decode!(json) do
      %{"headers" => %{"active_conditions" => conditions}} when is_list(conditions) ->
        MoveBroadcast.removed_by?(tags, conditions, patterns)

      _ ->
        false
    end
  end

  defp snapshot_end_message({xmin, xmax, xip_list}) do
    %{
      headers: %{
        control: "snapshot-end",
        xmin: to_string(xmin),
        xmax: to_string(xmax),
        xip_list: Enum.map(xip_list, &to_string/1)
      }
    }
  end
end
