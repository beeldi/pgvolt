defmodule Electric.Shapes.Consumer.EffectsTest do
  use ExUnit.Case, async: true

  alias Electric.Postgres.SnapshotQuery
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Consumer.Effects.StartMoveInQuery
  alias Electric.Shapes.Querying

  @shape_handle "move-in-effects-test"

  setup do
    stack_id = "effects-test-#{System.unique_integer([:positive])}"

    {:ok, supervisor} = Task.Supervisor.start_link()

    Repatch.patch(Querying, :move_in_where_clause, [mode: :shared], fn _plan,
                                                                       _dep,
                                                                       _before,
                                                                       _after,
                                                                       _refs ->
      {"true", []}
    end)

    consumer_state = %{
      shape: %Electric.Shapes.Shape{
        root_table: {"public", "items"},
        root_table_id: 1,
        where: %{query: "true", used_refs: %{}}
      },
      shape_handle: @shape_handle,
      stack_id: stack_id,
      storage: :unused_storage
    }

    request = %StartMoveInQuery{
      dnf_plan: :unused,
      trigger_dep_index: 0,
      values: [],
      views_after_move: :unused,
      views_before_move: :unused
    }

    %{consumer_state: consumer_state, request: request, supervisor: supervisor}
  end

  describe "query_move_in_async/4" do
    test "tells the consumer when the snapshot query raises", ctx do
      # Arrange
      Repatch.patch(SnapshotQuery, :execute_for_shape, [mode: :shared], fn _pool,
                                                                           _handle,
                                                                           _shape,
                                                                           _opts ->
        raise "snapshot query blew up"
      end)

      Repatch.allow(self(), ctx.supervisor)

      # Act
      :ok = Effects.query_move_in_async(ctx.supervisor, ctx.consumer_state, ctx.request, self())

      # Assert
      assert_receive {:query_move_in_error, %RuntimeError{message: "snapshot query blew up"},
                      _stack}
    end

    test "tells the consumer when the task fails before the query is even reached", ctx do
      # Arrange -- the span attributes are built inside the task but outside the query call, so a
      # guard wrapped around the query alone leaves this failure with no channel to the consumer.
      Repatch.patch(Electric.Shapes.Shape, :otel_attrs, [mode: :shared], fn _handle,
                                                                            _shape,
                                                                            _opts ->
        raise "attrs blew up"
      end)

      Repatch.allow(self(), ctx.supervisor)

      # Act
      :ok = Effects.query_move_in_async(ctx.supervisor, ctx.consumer_state, ctx.request, self())

      # Assert
      assert_receive {:query_move_in_error, %RuntimeError{message: "attrs blew up"}, _stack}
    end

    test "tells the consumer when the snapshot query exits, which a rescue cannot catch", ctx do
      # Arrange — a pool checkout timeout leaves the task with nothing to reply to the consumer,
      # and the task is started unlinked and unmonitored, so silence here is a permanent stall.
      Repatch.patch(SnapshotQuery, :execute_for_shape, [mode: :shared], fn _pool,
                                                                           _handle,
                                                                           _shape,
                                                                           _opts ->
        exit({:timeout, {DBConnection.Holder, :checkout, []}})
      end)

      Repatch.allow(self(), ctx.supervisor)

      # Act
      :ok = Effects.query_move_in_async(ctx.supervisor, ctx.consumer_state, ctx.request, self())

      # Assert
      assert_receive {:query_move_in_error, %RuntimeError{}, _stack}
    end
  end
end
