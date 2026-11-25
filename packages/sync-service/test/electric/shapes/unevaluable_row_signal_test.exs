defmodule Electric.Shapes.UnevaluableRowSignalTest do
  @moduledoc """
  A row excluded because its predicate could not be read is silent data loss: the snapshot holds it
  and its live changes never arrive. Routing records the same condition through
  `Filter.mark_routing_inexact/0`; this side had nothing, so the class could drop
  casts behind a lone warning.

  The signal has to discriminate an unreadable row from an ordinary non-match, and it has to be
  registered as a metric -- an event emitted into the void is what `move_in_triggered` already is.
  """
  use ExUnit.Case, async: false

  alias Electric.Replication.Eval.Parser
  alias Electric.Shapes.WhereClause
  alias Electric.StackSupervisor.Telemetry

  @event [:electric, :shape, :unevaluable_row_excluded]
  @refs %{["deletedAt"] => :timestamptz, ["value"] => :text}

  setup do
    test_pid = self()
    handler_id = "unevaluable-row-#{inspect(make_ref())}"

    :telemetry.attach(
      handler_id,
      @event,
      fn _event, measurements, metadata, _config ->
        send(test_pid, {:unevaluable_row, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  test "reports a row dropped because its predicate could not be read" do
    # Act
    refute includes?(~S|"deletedAt" > '2020-01-01 00:00:00+00'::timestamptz|, %{
             "deletedAt" => "infinity",
             "value" => "a"
           })

    # Assert
    assert_receive {:unevaluable_row, %{count: 1}, %{refs: refs}}
    assert refs[["deletedAt"]] == :timestamptz
  end

  test "stays quiet for a row that is merely excluded by the predicate" do
    # This is what makes the case above discriminate: both answers are `false`, and only one of
    # them is a row going missing.
    # Act
    refute includes?(~S|"value" = 'a'|, %{"deletedAt" => nil, "value" => "b"})

    # Assert
    refute_receive {:unevaluable_row, _, _}, 100
  end

  test "stays quiet for a row the predicate includes" do
    # Act
    assert includes?(~S|"value" = 'a'|, %{"deletedAt" => nil, "value" => "a"})

    # Assert
    refute_receive {:unevaluable_row, _, _}, 100
  end

  test "stays quiet when an unreadable value is only tested for nullity" do
    # The null tests answer without the value, so nothing is lost and nothing should be reported.
    # Act
    assert includes?(~S|"deletedAt" IS NOT NULL|, %{"deletedAt" => "infinity", "value" => "a"})

    # Assert
    refute_receive {:unevaluable_row, _, _}, 100
  end

  test "the event is registered as a metric, not emitted into the void" do
    # Assert
    assert Enum.any?(Telemetry.prometheus_metrics(), &(&1.event_name == @event)),
           "an emitted event nobody exports is exactly the gap this closes"
  end

  defp includes?(clause, record) do
    clause
    |> Parser.parse_and_validate_expression!(refs: @refs)
    |> WhereClause.includes_record?(record)
  end
end
