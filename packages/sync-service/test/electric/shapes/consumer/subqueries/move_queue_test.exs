defmodule Electric.Shapes.Consumer.Subqueries.MoveQueueTest do
  use ExUnit.Case, async: true

  alias Electric.Shapes.Consumer.Subqueries.MoveQueue

  @dep 0

  test "drops redundant move outs for values absent from the base view" do
    queue = MoveQueue.enqueue(MoveQueue.new(), @dep, %{move_out: [{1, "1"}]}, MapSet.new())

    assert %MoveQueue{move_out: empty_out, move_in: empty_in} = queue
    assert empty_out == %{}
    assert empty_in == %{}
  end

  test "drops redundant move ins for values already present in the base view" do
    queue = MoveQueue.enqueue(MoveQueue.new(), @dep, %{move_in: [{1, "1"}]}, MapSet.new([1]))

    assert %MoveQueue{move_out: empty_out, move_in: empty_in} = queue
    assert empty_out == %{}
    assert empty_in == %{}
  end

  test "cancels a pending move in with a later move out for the same value" do
    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(@dep, %{move_in: [{1, "1"}]}, MapSet.new())
      |> MoveQueue.enqueue(@dep, %{move_out: [{1, "1"}]}, MapSet.new())

    assert %MoveQueue{move_out: empty_out, move_in: empty_in} = queue
    assert empty_out == %{}
    assert empty_in == %{}
  end

  test "cancels a pending move out with a later move in for the same value" do
    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(@dep, %{move_out: [{1, "1"}]}, MapSet.new([1]))
      |> MoveQueue.enqueue(@dep, %{move_in: [{1, "1"}]}, MapSet.new([1]))

    assert %MoveQueue{move_out: empty_out, move_in: empty_in} = queue
    assert empty_out == %{}
    assert empty_in == %{}
  end

  test "merges repeated move ins and keeps the terminal tuple" do
    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(@dep, %{move_in: [{1, "01"}]}, MapSet.new())
      |> MoveQueue.enqueue(@dep, %{move_in: [{1, "1"}], move_out: []}, MapSet.new())

    assert %MoveQueue{move_in: %{0 => {[{1, "1"}], _, _}}, move_out: empty_out} = queue
    assert empty_out == %{}
  end

  test "merges repeated move outs and keeps the terminal tuple" do
    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(@dep, %{move_out: [{1, "01"}]}, MapSet.new([1]))
      |> MoveQueue.enqueue(@dep, %{move_out: [{1, "1"}], move_in: []}, MapSet.new([1]))

    assert %MoveQueue{move_out: %{0 => {[{1, "1"}], _, _}}, move_in: empty_in} = queue
    assert empty_in == %{}
  end

  test "orders surviving move outs before move ins" do
    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(@dep, %{move_in: [{2, "2"}]}, MapSet.new([1]))
      |> MoveQueue.enqueue(@dep, %{move_out: [{1, "1"}]}, MapSet.new([1]))

    assert %MoveQueue{
             move_out: %{0 => {[{1, "1"}], _, _}},
             move_in: %{0 => {[{2, "2"}], _, _}}
           } = queue
  end

  test "uses the provided base view when reducing buffering follow-up moves" do
    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(@dep, %{move_in: [{2, "2"}]}, MapSet.new([1]))
      |> MoveQueue.enqueue(@dep, %{move_out: [{2, "2"}]}, MapSet.new([1]))

    assert %MoveQueue{move_out: empty_out, move_in: empty_in} = queue
    assert empty_out == %{}
    assert empty_in == %{}
  end

  test "pop_next returns the whole move out batch before the move in batch" do
    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(@dep, %{move_in: [{2, "2"}], move_out: [{1, "1"}]}, MapSet.new([1]))
      |> MoveQueue.enqueue(@dep, %{move_in: [{3, "3"}]}, MapSet.new([1]))

    assert {{:move_out, 0, [{1, "1"}], [], []}, queue} = MoveQueue.pop_next(queue)
    assert map_size(queue.move_out) == 0
    assert {[{2, "2"}, {3, "3"}], _, _} = Map.fetch!(queue.move_in, 0)

    assert {{:move_in, 0, [{2, "2"}, {3, "3"}], [], []}, queue} = MoveQueue.pop_next(queue)
    assert queue.move_out == %{}
    assert queue.move_in == %{}
    assert nil == MoveQueue.pop_next(queue)
  end

  test "pairs both directions from a fresh transaction with full provenance" do
    queue =
      MoveQueue.enqueue(
        MoveQueue.new(),
        @dep,
        %{move_out: [{1, "1"}], move_in: [{2, "2"}], txids: [10], txids8: [4_294_967_306]},
        MapSet.new([1])
      )

    {move, queue} = MoveQueue.pop_next(queue)

    assert {{:replace, 0, {[{1, "1"}], [{2, "2"}]}, [10], [4_294_967_306]}, queue} =
             MoveQueue.pair_with_move_in(move, queue)

    assert nil == MoveQueue.pop_next(queue)
  end

  test "leaves both directions separate after a later payload merges into a replacement" do
    view = MapSet.new([1])

    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(@dep, %{move_out: [{1, "1"}], move_in: [{2, "2"}], txids: [10]}, view)
      |> MoveQueue.enqueue(@dep, %{move_in: [{3, "3"}], txids: [20]}, view)
      |> MoveQueue.enqueue(1, %{move_in: [{5, "5"}], txids: [20]}, MapSet.new())

    assert {{:move_out, 0, _, _, _} = move, queue} = MoveQueue.pop_next(queue)
    assert {^move, ^queue} = MoveQueue.pair_with_move_in(move, queue)
    assert {{:move_in, 0, [{2, "2"}, {3, "3"}], [10, 20], []}, _} = MoveQueue.pop_next(queue)
  end

  test "pairs an addition with a later removal when no other dependency has a move" do
    view = MapSet.new([1])

    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(@dep, %{move_in: [{2, "2"}], txids: [10]}, view)
      |> MoveQueue.enqueue(@dep, %{move_out: [{1, "1"}], txids: [20]}, view)

    assert MoveQueue.removal_after_addition?(queue, @dep)
    {move, queue} = MoveQueue.pop_next(queue)

    assert {{:replace, 0, {[{1, "1"}], [{2, "2"}]}, [10, 20], []}, queue} =
             MoveQueue.pair_with_move_in(move, queue)

    refute MoveQueue.removal_after_addition?(queue, @dep)
    assert nil == MoveQueue.pop_next(queue)
  end

  test "keeps a removal that reaches a queued addition apart from another dependency's move" do
    view = MapSet.new([1])

    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(@dep, %{move_in: [{2, "2"}], txids: [10]}, view)
      |> MoveQueue.enqueue(1, %{move_in: [{5, "5"}], txids: [10]}, MapSet.new())
      |> MoveQueue.enqueue(@dep, %{move_out: [{1, "1"}], txids: [20]}, view)

    assert {{:move_out, 0, [{1, "1"}], [20], []} = move, queue} = MoveQueue.pop_next(queue)
    assert {^move, ^queue} = MoveQueue.pair_with_move_in(move, queue)
    assert MoveQueue.removal_after_addition?(queue, @dep)
  end

  test "marks a removal only when it can follow a queued addition" do
    view = MapSet.new([1, 3])
    removal_first = MoveQueue.enqueue(MoveQueue.new(), @dep, %{move_out: [{3, "3"}]}, view)

    refute removal_first
           |> MoveQueue.enqueue(@dep, %{move_in: [{2, "2"}]}, view)
           |> MoveQueue.removal_after_addition?(@dep)

    assert removal_first
           |> MoveQueue.enqueue(@dep, %{move_out: [{1, "1"}], move_in: [{2, "2"}]}, view)
           |> MoveQueue.removal_after_addition?(@dep)
  end

  test "accumulates txids from successive enqueues per dependency" do
    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(
        @dep,
        %{move_in: [{1, "1"}], txids: [10]},
        MapSet.new()
      )
      |> MoveQueue.enqueue(
        @dep,
        %{move_in: [{2, "2"}], txids: [20]},
        MapSet.new()
      )

    assert {{:move_in, 0, _, [10, 20], []}, _queue} = MoveQueue.pop_next(queue)
  end

  test "preserves distinct full transaction IDs with the same wrapped transaction ID" do
    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(
        @dep,
        %{move_in: [{1, "1"}], txids: [10], txids8: [4_294_967_306]},
        MapSet.new()
      )
      |> MoveQueue.enqueue(
        @dep,
        %{move_in: [{2, "2"}], txids: [10], txids8: [8_589_934_602]},
        MapSet.new()
      )

    assert {{:move_in, 0, _, [10], [4_294_967_306, 8_589_934_602]}, _queue} =
             MoveQueue.pop_next(queue)
  end

  test "length counts queued values across both batches" do
    queue =
      MoveQueue.new()
      |> MoveQueue.enqueue(@dep, %{move_in: [{2, "2"}], move_out: [{1, "1"}]}, MapSet.new([1]))
      |> MoveQueue.enqueue(@dep, %{move_in: [{3, "3"}]}, MapSet.new([1]))

    assert 3 == MoveQueue.length(queue)
  end
end
