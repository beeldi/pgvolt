defmodule Electric.Shapes.EventRouter do
  @moduledoc """
  Routes replication events to shapes.

  This module is the routing half of the event pipeline: it decides which shapes a change
  reaches (via the Filter), excludes shapes added mid-transaction, and prepares shared
  conversions once per fan-out class. Turning those decisions into per-shape
  `TransactionFragment`s — with Begin/Commit bookkeeping — is the assembly half, in
  `Electric.Shapes.FragmentAssembler`.

  `event_by_shape_handle/2` composes the two halves for single-process use. The sharded
  collector calls `route_transaction_fragment/2` / `route_relation/2` and hands the decisions
  to per-shard assemblers instead, so both paths execute the same code.

  Transaction state is tracked to ensure:
  - Each shape receives Begin only once per transaction (on first relevant operation)
  - Each shape receives Commit only if it received operations in the transaction
  - Shapes added mid-transaction are skipped for that transaction
  - Shapes removed mid-transaction stop receiving events immediately
  """

  require Logger

  alias Electric.LogItems
  alias Electric.Replication.Changes
  alias Electric.Replication.Changes.PreparedChange
  alias Electric.Replication.Changes.Relation
  alias Electric.Replication.Changes.TransactionFragment
  alias Electric.ShapeCache.PureFileStorage.PayloadArena
  alias Electric.Shapes.EventRouter
  alias Electric.Shapes.Filter
  alias Electric.Shapes.FragmentAssembler
  alias Electric.Shapes.Shape

  defstruct filter: nil,
            in_txn: false,
            # Shapes added after the current transaction started (should be skipped)
            shapes_added_mid_txn: MapSet.new(),
            assembler: FragmentAssembler.new(),
            # shape_id => Shape.fanout_class/1, for shapes eligible to share a conversion.
            # Held here rather than recomputed per change because a shape's class is fixed for
            # its lifetime, and the per-change grouping is on the hot path.
            #
            # Stays empty while shared conversion is off, which is what makes the feature inert:
            # with no class there is no group to prepare, so every shape converts the change in
            # its own consumer, as it did before any of this existed.
            fanout_classes: %{},
            shared_conversion: false,
            # When set, prepared class payloads are appended once to this shared arena and
            # log items carry fixed-size refs, so consumers stop writing N copies of the
            # same bytes. Owned by the routing process: appends and syncs must only happen
            # on routing calls.
            payload_arena: nil,
            # :inline converts and encodes class payloads here, on the routing process.
            # :deferred emits `{:prepare, shape_ids}` group markers instead and leaves the
            # conversion to the shard workers (via `prepare_change_for_shape/4`), so the
            # encode cost parallelizes instead of serializing on the routing spine. Only
            # meaningful with shard workers; the unsharded path must stay :inline.
            prepare_mode: :inline

  @type t :: %EventRouter{}
  @type shape_id :: any()

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %EventRouter{
      filter: Filter.new(opts),
      assembler: FragmentAssembler.new(blocks?: Keyword.get(opts, :fragment_blocks, false)),
      payload_arena: Keyword.get(opts, :payload_arena),
      prepare_mode: Keyword.get(opts, :prepare_mode, :inline),
      shared_conversion: Keyword.get(opts, :shared_conversion, false)
    }
  end

  @doc """
  Exclude shapes from framed-block membership (their consumers need individual changes,
  e.g. for a subscribed materializer).
  """
  @spec mark_unblockable(t(), [shape_id()]) :: t()
  def mark_unblockable(%EventRouter{} = router, shape_ids) do
    %{router | assembler: FragmentAssembler.mark_unblockable(router.assembler, shape_ids)}
  end

  @spec add_shape(t(), shape_id(), Shape.t()) :: t()
  def add_shape(%EventRouter{} = router, shape_id, shape) do
    router = %EventRouter{
      router
      | filter: Filter.add_shape(router.filter, shape_id, shape),
        fanout_classes: put_fanout_class(router, shape_id, shape)
    }

    if router.in_txn do
      %{router | shapes_added_mid_txn: MapSet.put(router.shapes_added_mid_txn, shape_id)}
    else
      router
    end
  end

  defp put_fanout_class(router, shape_id, shape) do
    case fanout_class(router, shape) do
      nil -> router.fanout_classes
      class -> Map.put(router.fanout_classes, shape_id, class)
    end
  end

  defp fanout_class(%EventRouter{shared_conversion: false}, _shape), do: nil
  defp fanout_class(%EventRouter{}, shape), do: Shape.fanout_class(shape)

  @doc """
  `add_shape/3` for the collector's restore, which spreads the shapes it restores over several
  processes (`ShapeLogCollector.Restore`), each one with a copy of the router: the shape goes into
  the filter they share, and its fanout class, which goes in the router itself, is returned for
  the restore to put in with `put_fanout_classes/2` once they are done. `analysis` is the shape's
  `Filter.analyse_shape/1`, which the restore has run already for the shape's routing state.
  """
  @spec add_restored_shape(t(), shape_id(), Shape.t(), term()) :: term() | nil
  def add_restored_shape(%EventRouter{in_txn: false} = router, shape_id, shape, analysis) do
    Filter.add_shape(router.filter, shape_id, shape, analysis)
    fanout_class(router, shape)
  end

  @spec put_fanout_classes(t(), %{shape_id() => term()}) :: t()
  def put_fanout_classes(%EventRouter{} = router, classes),
    do: %{router | fanout_classes: Map.merge(router.fanout_classes, classes)}

  @spec remove_shape(t(), shape_id()) :: t()
  def remove_shape(%EventRouter{} = router, shape_id) do
    %EventRouter{
      router
      | filter: Filter.remove_shape(router.filter, shape_id),
        shapes_added_mid_txn: MapSet.delete(router.shapes_added_mid_txn, shape_id),
        assembler: FragmentAssembler.remove_shape(router.assembler, shape_id),
        fanout_classes: Map.delete(router.fanout_classes, shape_id)
    }
  end

  @spec has_shape?(t(), shape_id()) :: boolean()
  def has_shape?(%EventRouter{} = router, shape_id) do
    Filter.has_shape?(router.filter, shape_id)
  end

  @spec active_shapes(t()) :: MapSet.t(shape_id())
  def active_shapes(%EventRouter{} = router) do
    Filter.active_shapes(router.filter)
  end

  @spec event_by_shape_handle(t(), Relation.t() | TransactionFragment.t()) ::
          {%{shape_id() => Relation.t() | TransactionFragment.t()}, t()}
  def event_by_shape_handle(%EventRouter{} = router, %Relation{} = relation) do
    {shape_ids, router} = route_relation(router, relation)
    {Map.new(shape_ids, fn shape_id -> {shape_id, relation} end), router}
  end

  def event_by_shape_handle(%EventRouter{} = router, %TransactionFragment{} = txn_fragment) do
    {decisions, router} = route_transaction_fragment(router, txn_fragment)

    {fragments, assembler} =
      FragmentAssembler.assemble(router.assembler, txn_fragment, decisions)

    {fragments, %{router | assembler: assembler}}
  end

  @doc """
  The routing half on its own: which shapes each change reaches, with shared conversions
  prepared once per fan-out class.

  Returns `{decisions, router}` where each decision is `{change, prepared_groups,
  unprepared_shape_ids}` in change order, ready for `FragmentAssembler.assemble/3` — whole or
  sliced per shard. Each prepared conversion appears exactly once, paired with the shapes
  that use it: decisions cross process boundaries (ETS, messages), and those copies expand
  term sharing — a per-shape map holding one struct under 22 keys would be materialized as
  22 full copies of the converted record on every hop.
  """
  @spec route_transaction_fragment(t(), TransactionFragment.t()) ::
          {[FragmentAssembler.decision()], t()}
  def route_transaction_fragment(%EventRouter{} = router, %TransactionFragment{} = txn_fragment) do
    router = maybe_start_transaction(router, txn_fragment)

    {decisions, router} =
      Enum.map_reduce(txn_fragment.changes, router, fn change, router ->
        {affected_shapes, exact?} = find_affected_shapes_for_change(router, change)

        # Sharing a conversion takes routing as membership. When routing could not evaluate
        # a predicate it includes the shapes it could not rule out, so such a change goes to
        # its consumers unprepared and is re-evaluated there, as it was before sharing.
        {prepared_groups, prepared_ids, router} =
          if exact? do
            prepare_shared_changes(router, change, affected_shapes, txn_fragment)
          else
            {[], [], router}
          end

        unprepared =
          case prepared_ids do
            [] ->
              MapSet.to_list(affected_shapes)

            _ ->
              affected_shapes |> MapSet.difference(MapSet.new(prepared_ids)) |> MapSet.to_list()
          end

        {{change, prepared_groups, unprepared}, router}
      end)

    router = router |> maybe_end_transaction(txn_fragment) |> maybe_sync_arena(txn_fragment)

    {decisions, router}
  end

  # Any ref a consumer can ever flush must be backed by durable arena bytes, so the arena is
  # datasynced at every commit fragment BEFORE the decisions leave this process. Refs from
  # mid-transaction fragments are covered by recovery: shape logs are trimmed to their last
  # committed transaction on boot and the replication slot was never acked past it, so those
  # transactions are resent and re-appended.
  defp maybe_sync_arena(%EventRouter{payload_arena: nil} = router, _txn_fragment), do: router
  defp maybe_sync_arena(router, %TransactionFragment{commit: nil}), do: router

  defp maybe_sync_arena(%EventRouter{} = router, _txn_fragment),
    do: %{router | payload_arena: PayloadArena.sync!(router.payload_arena)}

  @spec route_relation(t(), Relation.t()) :: {[shape_id()], t()}
  def route_relation(%EventRouter{} = router, %Relation{} = relation) do
    {router.filter |> Filter.affected_shapes(relation) |> MapSet.to_list(), router}
  end

  # Transaction state management

  defp maybe_start_transaction(router, %TransactionFragment{has_begin?: false}), do: router

  defp maybe_start_transaction(%EventRouter{} = router, %TransactionFragment{has_begin?: true}) do
    %{router | shapes_added_mid_txn: MapSet.new(), in_txn: true}
  end

  defp maybe_end_transaction(router, %TransactionFragment{commit: nil}), do: router

  defp maybe_end_transaction(%EventRouter{} = router, %TransactionFragment{}) do
    %{router | shapes_added_mid_txn: MapSet.new(), in_txn: false}
  end

  # Shape eligibility

  defp find_affected_shapes_for_change(router, change) do
    {affected, exact?} = Filter.affected_shapes_exact(router.filter, change)
    {MapSet.difference(affected, router.shapes_added_mid_txn), exact?}
  end

  # Convert and encode once per fan-out class instead of once per participating shape.
  #
  # Restricted to inserts and deletes, where a shape's membership is one boolean that routing has
  # already decided, so every shape in a class converts the change identically. An update is not
  # shareable on the routed set: routing unions the shapes matching the old record with those
  # matching the new one, and a shape matching only the old sees a move-out delete while a shape
  # matching only the new sees a move-in insert. Recovering that distinction means evaluating both
  # records per shape, which is the work this is trying to avoid.
  #
  # A class is also only worth preparing when more than one shape is in it. For a single shape the
  # work is identical either way, and doing it here would move it out of that shape's own consumer
  # and into the router, which is one process shared by every shape on the node.
  defp prepare_shared_changes(
         %EventRouter{fanout_classes: classes} = router,
         change,
         shapes,
         transaction
       )
       when classes != %{} and
              (is_struct(change, Changes.NewRecord) or is_struct(change, Changes.DeletedRecord)) do
    shapes
    |> Enum.reduce(%{}, fn shape_id, groups ->
      case classes do
        %{^shape_id => class} -> Map.update(groups, class, [shape_id], &[shape_id | &1])
        _ -> groups
      end
    end)
    |> Enum.reduce({[], [], router}, fn
      {_class, [_only_one]}, acc ->
        acc

      {_class, shape_ids},
      {groups, prepared_ids, %EventRouter{prepare_mode: :deferred} = router} ->
        {[{:prepare, shape_ids} | groups], shape_ids ++ prepared_ids, router}

      {_class, [representative | _] = shape_ids}, {groups, prepared_ids, router} = acc ->
        case prepare_change(router, representative, change, transaction) do
          nil ->
            acc

          {prepared_change, payload_arena} ->
            {[{prepared_change, shape_ids} | groups], shape_ids ++ prepared_ids,
             %{router | payload_arena: payload_arena}}
        end
    end)
  end

  defp prepare_shared_changes(router, _change, _shapes, _transaction), do: {[], [], router}

  # `Filter.get_shape/2` returns nil for a shape id the filter no longer holds, and the rescue
  # in `prepare_change_for_shape/4` reads `shape.root_table` to build its message -- so a nil
  # shape would raise inside the rescue and take the router down, which is the one thing that
  # rescue exists to prevent. Fall back to per-shape conversion instead, as the deferred twin
  # in `RouterWorker.slice_group/4` already does.
  defp prepare_change(%EventRouter{} = router, shape_id, change, transaction) do
    case Filter.get_shape(router.filter, shape_id) do
      nil -> nil
      shape -> prepare_change_for_shape(shape, change, transaction, router.payload_arena)
    end
  end

  @doc """
  Convert and encode one change for a whole fan-out class, on behalf of `shape`.

  Every shape in a class produces the identical result, so any member works as the
  representative — shard workers running in :deferred mode call this with a member from
  their own slice. Returns `{prepared_change, payload_arena}` or nil to fall back to
  per-shape conversion in the consumer.
  """
  @spec prepare_change_for_shape(Shape.t(), struct(), TransactionFragment.t(), term()) ::
          {PreparedChange.t(), term()} | nil
  def prepare_change_for_shape(shape, change, transaction, payload_arena) do
    do_prepare_change(shape, change, transaction, payload_arena)
  rescue
    # Converting and encoding here is an optimisation, and it must not widen the blast radius of a
    # change that cannot be encoded. Consumers do this work in their own process, so a failure
    # takes down one shape; the router serves every shape on the node. Falling back routes the
    # change unprepared and the consumer raises exactly as it does today, in the right process.
    # A partially-appended arena writer is discarded with the rest of the attempt; its flushes
    # are positioned writes, so the retained writer simply overwrites the orphan bytes.
    #
    # Logged because nothing else can report it. An arena whose handle has died fails every
    # append from here on, and each failure is answered by keeping the previous writer -- so
    # `sync!/1` never touches the dead handle, no commit ever raises, and the whole point of
    # running this fork silently stops applying. One line per failed class is a storm only when
    # the failure is systemic, which is precisely when it needs to be visible. It names the
    # exception and where it was raised, not its message or arguments, which can quote the row:
    # an encode error quotes the string it could not encode.
    error ->
      Logger.warning(
        "Shared conversion failed for #{inspect(shape.root_table)}, falling back to per-shape " <>
          "conversion: #{inspect(error.__struct__)}#{raised_in(__STACKTRACE__)}"
      )

      nil
  end

  defp raised_in([{module, function, args_or_arity, _location} | _]) do
    arity = if is_list(args_or_arity), do: length(args_or_arity), else: args_or_arity
    " in #{Exception.format_mfa(module, function, arity)}"
  end

  defp raised_in(_stacktrace), do: ""

  defp do_prepare_change(shape, change, transaction, payload_arena) do
    # Shapes in a class have no move tags, so `convert_change/3` never reads stack_id or
    # shape_handle and there is nothing shape-specific left to pass.
    #
    # Anything other than exactly one converted change falls back to per-shape conversion rather
    # than being modelled here: no match is nothing to share, and a conversion that fans out to
    # several changes is not a shape the consumer's own path is written to expect either.
    case Shape.convert_routed_change(shape, change) do
      [converted] ->
        {encoded, total_size} =
          LogItems.encode_changes([converted], transaction.xid, transaction.full_xid, shape)

        {log_items, payload_arena} =
          Enum.map_reduce(encoded, payload_arena, fn {offset, key, op, json}, arena ->
            case arena do
              nil ->
                {{offset, key, op, json}, nil}

              arena ->
                {arena, ref} = PayloadArena.append!(arena, key, json)
                {{offset, key, op, json, ref}, arena}
            end
          end)

        {%PreparedChange{
           change: converted,
           log_items: log_items,
           total_size: total_size
         }, payload_arena}

      _ ->
        nil
    end
  end
end
