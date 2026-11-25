defmodule Electric.CoreSupervisor do
  @moduledoc """
  A supervisor that starts the core components of the Electric system.
  This is divided into two subsystems:
  1. The connection subsystem (processes that may exit on a connection failure), started with Connection.Supervisor
  2. The shape subsystem (processes that are resilient to connection failures), started with Shapes.Supervisor
  """

  use Supervisor, restart: :transient, significant: true

  def name(stack_ref) do
    Electric.ProcessRegistry.name(stack_ref, __MODULE__)
  end

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: name(opts))
  end

  @impl true
  def init(opts) do
    stack_id = Keyword.fetch!(opts, :stack_id)

    Process.set_label({:core_supervisor, stack_id})
    Logger.metadata(stack_id: stack_id)
    Electric.Telemetry.Sentry.set_tags_context(stack_id: stack_id)

    connection_manager_opts = Keyword.fetch!(opts, :connection_manager_opts)

    children = [
      {Electric.Connection.Supervisor, connection_manager_opts}
    ]

    Supervisor.init(children, strategy: :one_for_one, auto_shutdown: :any_significant)
  end

  # The publication manager's drains share the admin pool with every other user of it. Sized as
  # `Connection.Manager` sizes the pool it starts.
  defp admin_pool_size(nil), do: nil

  defp admin_pool_size(pool_opts),
    do: Electric.Connection.Manager.pool_sizes(Keyword.get(pool_opts, :pool_size, 2)).admin

  @doc """
  This function is supposed to be called from Connection.Manager at the right point in its
  initialization sequence.
  """
  def start_shapes_supervisor(opts) do
    stack_id = Keyword.fetch!(opts, :stack_id)
    shape_cache_opts = Keyword.fetch!(opts, :shape_cache_opts)
    replication_opts = Keyword.fetch!(opts, :replication_opts)
    inspector = Keyword.fetch!(opts, :inspector)
    persistent_kv = Keyword.fetch!(opts, :persistent_kv)
    tweaks = Keyword.fetch!(opts, :tweaks)
    max_shapes = Keyword.fetch!(opts, :max_shapes)

    consumer_supervisor_spec =
      {Electric.Shapes.DynamicConsumerSupervisor,
       [
         stack_id: stack_id,
         max_shapes: max_shapes,
         partitions: Keyword.get(tweaks, :consumer_partitions)
       ]}

    shape_cache_spec = {Electric.ShapeCache, shape_cache_opts}

    publication_manager_spec =
      {Electric.Replication.PublicationManager,
       stack_id: stack_id,
       publication_name: Keyword.fetch!(replication_opts, :publication_name),
       manual_table_publishing?: Keyword.fetch!(opts, :manual_table_publishing?),
       db_pool: Electric.Connection.Manager.admin_pool(stack_id),
       admin_pool_size: admin_pool_size(Keyword.get(opts, :pool_opts)),
       persistent_kv: persistent_kv,
       database_timeline: Keyword.fetch!(opts, :database_timeline),
       update_debounce_timeout: Keyword.get(tweaks, :publication_alter_debounce_ms, 0),
       refresh_period: Keyword.get(tweaks, :publication_refresh_period, 60_000)}

    shape_log_collector_spec =
      {Electric.Replication.ShapeLogCollector.Supervisor,
       stack_id: stack_id, inspector: inspector, persistent_kv: persistent_kv}

    schema_reconciler_spec =
      {Electric.Replication.SchemaReconciler,
       stack_id: stack_id,
       inspector: inspector,
       period: Keyword.get(tweaks, :schema_reconciler_period, 60_000)}

    expiry_manager_spec =
      {Electric.ShapeCache.ExpiryManager, max_shapes: max_shapes, stack_id: stack_id}

    # Never restarted in place: Postgres resends the transactions its consumers had not
    # flushed only to a new replication connection. So when it stops on its own (on any exit of
    # a child, which it does not restart), the core stops with it (`significant: true` with
    # `auto_shutdown: :any_significant`), and so does the stack. `stop_shapes_supervisor/1`
    # does not count.
    child_spec =
      Supervisor.child_spec(
        {
          Electric.Shapes.Supervisor,
          stack_id: stack_id,
          consumer_supervisor: consumer_supervisor_spec,
          shape_cache: shape_cache_spec,
          publication_manager: publication_manager_spec,
          log_collector: shape_log_collector_spec,
          schema_reconciler: schema_reconciler_spec,
          expiry_manager: expiry_manager_spec
        },
        restart: :temporary,
        significant: true
      )

    Supervisor.start_child(name(opts), child_spec)
  end

  @doc """
  Stops the Shapes.Supervisor if it's currently running.

  This is useful when you need to reset storage before starting a new supervisor.
  Returns :ok if the supervisor was stopped or wasn't running.
  """
  def stop_shapes_supervisor(opts) do
    # A temporary child's spec goes away with the process, so there is nothing to delete.
    case Supervisor.terminate_child(name(opts), Electric.Shapes.Supervisor) do
      :ok ->
        :ok

      {:error, :not_found} ->
        :ok
    end
  end
end
