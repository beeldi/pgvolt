defmodule Electric.Shapes.Consumer.EventHandlerBuilder do
  # Builds the initial event handler and ordered setup effects for a consumer shape.

  alias Electric.Shapes.Consumer.EventHandler
  alias Electric.Shapes.Consumer.Materializer
  alias Electric.Shapes.Consumer.SetupEffects
  alias Electric.Shapes.Consumer.State
  alias Electric.Shapes.Consumer.Subqueries.MoveBroadcast
  alias Electric.Shapes.Consumer.Subqueries.Views
  alias Electric.Shapes.DnfPlan
  alias Electric.Shapes.Shape

  @spec build(State.t(), :create | :restore) ::
          {:ok, EventHandler.t(), [SetupEffects.t()]}
  def build(%State{shape: %Shape{shape_dependencies_handles: dep_handles}} = state, action)
      when dep_handles != [] do
    {:ok, dnf_plan} = DnfPlan.compile(state.shape)
    dnf_plan = MoveBroadcast.allow_direct_text_pairs(dnf_plan, state.shape, state.stack_id)

    {views, dep_handle_to_ref, dep_index_to_ref} =
      dep_handles
      |> Enum.with_index()
      |> Enum.reduce({%{}, %{}, %{}}, fn {handle, index},
                                         {views, handle_mapping, index_mapping} ->
        materializer_opts = %{stack_id: state.stack_id, shape_handle: handle}
        :ok = Materializer.wait_until_ready(materializer_opts)
        ref = ["$sublink", Integer.to_string(index)]
        keys = Materializer.get_link_view(materializer_opts)
        view = Views.new(keys, state.shape.where.used_refs[ref])

        {Map.put(views, ref, view), Map.put(handle_mapping, handle, {index, ref}),
         Map.put(index_mapping, index, ref)}
      end)

    buffer_max_transactions =
      Electric.StackConfig.lookup(
        state.stack_id,
        :subquery_buffer_max_transactions,
        Electric.Config.default(:subquery_buffer_max_transactions)
      )

    handler = %EventHandler.Subqueries.Steady{
      shape_info: %Electric.Shapes.Consumer.Subqueries.ShapeInfo{
        shape: state.shape,
        stack_id: state.stack_id,
        shape_handle: state.shape_handle,
        dnf_plan: dnf_plan,
        ref_resolver:
          Electric.Shapes.Consumer.Subqueries.RefResolver.new(dep_handle_to_ref, dep_index_to_ref),
        buffer_max_transactions: buffer_max_transactions
      },
      views: views
    }

    {:ok, handler,
     [%SetupEffects.SubscribeShape{action: action}, %SetupEffects.SeedSubqueryIndex{}]}
  end

  def build(%State{} = state, action) do
    handler = %EventHandler.Default{
      shape: state.shape,
      stack_id: state.stack_id,
      shape_handle: state.shape_handle
    }

    {:ok, handler, [%SetupEffects.SubscribeShape{action: action}]}
  end
end
