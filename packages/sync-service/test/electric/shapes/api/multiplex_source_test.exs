defmodule Electric.Shapes.Api.Multiplex.SourceTest do
  use ExUnit.Case, async: false

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup

  alias Electric.Shapes
  alias Electric.Shapes.Api
  alias Electric.Shapes.Api.Multiplex.Source

  @moduletag :tmp_dir

  @shape_a %{"table" => "items", "where" => "value = 'a'", "replica" => "full"}
  @shape_b %{"table" => "items", "where" => "value = 'b'", "replica" => "full"}

  setup [:with_unique_db, :with_basic_tables, :with_complete_stack]

  setup ctx do
    :ok =
      Electric.StatusMonitor.wait_until_active(ctx.stack_id,
        timeout: ExUnit.configuration()[:assert_receive_timeout]
      )

    api = Keyword.fetch!(build_router_opts(ctx), :api)

    %{
      api: api,
      handle_a: create_shape(api, ctx.stack_id, @shape_a),
      handle_b: create_shape(api, ctx.stack_id, @shape_b)
    }
  end

  test "accepts the definition its handle was created for", ctx do
    assert :ok = Source.validate_shape(ctx.api, ctx.handle_a, "0_0", @shape_a, nil)
    assert :ok = Source.validate_shape(ctx.api, ctx.handle_b, "0_0", @shape_b, nil)
  end

  test "refuses the handle of another live shape", ctx do
    assert :mismatch = Source.validate_shape(ctx.api, ctx.handle_b, "0_0", @shape_a, nil)
  end

  test "refuses a definition that differs from the handle's only by a projection parameter",
       ctx do
    definition = Map.put(@shape_a, "replica", "default")

    assert :mismatch = Source.validate_shape(ctx.api, ctx.handle_a, "0_0", definition, nil)
  end

  test "refuses a handle no shape has", ctx do
    assert :mismatch = Source.validate_shape(ctx.api, "123-456", "0_0", @shape_a, nil)
  end

  test "refuses a definition the shape endpoint would refuse", ctx do
    definition = Map.put(@shape_a, "table", "missing_table")

    assert :invalid = Source.validate_shape(ctx.api, ctx.handle_a, "0_0", definition, nil)
  end

  test "refuses a definition that asks to wait for an LSN, which a watch cannot answer", ctx do
    definition = Map.put(@shape_a, "wait_for_lsn", "1")

    assert :invalid = Source.validate_shape(ctx.api, ctx.handle_a, "0_0", definition, nil)
  end

  test "refuses a definition whose wait_for_lsn is empty", ctx do
    definition = Map.put(@shape_a, "wait_for_lsn", "")

    assert :invalid = Source.validate_shape(ctx.api, ctx.handle_a, "0_0", definition, nil)
  end

  # The shape endpoint answers a subset request at once with its rows, so a watch for one has
  # nothing to wait for. No subset cap is set, so only the refusal can fail these.
  test "refuses a definition carrying a subset in any of the query's spellings", ctx do
    subset = %{"order_by" => "value", "limit" => "1"}

    for definition <- [
          Map.put(@shape_a, "subset", subset),
          Map.put(@shape_a, "subset__order_by", "value"),
          Map.merge(@shape_a, %{"subset[order_by]" => "value", "subset[limit]" => "1"}),
          Map.put(@shape_a, "subset[]", "value")
        ] do
      assert Source.validate_shape(ctx.api, ctx.handle_a, "0_0", definition, nil) == :invalid,
             inspect(definition)
    end

    assert :ok = Source.validate_shape(ctx.api, ctx.handle_a, "0_0", @shape_a, nil)
  end

  # The query's decoding nests a bracketed key only when it ends with its closing bracket, so a
  # watch reads these as the plain shape and waits as one. The shape endpoint serves
  # `subset[trace` and `subsets` as the plain shape too, and refuses `subset[order_by]x` before it
  # parses the query, as another spelling of `subset[order_by]`: no handle comes from that request.
  test "arms a definition whose key starts like a subset field the query does not nest", ctx do
    for key <- ["subset[trace", "subset[order_by]x", "subsets"] do
      definition = Map.put(@shape_a, key, "value")

      assert Source.validate_shape(ctx.api, ctx.handle_a, "0_0", definition, nil) == :ok,
             inspect(definition)
    end
  end

  defp create_shape(api, stack_id, definition) do
    {:ok, %Api.Params{shape_definition: shape}} =
      Api.Params.validate(api, Map.put(definition, "offset", "-1"))

    {handle, _offset} = Shapes.get_or_create_shape_handle(stack_id, shape)
    handle
  end
end
