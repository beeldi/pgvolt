defmodule Electric.Shapes.FullTransactionSourcePositionsTest do
  use ExUnit.Case, async: true

  alias Electric.Shapes.Api
  alias Electric.Shapes.Shape
  alias Electric.StackConfig

  import Support.ComponentSetup

  @inspector Support.StubInspector.new(
               tables: ["users", "memberships"],
               columns: [
                 %{name: "id", type: "int8", pk_position: 0, type_id: {20, 1}},
                 %{name: "value", type: "text", pk_position: nil, type_id: {25, 1}}
               ]
             )

  setup [:with_stack_id_from_test]

  test "accepts the explicit producer feature flag without enabling it by default" do
    assert Electric.Config.parse_feature_flags("full_xid_source_positions") == [
             "full_xid_source_positions"
           ]

    refute "full_xid_source_positions" in Electric.Config.default(:feature_flags)
  end

  setup %{stack_id: stack_id} do
    StackConfig.put(stack_id, :full_xid_source_positions_ready, true)
    StackConfig.put(stack_id, :full_xid_source_generation, "source-generation-a")

    [
      api: %Api{
        stack_id: stack_id,
        inspector: @inspector,
        feature_flags: ["full_xid_source_positions"]
      }
    ]
  end

  test "keeps legacy identity stable and separates full transaction generations", %{api: api} do
    assert {:ok, legacy} = Api.Params.validate(api, %{table: "users", offset: "-1"})

    assert {:ok, full} =
             Api.Params.validate(api, %{
               table: "users",
               offset: "-1",
               source_positions: "full_xid"
             })

    shape = legacy.shape_definition

    expected =
      {:shape, {shape.root_table_id, shape.root_table}, shape.root_pk, nil,
       shape.selected_columns, shape.explicitly_selected_columns, [:selects_all_columns],
       shape.replica, shape.log_mode}

    assert Shape.comparable(shape) == expected
    assert full.shape_definition.source_generation == "source-generation-a"
    refute Shape.comparable(shape) == Shape.comparable(full.shape_definition)
    refute Shape.hash(shape) == Shape.hash(full.shape_definition)

    StackConfig.put(api.stack_id, :full_xid_source_generation, "source-generation-b")

    assert {:ok, next} =
             Api.Params.validate(api, %{
               table: "users",
               offset: "-1",
               source_positions: "full_xid"
             })

    refute Shape.comparable(full.shape_definition) == Shape.comparable(next.shape_definition)
  end

  test "persists the mode and source generation through dependencies and recovery", %{api: api} do
    assert {:ok, params} =
             Api.Params.validate(api, %{
               table: "users",
               offset: "-1",
               source_positions: "full_xid",
               where: "id IN (SELECT id FROM memberships)"
             })

    shape = params.shape_definition

    assert [%Shape{source_positions: :full_xid, source_generation: "source-generation-a"}] =
             shape.shape_dependencies

    assert {:ok, recovered} =
             shape |> Jason.encode!() |> Jason.decode!() |> Shape.from_json_safe()

    assert Shape.comparable(shape) == Shape.comparable(recovered)

    assert [%Shape{source_positions: :full_xid, source_generation: "source-generation-a"}] =
             recovered.shape_dependencies
  end

  test "rejects full transaction mode before decoder provenance is ready", %{api: api} do
    StackConfig.put(api.stack_id, :full_xid_source_positions_ready, false)

    assert {:error, %{status: 503}} =
             Api.Params.validate(api, %{
               table: "users",
               offset: "-1",
               source_positions: "full_xid"
             })

    assert {:ok, _legacy} = Api.Params.validate(api, %{table: "users", offset: "-1"})
  end

  test "retains legacy Erlang shape records written before the source protocol fields existed", %{
    api: api
  } do
    shape = Shape.new!("users", inspector: @inspector)

    stored =
      shape |> Map.drop([:source_positions, :source_generation]) |> :erlang.term_to_binary()

    recovered = :erlang.binary_to_term(stored)
    assert Shape.comparable(recovered) == Shape.comparable(shape)
    assert Shape.hash(recovered) == Shape.hash(shape)

    assert {:ok, %{source_positions: :legacy}} =
             Api.Params.validate(%{api | shape: recovered}, %{offset: "-1"})

    assert Jason.encode!(recovered) =~ "legacy"
  end

  test "rejects predefined shapes from an earlier database generation", %{api: api} do
    assert {:ok, params} =
             Api.Params.validate(api, %{
               table: "users",
               offset: "-1",
               source_positions: "full_xid"
             })

    StackConfig.put(api.stack_id, :full_xid_source_generation, "source-generation-b")

    assert {:error, %{status: 503}} =
             Api.Params.validate(%{api | shape: params.shape_definition}, %{offset: "-1"})
  end

  test "shape construction cannot bypass generation and compaction requirements" do
    assert {:error, {:source_positions, _}} =
             Shape.new("users", inspector: @inspector, source_positions: :full_xid)

    assert {:error, {:source_positions, _}} =
             Shape.new("users",
               inspector: @inspector,
               source_positions: :full_xid,
               source_generation: "source-generation-a",
               storage: %{compaction: :enabled}
             )
  end

  test "rejects a disabled capability and metadata stripping compaction", %{api: api} do
    params = %{table: "users", offset: "-1", source_positions: "full_xid"}
    assert {:error, %{status: 426}} = Api.Params.validate(%{api | feature_flags: []}, params)

    assert {:error, %{status: 400}} =
             Api.Params.validate(api, Map.put(params, :experimental_compaction, true))
  end

  test "cannot advertise a full transaction request using a predefined legacy shape", %{api: api} do
    shape = Shape.new!("users", inspector: @inspector)

    assert {:error, %{status: 426}} =
             Api.Params.validate(%{api | shape: shape}, %{
               offset: "-1",
               source_positions: "full_xid"
             })
  end

  test "emits captured source generation on successful regular and subset responses", %{api: api} do
    assert {:ok, params} =
             Api.Params.validate(api, %{
               table: "users",
               offset: "-1",
               source_positions: "full_xid"
             })

    StackConfig.put(api.stack_id, :full_xid_source_generation, "source-generation-b")

    for response_type <- [:normal_log, :subset] do
      response =
        Api.Response.final(%Api.Response{
          api: api,
          params: params,
          shape_definition: params.shape_definition,
          response_type: response_type,
          body: [],
          status: 200
        })

      conn = Api.Response.send(Plug.Test.conn(:get, "/v1/shape"), response)
      assert Plug.Conn.get_resp_header(conn, "electric-source-positions") == ["mvcc-full-xid-v1"]

      assert Plug.Conn.get_resp_header(conn, "electric-source-generation") == [
               "source-generation-a"
             ]
    end
  end

  test "never advertises capability on a legacy shape or failed response", %{api: api} do
    assert {:ok, params} = Api.Params.validate(api, %{table: "users", offset: "-1"})

    for status <- [200, 400, 503] do
      response =
        Api.Response.final(%Api.Response{
          api: api,
          params: %{params | source_positions: :full_xid},
          shape_definition: params.shape_definition,
          body: [],
          status: status
        })

      conn = Api.Response.send(Plug.Test.conn(:get, "/v1/shape"), response)
      assert Plug.Conn.get_resp_header(conn, "electric-source-positions") == []
      assert Plug.Conn.get_resp_header(conn, "electric-source-generation") == []
    end
  end
end
