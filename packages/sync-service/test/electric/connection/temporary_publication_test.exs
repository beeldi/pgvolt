defmodule Electric.Connection.TemporaryPublicationTest do
  use ExUnit.Case, async: true

  import Support.DbSetup

  alias Electric.Replication.PublicationManager
  alias Electric.Shapes.Shape
  alias Electric.StackSupervisor
  alias Electric.StatusMonitor

  @moduletag :tmp_dir

  test "pairs a random temporary slot with a default publication and honors configured names",
       ctx do
    opts = configuration_opts(ctx, "names")

    %{slot_name: "electric_slot_" <> suffix, publication_name: publication} =
      opts
      |> Electric.Application.configuration()
      |> Keyword.fetch!(:replication_opts)
      |> Map.new()

    assert publication == "electric_publication_#{suffix}"

    assert Electric.Application.configuration([publication_name: "custom"] ++ opts)[
             :replication_opts
           ][:publication_name] == "custom"

    for override <- [[manual_table_publishing?: true], [replication_slot_temporary?: false]] do
      assert Electric.Application.configuration(override ++ opts)[:replication_opts][
               :publication_name
             ] == "electric_publication_test"
    end
  end

  describe "Postgres publication lifecycle" do
    setup :with_unique_db

    test "overlapping temporary slots reconcile their own publications", ctx do
      for table <- ~w(items other_items) do
        Postgrex.query!(ctx.db_conn, "CREATE TABLE #{table} (id int primary key)", [])
      end

      first = start_stack(ctx, "first")
      add_shape(first, "items")

      second = start_stack(ctx, "second")
      add_shape(second, "other_items")

      assert publication_tables(ctx.db_conn, first) == [["items"]]
      assert publication_tables(ctx.db_conn, second) == [["other_items"]]
    end

    test "a clean stop removes only the generated publication", ctx do
      explicit = start_stack(ctx, "explicit", publication_name: "explicit_publication")
      generated = start_stack(ctx, "generated")

      stop_supervised!(generated[:stack_id])

      assert %{rows: [["explicit_publication"]]} =
               Postgrex.query!(
                 ctx.db_conn,
                 "SELECT pubname FROM pg_publication ORDER BY pubname",
                 []
               )

      stop_supervised!(explicit[:stack_id])

      assert %{rows: [["explicit_publication"]]} =
               Postgrex.query!(ctx.db_conn, "SELECT pubname FROM pg_publication", [])
    end
  end

  defp configuration_opts(ctx, id) do
    [
      stack_id: "#{ctx.module}:#{ctx.test}:#{id}",
      storage_dir: Path.join(ctx.tmp_dir, id),
      replication_stream_id: "test",
      replication_slot_temporary?: true,
      replication_slot_temporary_random_name?: true
    ]
  end

  defp start_stack(ctx, id, overrides \\ []) do
    config =
      Electric.Application.configuration(
        overrides ++
          [
            replication_connection_opts: ctx.db_config,
            query_connection_opts: ctx.pooled_db_config,
            pool_opts: [pool_size: 2],
            process_registry_partitions: 1
          ] ++
          configuration_opts(ctx, id)
      )

    stack_id = Keyword.fetch!(config, :stack_id)
    start_supervised!({StackSupervisor, config}, id: stack_id, significant: false)

    assert :ok =
             StatusMonitor.wait_until_active(stack_id,
               timeout: ExUnit.configuration()[:assert_receive_timeout]
             )

    config
  end

  defp add_shape(config, table) do
    stack_id = config[:stack_id]
    inspector = Electric.StackConfig.lookup!(stack_id, :inspector)
    shape = Shape.new!(table, inspector: inspector)
    assert :ok = PublicationManager.add_shape(stack_id, table, shape)
  end

  defp publication_tables(conn, config) do
    Postgrex.query!(
      conn,
      "SELECT tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY tablename",
      [config[:replication_opts][:publication_name]]
    ).rows
  end
end
