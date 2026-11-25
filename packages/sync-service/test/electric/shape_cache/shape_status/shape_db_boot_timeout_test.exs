defmodule Electric.ShapeCache.ShapeStatus.ShapeDbBootTimeoutTest do
  # not async: mutates the :shape_db_boot_read_timeout application env
  use ExUnit.Case, async: false

  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Connection

  import Support.ComponentSetup

  @moduletag :tmp_dir

  setup [:with_stack_id_from_test, :with_shape_db]

  setup do
    previous = Application.get_env(:electric, :shape_db_boot_read_timeout)
    on_exit(fn -> Application.put_env(:electric, :shape_db_boot_read_timeout, previous) end)
    :ok
  end

  # The read pool is size 1 here, so this starves every other reader for `duration`,
  # which is what the boot restores do to each other on a large shape store.
  defp occupy_read_connection(stack_id, duration) do
    test = self()

    task =
      Task.async(fn ->
        Connection.checkout!(stack_id, :test_occupy, fn _conn ->
          send(test, :occupied)
          Process.sleep(duration)
        end)
      end)

    assert_receive :occupied, 5_000
    task
  end

  test "the default boot read timeout is minutes, not the 10s checkout default" do
    # Arrange
    Application.delete_env(:electric, :shape_db_boot_read_timeout)

    # Act
    timeout = Electric.Config.get_env(:shape_db_boot_read_timeout)

    # Assert
    assert timeout == 300_000
  end

  test "list_shapes waits on the configured boot read timeout, not the 10s default", ctx do
    # Arrange
    Application.put_env(:electric, :shape_db_boot_read_timeout, 200)
    task = occupy_read_connection(ctx.stack_id, 2_000)

    # Act / Assert
    # Under the unpatched 10s default this call would simply queue and succeed.
    assert catch_exit(ShapeDb.list_shapes(ctx.stack_id))

    Task.await(task, 10_000)
  end

  test "reduce_shape_meta waits on the configured boot read timeout", ctx do
    # Arrange
    Application.put_env(:electric, :shape_db_boot_read_timeout, 200)
    task = occupy_read_connection(ctx.stack_id, 2_000)

    # Act / Assert
    assert catch_exit(
             ShapeDb.reduce_shape_meta(ctx.stack_id, [], fn meta, acc -> [meta | acc] end)
           )

    Task.await(task, 10_000)
  end

  @tag :slow
  test "list_shapes survives a connection hold longer than the old 10s default", ctx do
    # Arrange
    # This is the failure with a large shape count in miniature: there the boot restores hold the
    # connection for minutes, and in exclusive mode there is only one.
    task = occupy_read_connection(ctx.stack_id, 12_000)

    # Act
    result = ShapeDb.list_shapes(ctx.stack_id)

    # Assert
    assert {:ok, []} = result

    Task.await(task, 20_000)
  end

  test "hot path reads keep the 10s checkout default", ctx do
    # Arrange
    Application.put_env(:electric, :shape_db_boot_read_timeout, 200)
    task = occupy_read_connection(ctx.stack_id, 2_000)

    # Act
    result = ShapeDb.count_shapes(ctx.stack_id)

    # Assert
    assert {:ok, 0} = result

    Task.await(task, 10_000)
  end
end
