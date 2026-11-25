defmodule Electric.Integration.SubqueryMoveOutTest do
  @moduledoc """
  Integration tests for subquery move-out functionality.

  These tests verify that the Elixir client correctly handles:
  1. Tags on change messages (indicating why a row belongs to the shape)
  2. Move-out control messages (when dependency values are removed)
  3. Synthetic delete generation from move-out patterns
  """
  use ExUnit.Case, async: false
  use Repatch.ExUnit

  import Support.ComponentSetup
  import Support.DbSetup
  import Support.DbStructureSetup
  import Support.IntegrationSetup
  import Support.StreamConsumer
  import Support.TestUtils, only: [wait_until: 1]

  alias Electric.Client
  alias Electric.Client.ShapeDefinition
  alias Electric.Client.Message.ChangeMessage
  alias Electric.Client.Message.ControlMessage
  alias Electric.Postgres.Inspector
  alias Electric.Postgres.SnapshotQuery
  alias Electric.Shapes
  alias Electric.Shapes.Consumer
  alias Electric.Shapes.Consumer.Effects
  alias Electric.Shapes.Consumer.Materializer
  alias Electric.Shapes.Shape

  @moduletag :tmp_dir

  # Shape definition for child table filtered by active parents
  @subquery_where "parent_id IN (SELECT id FROM parent WHERE active = true)"

  describe "subquery move-out with parent/child tables" do
    setup [:with_unique_db, :with_parent_child_tables, :with_sql_execute]
    setup :with_complete_stack

    setup :with_electric_client

    setup _ctx do
      shape = ShapeDefinition.new!("child", where: @subquery_where)
      %{shape: shape}
    end

    @tag with_sql: [
           "INSERT INTO parent (id, active) VALUES ('parent-1', true)",
           "INSERT INTO child (id, parent_id, value) VALUES ('child-1', 'parent-1', 'test value')"
         ]
    test "change messages include tags for subquery-matched rows", %{client: client, shape: shape} do
      stream = Client.stream(client, shape, live: false)

      with_consumer stream do
        insert = assert_insert(consumer, %{"id" => "child-1"})
        assert %{headers: %{tags: [_]}} = insert
      end
    end

    @tag with_sql: [
           "INSERT INTO parent (id, active) VALUES ('parent-1', true)",
           "INSERT INTO child (id, parent_id, value) VALUES ('child-1', 'parent-1', 'test value')"
         ]
    test "receives move-out control message when parent is deactivated", %{
      client: client,
      shape: shape,
      db_conn: db_conn
    } do
      stream = Client.stream(client, shape, live: true)

      with_consumer stream do
        # Wait for initial snapshot
        assert_insert(consumer, %{"id" => "child-1"})
        assert_up_to_date(consumer)

        # Deactivate the parent - this should trigger a move-out
        Postgrex.query!(db_conn, "UPDATE parent SET active = false WHERE id = 'parent-1'", [])

        # Should receive a synthetic delete for child-1
        assert_delete(consumer, %{"id" => "child-1"})
      end
    end

    @tag with_sql: [
           "INSERT INTO parent (id, active) VALUES ('parent-1', true)",
           "INSERT INTO child (id, parent_id, value) VALUES ('child-1', 'parent-1', 'value 1')",
           "INSERT INTO child (id, parent_id, value) VALUES ('child-2', 'parent-1', 'value 2')"
         ]
    test "move-out generates synthetic deletes for all affected child rows", %{
      client: client,
      shape: shape,
      db_conn: db_conn
    } do
      stream = Client.stream(client, shape, live: true)

      with_consumer stream do
        # Wait for initial snapshot
        assert_insert(consumer, %{"id" => "child-1"})
        assert_insert(consumer, %{"id" => "child-2"})
        assert_up_to_date(consumer)

        # Deactivate the parent
        Postgrex.query!(db_conn, "UPDATE parent SET active = false WHERE id = 'parent-1'", [])

        # Wait for both synthetic deletes
        {:ok, deletes} =
          await_count(consumer, 2,
            match: fn msg ->
              match?(%ChangeMessage{headers: %{operation: :delete}}, msg)
            end
          )

        delete_ids = Enum.map(deletes, & &1.value["id"]) |> Enum.sort()
        assert delete_ids == ["child-1", "child-2"]
      end
    end

    @tag with_sql: [
           "INSERT INTO parent (id, active) VALUES ('parent-1', true)",
           "INSERT INTO child (id, parent_id, value) VALUES ('child-1', 'parent-1', 'test value')"
         ]
    test "deleting parent row triggers move-out", %{
      client: client,
      shape: shape,
      db_conn: db_conn
    } do
      stream = Client.stream(client, shape, live: true)

      with_consumer stream do
        # Wait for initial snapshot
        assert_insert(consumer, %{"id" => "child-1"})
        assert_up_to_date(consumer)

        # Delete the parent row
        Postgrex.query!(db_conn, "DELETE FROM parent WHERE id = 'parent-1'", [])

        # Should receive a synthetic delete for the child
        assert_delete(consumer, %{"id" => "child-1"})
      end
    end

    @tag with_sql: [
           "INSERT INTO parent (id, active) VALUES ('parent-1', true)",
           "INSERT INTO parent (id, active) VALUES ('parent-2', true)",
           "INSERT INTO child (id, parent_id, value) VALUES ('child-1', 'parent-1', 'belongs to parent-1')"
         ]
    test "move-in after row becomes visible through different parent", %{
      client: client,
      shape: shape,
      db_conn: db_conn
    } do
      stream = Client.stream(client, shape, live: true)

      with_consumer stream do
        # Wait for initial snapshot
        assert_insert(consumer, %{"id" => "child-1"})
        assert_up_to_date(consumer)

        # Deactivate parent-1
        Postgrex.query!(db_conn, "UPDATE parent SET active = false WHERE id = 'parent-1'", [])

        # Wait for the move-out (synthetic delete)
        assert_delete(consumer, %{"id" => "child-1"})
        assert_up_to_date(consumer)

        # Change child to reference parent-2 (which is still active)
        Postgrex.query!(
          db_conn,
          "UPDATE child SET parent_id = 'parent-2' WHERE id = 'child-1'",
          []
        )

        # Should receive a new insert (move-in) for the child
        assert_insert(consumer, %{"id" => "child-1", "parent_id" => "parent-2"})
      end
    end
  end

  describe "tag handling during updates" do
    setup [:with_unique_db, :with_parent_child_tables, :with_sql_execute]
    setup :with_complete_stack

    setup :with_electric_client

    setup _ctx do
      shape = ShapeDefinition.new!("child", where: @subquery_where)
      %{shape: shape}
    end

    @tag with_sql: [
           "INSERT INTO parent (id, active) VALUES ('parent-1', true)",
           "INSERT INTO parent (id, active) VALUES ('parent-2', true)",
           "INSERT INTO child (id, parent_id, value) VALUES ('child-1', 'parent-1', 'initial')"
         ]
    test "update that changes parent reference updates tags", %{
      client: client,
      shape: shape,
      db_conn: db_conn
    } do
      stream = Client.stream(client, shape, live: true)

      with_consumer stream do
        # Wait for initial snapshot
        initial = assert_insert(consumer, %{"id" => "child-1"})
        assert_up_to_date(consumer)

        # Store initial tags
        initial_tags = Map.get(initial.headers, :tags, [])

        # Change child to reference parent-2
        Postgrex.query!(
          db_conn,
          "UPDATE child SET parent_id = 'parent-2' WHERE id = 'child-1'",
          []
        )

        # Should receive an update with new tags
        update_msg = assert_update(consumer, %{"id" => "child-1"})

        # The headers should include both removed_tags and new tags
        new_tags = Map.get(update_msg.headers, :tags, [])
        removed_tags = Map.get(update_msg.headers, :removed_tags, [])

        # Either we have explicit removed_tags, or tags have changed
        assert new_tags != initial_tags or length(removed_tags) > 0
      end
    end

    @tag with_sql: [
           "INSERT INTO parent (id, active) VALUES ('parent-1', true)",
           "INSERT INTO parent (id, active) VALUES ('parent-2', true)",
           "INSERT INTO child (id, parent_id, value) VALUES ('child-1', 'parent-1', 'initial')"
         ]
    test "deactivating old parent after child changed parents should not generate delete", %{
      client: client,
      shape: shape,
      db_conn: db_conn
    } do
      # This tests that the tag index is properly updated when a row's tags change.
      # If the client doesn't clear stale tag entries, deactivating the OLD parent
      # would incorrectly generate a synthetic delete even though the child is
      # still in the shape (via the new parent).

      stream = Client.stream(client, shape, live: true)

      with_consumer stream do
        # Wait for initial snapshot - child-1 is in shape via parent-1
        assert_insert(consumer, %{"id" => "child-1"})
        assert_up_to_date(consumer)

        # Change child to reference parent-2 (also active)
        # This should update the tag index: remove parent-1's tag, add parent-2's tag
        Postgrex.query!(
          db_conn,
          "UPDATE child SET parent_id = 'parent-2' WHERE id = 'child-1'",
          []
        )

        # Should receive an update (child is still in shape, just via different parent)
        assert_update(consumer, %{"id" => "child-1"})
        assert_up_to_date(consumer)

        # Now deactivate parent-1 - this triggers a move-out for parent-1's tag
        # Since child-1 no longer has parent-1's tag (it was removed in the update),
        # this should NOT generate a synthetic delete
        Postgrex.query!(db_conn, "UPDATE parent SET active = false WHERE id = 'parent-1'", [])

        # Collect any messages that arrive - we should NOT see a delete for child-1
        messages = collect_messages(consumer, timeout: 500)

        delete_msgs =
          Enum.filter(messages, &match?(%ChangeMessage{headers: %{operation: :delete}}, &1))

        assert delete_msgs == [],
               "Should not generate synthetic delete for child-1 after old parent deactivated. " <>
                 "The tag index should have been updated when child changed parents. " <>
                 "Got: #{inspect(delete_msgs)}"
      end
    end
  end

  describe "tag consistency across streams" do
    setup [:with_unique_db, :with_parent_child_tables, :with_sql_execute]
    setup :with_complete_stack

    setup :with_electric_client

    setup _ctx do
      shape = ShapeDefinition.new!("child", where: @subquery_where)
      %{shape: shape}
    end

    @tag with_sql: [
           "INSERT INTO parent (id, active) VALUES ('parent-1', true)",
           "INSERT INTO child (id, parent_id, value) VALUES ('child-1', 'parent-1', 'test')"
         ]
    test "fresh streams have consistent tags", %{client: client, shape: shape} do
      # Get initial data with tags from first stream
      stream1 = Client.stream(client, shape, live: false)

      msg1 =
        with_consumer stream1 do
          assert_insert(consumer, %{"id" => "child-1"})
        end

      # Get data from a second fresh stream
      stream2 = Client.stream(client, shape, live: false)

      msg2 =
        with_consumer stream2 do
          assert_insert(consumer, %{"id" => "child-1"})
        end

      # Both streams should report consistent tags for the same row
      tags1 = Map.get(msg1.headers, :tags)
      tags2 = Map.get(msg2.headers, :tags)

      # Tags must be present and consistent
      assert tags1 != nil
      assert tags1 == tags2
    end
  end

  describe "resume preserves move-out state" do
    setup [:with_unique_db, :with_parent_child_tables, :with_sql_execute]
    setup :with_complete_stack

    setup :with_electric_client

    setup _ctx do
      shape = ShapeDefinition.new!("child", where: @subquery_where)
      %{shape: shape}
    end

    @tag with_sql: [
           "INSERT INTO parent (id, active) VALUES ('parent-1', true)",
           "INSERT INTO child (id, parent_id, value) VALUES ('child-1', 'parent-1', 'test value')"
         ]
    test "move-out after resume generates synthetic delete", %{
      client: client,
      shape: shape,
      db_conn: db_conn
    } do
      # First, stream with live: false to get a ResumeMessage
      # This simulates a client that synced initial data and then disconnected
      stream1 = Client.stream(client, shape, live: false)

      resume_msg =
        with_consumer stream1 do
          assert_insert(consumer, %{"id" => "child-1"})
          assert_up_to_date(consumer)
          assert_resume(consumer)
        end

      # Now deactivate the parent while "disconnected"
      # This should trigger a move-out on the server side
      Postgrex.query!(db_conn, "UPDATE parent SET active = false WHERE id = 'parent-1'", [])

      # Resume the stream - with proper move-out support, the client should
      # receive a synthetic delete for child-1 because its parent was deactivated
      stream2 = Client.stream(client, shape, live: false, resume: resume_msg)

      with_consumer stream2 do
        assert_delete(consumer, %{"id" => "child-1"})
      end
    end
  end

  describe "negated subquery move-in and move-out" do
    setup [:with_unique_db, :with_inner_outer_tables, :with_sql_execute]
    setup :with_complete_stack
    setup :with_electric_client

    # Shape: outer rows whose inner row is NOT active
    @negated_where "inner_id NOT IN (SELECT id FROM inner_table WHERE active = true)"

    setup _ctx do
      shape = ShapeDefinition.new!("outer_table", where: @negated_where)
      %{shape: shape}
    end

    @tag with_sql: [
           "INSERT INTO inner_table (id, active) VALUES ('inner-1', true)",
           "INSERT INTO outer_table (id, inner_id, value) VALUES ('outer-1', 'inner-1', 'test value')"
         ]
    test "outer row enters shape when inner row becomes inactive (negated move-in)", %{
      client: client,
      shape: shape,
      db_conn: db_conn
    } do
      stream = Client.stream(client, shape, live: true)

      with_consumer stream do
        # Initially outer-1 is not in shape because inner-1 is active.
        # NOT IN active inner rows means only outer rows linked to inactive
        # inner rows are included.
        assert_up_to_date(consumer)

        # Deactivate inner-1 and outer-1 should enter the shape.
        Postgrex.query!(db_conn, "UPDATE inner_table SET active = false WHERE id = 'inner-1'", [])

        assert_insert(consumer, %{"id" => "outer-1"})
      end
    end

    @tag with_sql: [
           "INSERT INTO inner_table (id, active) VALUES ('inner-1', false)",
           "INSERT INTO outer_table (id, inner_id, value) VALUES ('outer-1', 'inner-1', 'test value')"
         ]
    test "outer row leaves shape when inner row becomes active (negated move-out)", %{
      client: client,
      shape: shape,
      db_conn: db_conn
    } do
      stream = Client.stream(client, shape, live: true)

      with_consumer stream do
        # Initially outer-1 is in shape because inner-1 is inactive.
        assert_insert(consumer, %{"id" => "outer-1"})
        assert_up_to_date(consumer)

        # Activate inner-1 and outer-1 should leave the shape.
        Postgrex.query!(db_conn, "UPDATE inner_table SET active = true WHERE id = 'inner-1'", [])

        assert_delete(consumer, %{"id" => "outer-1"})
      end
    end
  end

  describe "nullable NOT IN dependency" do
    setup [:with_unique_db, :with_sql_execute]
    setup :with_complete_stack
    setup :with_electric_client

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, a INT[], b INT[])",
           "CREATE TABLE child (id INT PRIMARY KEY, a INT[], b INT[])",
           "INSERT INTO child VALUES (1, ARRAY[1], ARRAY[2]), (2, ARRAY[1,2], ARRAY[3]), (3, ARRAY[1], ARRAY[3])"
         ]
    test "adding array-valued composite keys removes only matching children", %{
      client: client,
      db_conn: conn
    } do
      where = "(a,b) NOT IN (SELECT a,b FROM parent)"
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert {:ok, [_, _, _]} =
                 await_count(consumer, 3, match: &match?(%ChangeMessage{}, &1))

        assert_up_to_date(consumer)

        Postgrex.query!(
          conn,
          "INSERT INTO parent VALUES (1, ARRAY[1], ARRAY[2]), (2, ARRAY[1,2], ARRAY[3])",
          []
        )

        assert [[3]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows

        assert {:ok, deletes} =
                 await_count(consumer, 2,
                   match: &match?(%ChangeMessage{headers: %{operation: :delete}}, &1)
                 )

        assert [1, 2] = Enum.sort(Enum.map(deletes, & &1.value["id"]))
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, a NUMERIC[], b NUMERIC[])",
           "CREATE TABLE child (id INT PRIMARY KEY, a NUMERIC[], b NUMERIC[])",
           "INSERT INTO child VALUES (1, '{9007199254740993}', '{1}'), (2, '{9007199254740992}', '{1}')"
         ]
    test "adding a composite numeric array key removes the exact match", %{
      client: client,
      db_conn: conn
    } do
      where = "(a,b) NOT IN (SELECT a,b FROM parent)"
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert {:ok, [_, _]} = await_count(consumer, 2, match: &match?(%ChangeMessage{}, &1))
        assert_up_to_date(consumer)

        Postgrex.query!(
          conn,
          "INSERT INTO parent VALUES (1, '{9007199254740993}', '{1}')",
          []
        )

        assert [[2]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows
        assert_delete(consumer, %{"id" => 1})
        assert_up_to_date(consumer)
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, a NUMERIC, b NUMERIC)",
           "CREATE TABLE child (id INT PRIMARY KEY, a NUMERIC, b NUMERIC)",
           "INSERT INTO child VALUES (1, 9007199254740993, 1), (2, 9007199254740992, 1)"
         ]
    test "adding a composite numeric key removes the exact match", %{
      client: client,
      db_conn: conn
    } do
      where = "(a,b) NOT IN (SELECT a,b FROM parent)"
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert {:ok, [_, _]} = await_count(consumer, 2, match: &match?(%ChangeMessage{}, &1))
        assert_up_to_date(consumer)

        Postgrex.query!(conn, "INSERT INTO parent VALUES (1, 9007199254740993, 1)", [])
        assert [[2]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows
        assert_delete(consumer, %{"id" => 1})
        assert_up_to_date(consumer)
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, a NUMERIC, b NUMERIC)",
           "CREATE TABLE child (id INT PRIMARY KEY, a NUMERIC, b NUMERIC)",
           "INSERT INTO child VALUES (1, 1e200, 1), (2, 7, 1)"
         ]
    test "adding a composite numeric key past the float range removes the exact match", %{
      client: client,
      db_conn: conn
    } do
      where = "(a,b) NOT IN (SELECT a,b FROM parent)"
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert {:ok, [_, _]} = await_count(consumer, 2, match: &match?(%ChangeMessage{}, &1))
        assert_up_to_date(consumer)

        Postgrex.query!(conn, "INSERT INTO parent VALUES (1, 1e200, 1)", [])
        assert [[2]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows
        assert_delete(consumer, %{"id" => 1})
        assert_up_to_date(consumer)
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, period INTERVAL NOT NULL)",
           "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT NOT NULL, period INTERVAL NOT NULL)",
           "INSERT INTO child VALUES (1, 1, '1 day')"
         ]
    test "adding a composite interval key removes the matching child", %{
      client: client,
      db_conn: conn
    } do
      where = "(parent_id, period) NOT IN (SELECT id, period FROM parent)"
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert_insert(consumer, %{"id" => 1})
        assert_up_to_date(consumer)

        Postgrex.query!(conn, "INSERT INTO parent VALUES (1, '1 day')", [])
        assert Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows == []
        assert_delete(consumer, %{"id" => 1})
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, value INT)",
           "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT)",
           "INSERT INTO parent VALUES (1, NULL)",
           "INSERT INTO child VALUES (1, 2), (2, NULL)"
         ]
    test "removing the last NULL admits the rows Postgres now returns", %{
      client: client,
      db_conn: conn
    } do
      where = "parent_id NOT IN (SELECT value FROM parent)"
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert_up_to_date(consumer)
        assert Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows == []

        Postgrex.query!(conn, "DELETE FROM parent WHERE id = 1", [])

        assert Postgrex.query!(conn, "SELECT id FROM child WHERE #{where} ORDER BY id", []).rows ==
                 [[1], [2]]

        assert {:ok, inserts} =
                 await_count(consumer, 2,
                   match: &match?(%ChangeMessage{headers: %{operation: :insert}}, &1)
                 )

        assert Enum.sort(Enum.map(inserts, & &1.value["id"])) == [1, 2]

        Postgrex.query!(conn, "INSERT INTO parent VALUES (2, 3)", [])
        assert Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows == [[1]]

        assert {:ok, [%ChangeMessage{value: %{"id" => 2}}]} =
                 await_count(consumer, 1,
                   match: &match?(%ChangeMessage{headers: %{operation: :delete}}, &1)
                 )

        Postgrex.query!(conn, "DELETE FROM parent WHERE id = 2", [])

        assert Postgrex.query!(conn, "SELECT id FROM child WHERE #{where} ORDER BY id", []).rows ==
                 [[1], [2]]

        assert {:ok, [%ChangeMessage{value: %{"id" => 2}}]} =
                 await_count(consumer, 1,
                   match: &match?(%ChangeMessage{headers: %{operation: :insert}}, &1)
                 )
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, value INT)",
           "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT)",
           "INSERT INTO child VALUES (1, 2), (2, NULL)"
         ]
    test "adding NULL to an empty view removes every row Postgres excludes", %{
      client: client,
      db_conn: conn
    } do
      where = "parent_id NOT IN (SELECT value FROM parent)"
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert {:ok, inserts} =
                 await_count(consumer, 2,
                   match: &match?(%ChangeMessage{headers: %{operation: :insert}}, &1)
                 )

        assert Enum.sort(Enum.map(inserts, & &1.value["id"])) == [1, 2]
        assert_up_to_date(consumer)

        Postgrex.query!(conn, "INSERT INTO parent VALUES (1, NULL)", [])
        assert Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows == []

        assert {:ok, deletes} =
                 await_count(consumer, 2,
                   match: &match?(%ChangeMessage{headers: %{operation: :delete}}, &1)
                 )

        assert Enum.sort(Enum.map(deletes, & &1.value["id"])) == [1, 2]
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, value INT)",
           "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT)",
           "INSERT INTO parent VALUES (1, NULL)",
           "INSERT INTO child VALUES (1, 3), (2, 2), (3, NULL)"
         ]
    test "replacing the last NULL only inserts children in the final view", %{
      client: client,
      db_conn: conn
    } do
      where = "parent_id NOT IN (SELECT value FROM parent)"
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert_up_to_date(consumer)
        assert Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows == []

        Postgrex.query!(conn, "UPDATE parent SET value = 3 WHERE id = 1", [])
        assert Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows == [[2]]
        assert_insert(consumer, %{"id" => 2})
        assert_up_to_date(consumer)

        Postgrex.query!(conn, "UPDATE parent SET value = NULL WHERE id = 1", [])
        assert Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows == []
        assert_delete(consumer, %{"id" => 2})
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, value INT)",
           "CREATE TABLE child (id INT PRIMARY KEY, first_id INT, second_id INT)",
           "INSERT INTO parent VALUES (1, 3)",
           "INSERT INTO child VALUES (1, 2, 3), (2, 2, 2)"
         ]
    test "a replacement keeps rows that switch between negated disjuncts", %{
      client: client,
      db_conn: conn
    } do
      where = """
      first_id NOT IN (SELECT value FROM parent)
      OR second_id NOT IN (SELECT value FROM parent)
      """

      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert {:ok, [_, _]} =
                 await_count(consumer, 2,
                   match: &match?(%ChangeMessage{headers: %{operation: :insert}}, &1)
                 )

        assert_up_to_date(consumer)
        Postgrex.query!(conn, "UPDATE parent SET value = 2 WHERE id = 1", [])
        assert Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows == [[1]]

        assert_delete(consumer, %{"id" => 2})
        assert_up_to_date(consumer)
      end
    end

    # Each view keeps a second value: a replacement that empties its view keeps both halves.
    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, group_id INT, value INT)",
           "CREATE TABLE child (id INT PRIMARY KEY, a INT, b INT)",
           "INSERT INTO parent VALUES (1, 1, 1), (2, 2, 1), (3, 1, 9), (4, 2, 9)",
           "INSERT INTO child VALUES (1, 2, 1), (2, 1, 2), (3, 1, 1)"
         ]
    test "a transaction keeps rows that switch between distinct negated dependencies", %{
      client: client,
      db_conn: conn,
      stack_id: stack_id
    } do
      where = """
      a NOT IN (SELECT value FROM parent WHERE group_id = 1)
      OR b NOT IN (SELECT value FROM parent WHERE group_id = 2)
      """

      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert {:ok, [_, _]} = await_count(consumer, 2, match: &match?(%ChangeMessage{}, &1))
        %{handle: handle} = assert_up_to_date(consumer)
        consumer_name = Consumer.name(stack_id, handle)

        Postgrex.query!(conn, "UPDATE parent SET value = 2 WHERE value = 1", [])

        assert [[1], [2], [3]] =
                 Postgrex.query!(conn, "SELECT id FROM child WHERE #{where} ORDER BY id", []).rows

        assert wait_until(fn ->
                 case :sys.get_state(consumer_name).event_handler do
                   %{views: views} ->
                     Enum.all?(views, fn {_, keys} -> MapSet.member?(keys, 2) end)

                   %{active_move: _} ->
                     false
                 end
               end)

        Postgrex.query!(conn, "INSERT INTO child VALUES (4, 3, 3)", [])

        assert {:ok,
                [
                  %ChangeMessage{headers: %{operation: :insert}, value: %{"id" => 3}},
                  %ChangeMessage{headers: %{operation: :insert}, value: %{"id" => 4}}
                ]} = await_count(consumer, 2, match: &match?(%ChangeMessage{}, &1))
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, a TEXT, b TEXT)",
           "CREATE TABLE child (id INT PRIMARY KEY, a TEXT, b TEXT)"
         ]
    test "root inserts see both committed exclusions of text pairs", %{
      client: client,
      db_conn: conn,
      db_config: db_config,
      stack_id: stack_id
    } do
      where = "(a,b) NOT IN (SELECT a,b FROM parent)"
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        %{handle: handle} = assert_up_to_date(consumer)
        consumer_name = Consumer.name(stack_id, handle)

        settings =
          db_config |> Electric.Utils.deobfuscate_password() |> Keyword.put(:pool_size, 1)

        other_conn = start_supervised!({Postgrex, settings}, id: :root_lock_pool)
        test_pid = self()

        writer =
          Task.async(fn ->
            Postgrex.transaction(other_conn, fn tx ->
              Postgrex.query!(tx, "LOCK TABLE child IN ACCESS EXCLUSIVE MODE", [])
              send(test_pid, :root_locked)

              receive do
                :insert_root ->
                  Postgrex.query!(
                    tx,
                    "INSERT INTO child VALUES (1, 'hidden', 'key'), (2, 'visible', 'key')",
                    []
                  )
              end
            end)
          end)

        assert_receive :root_locked

        Postgrex.query!(conn, "INSERT INTO parent VALUES (1, 'other', 'key')", [])

        assert wait_until(fn ->
                 case :sys.get_state(consumer_name).event_handler do
                   %{active_move: move} -> move.snapshot != nil
                   %{views: views} -> composite_view_contains?(views, {"other", "key"})
                 end
               end)

        Postgrex.query!(conn, "INSERT INTO parent VALUES (2, 'hidden', 'key')", [])

        assert wait_until(fn ->
                 case :sys.get_state(consumer_name).event_handler do
                   %{active_move: _, queue: queue} -> Map.has_key?(queue.move_in, 0)
                   %{views: views} -> composite_view_contains?(views, {"hidden", "key"})
                 end
               end)

        send(writer.pid, :insert_root)
        assert {:ok, _} = Task.await(writer)
        assert [[2]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows

        assert {:ok, [%ChangeMessage{headers: %{operation: :insert}, value: %{"id" => 2}}]} =
                 await_count(consumer, 1, match: &match?(%ChangeMessage{}, &1))
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, grp INT, k INT)",
           "CREATE TABLE child (id INT PRIMARY KEY, a INT, b INT, c INT)",
           "INSERT INTO parent VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1)",
           "INSERT INTO child VALUES (1, NULL, 1, 1), (2, 3, 3, 3)"
         ]
    test "a replacement that empties a negated view never shows a row with a NULL key", %{
      client: client,
      db_conn: conn,
      stack_id: stack_id
    } do
      where = """
      a NOT IN (SELECT k FROM parent WHERE grp = 1)
      OR b NOT IN (SELECT k FROM parent WHERE grp = 2)
      OR c NOT IN (SELECT k FROM parent WHERE grp = 3)
      """

      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert_insert(consumer, %{"id" => 2})
        %{handle: handle} = assert_up_to_date(consumer)
        consumer_name = Consumer.name(stack_id, handle)

        Postgrex.transaction(conn, fn tx ->
          Postgrex.query!(tx, "UPDATE parent SET k = 2 WHERE id = 1", [])
          Postgrex.query!(tx, "UPDATE parent SET k = 3 WHERE id = 1", [])
        end)

        # Insert the marker once the move is applied, so it cannot reach the log first.
        assert wait_until(fn ->
                 case :sys.get_state(consumer_name).event_handler do
                   %{views: views} -> MapSet.member?(views[["$sublink", "0"]], 3)
                   %{active_move: _} -> false
                 end
               end)

        Postgrex.query!(conn, "INSERT INTO child VALUES (3, 5, 5, 5)", [])

        assert [[2], [3]] =
                 Postgrex.query!(conn, "SELECT id FROM child WHERE #{where} ORDER BY id", []).rows

        assert {:ok, [%ChangeMessage{headers: %{operation: :insert}, value: %{"id" => 3}}]} =
                 await_count(consumer, 1, match: &match?(%ChangeMessage{}, &1))
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (a TEXT, b TEXT)",
           "CREATE TABLE child (id INT PRIMARY KEY, a TEXT, b TEXT)",
           "INSERT INTO child VALUES (1, 'x', 'yb:v:z'), (2, 'xb:v:y', 'z')"
         ]
    test "a text pair exclusion only removes the child with the matching key", %{
      client: client,
      db_conn: conn
    } do
      where = "(a,b) NOT IN (SELECT a,b FROM parent)"
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert {:ok, snapshot} = await_count(consumer, 2, match: &match?(%ChangeMessage{}, &1))
        assert snapshot |> Enum.map(& &1.value["id"]) |> Enum.sort() == [1, 2]
        %{handle: handle} = assert_up_to_date(consumer)

        Postgrex.query!(conn, "INSERT INTO parent VALUES ('xb:v:y', 'z')", [])
        Postgrex.query!(conn, "INSERT INTO child VALUES (3, 'marker', 'marker')", [])

        assert operations_until_insert(consumer, 3) == [delete: 2, insert: 3]
        assert %{handle: ^handle} = assert_up_to_date(consumer)
      end
    end

    # Both pairs have the legacy tag input "a:v:xb:v:yb:v:z".
    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, a TEXT, b TEXT)",
           "CREATE TABLE child (id INT PRIMARY KEY, a TEXT, b TEXT)",
           "INSERT INTO child VALUES (1, 'x', 'yb:v:z')"
         ]
    test "refetching an ambiguous exclusion keeps a child that shares its tag input", %{
      client: client,
      db_conn: conn,
      stack_id: stack_id
    } do
      where = "(a,b) NOT IN (SELECT a,b FROM parent)"
      old_handle = create_legacy_shape(stack_id, where)
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert_insert(consumer, %{"id" => 1})
        assert %{handle: ^old_handle} = assert_up_to_date(consumer)

        Postgrex.query!(conn, "INSERT INTO parent VALUES (1, 'xb:v:y', 'z')", [])

        assert [[1]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows

        assert [:must_refetch, {:insert, 1}] = operations_until_insert(consumer, 1)
        assert %{handle: new_handle} = assert_up_to_date(consumer)
        assert new_handle != old_handle
      end
    end
  end

  describe "negated dependencies changing during a move query" do
    setup [:with_unique_db, :with_sql_execute]
    setup :with_complete_stack
    setup :with_electric_client

    for {label, value, reset} <- [
          {"non-field", "x b:garbage", []},
          {"ambiguous", "x b:v:garbage", [:must_refetch]}
        ] do
      @tag with_sql: [
             "CREATE TABLE parent (id INT PRIMARY KEY, ab TEXT, b TEXT)",
             "CREATE TABLE child (id INT PRIMARY KEY, ab TEXT, b TEXT)"
           ]
      test "a move with marker type #{label} never exposes later excluded root inserts", %{
        client: client,
        db_conn: conn,
        stack_id: stack_id
      } do
        where = "(ab,b) NOT IN (SELECT ab,b FROM parent)"
        legacy_handle = create_legacy_shape(stack_id, where)
        stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

        with_consumer stream do
          assert %{handle: ^legacy_handle} = assert_up_to_date(consumer)
          name = Consumer.name(stack_id, legacy_handle)
          pid = GenServer.whereis(name)
          root = unquote(value)
          gate_first_move_query(name, :before_snapshot)

          Postgrex.query!(conn, "INSERT INTO parent VALUES (1, 'other', $1)", [root])
          assert [] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows

          assert wait_until(fn ->
                   case live_handler(pid) do
                     %{active_move: _} -> true
                     %{views: views} -> composite_view_contains?(views, {"other", root})
                     nil -> true
                   end
                 end)

          gate =
            case live_handler(pid) do
              %{active_move: _} ->
                assert_receive {:move_query_paused, query}
                query

              _ ->
                nil
            end

          assert {:ok, _} =
                   Postgrex.transaction(conn, fn tx ->
                     Postgrex.query!(tx, "INSERT INTO parent VALUES (2, 'hidden', 'key')", [])

                     Postgrex.query!(
                       tx,
                       "INSERT INTO child VALUES (1, 'hidden', 'key'), (2, 'visible', 'key')",
                       []
                     )
                   end)

          assert [[2]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows

          if gate do
            assert wait_until(fn ->
                     case :sys.get_state(name).event_handler do
                       %{active_move: %{buffered_txn_count: count}, queue: queue} ->
                         count > 0 and Map.has_key?(queue.move_in, 0)

                       _ ->
                         false
                     end
                   end)

            send(gate, :resume_move_query)
          end

          assert wait_until(fn ->
                   case live_handler(pid) do
                     %{views: views} -> composite_view_contains?(views, {"hidden", "key"})
                     nil -> true
                     _ -> false
                   end
                 end)

          Postgrex.query!(conn, "INSERT INTO child VALUES (9, 'marker', 'marker')", [])

          assert operations_until_insert(consumer, 9) ==
                   unquote(reset) ++ [insert: 2, insert: 9]
        end
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, grp INT, k INT)",
           "CREATE TABLE child (id INT PRIMARY KEY, a INT, b INT)",
           "INSERT INTO parent VALUES (1, 1, 1), (2, 2, 1)",
           "INSERT INTO child VALUES (1, 2, 1), (2, 1, 2)"
         ]
    test "refetches a same-transaction switch when two negated views empty and refill", %{
      client: client,
      db_conn: conn,
      stack_id: stack_id
    } do
      where = """
      a NOT IN (SELECT k FROM parent WHERE grp = 1)
      OR b NOT IN (SELECT k FROM parent WHERE grp = 2)
      """

      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert {:ok, [_, _]} = await_count(consumer, 2, match: &match?(%ChangeMessage{}, &1))
        %{handle: handle} = assert_up_to_date(consumer)
        name = Consumer.name(stack_id, handle)
        pid = GenServer.whereis(name)
        gate_first_move_query(name, :before_snapshot)

        assert [[1], [2]] =
                 Postgrex.query!(conn, "SELECT id FROM child WHERE #{where} ORDER BY id", []).rows

        Postgrex.query!(conn, "UPDATE parent SET k = 2", [])
        assert_receive {:move_query_paused, query}

        assert [[1], [2]] =
                 Postgrex.query!(conn, "SELECT id FROM child WHERE #{where} ORDER BY id", []).rows

        assert wait_until(fn ->
                 case live_handler(pid) do
                   %{active_move: %{dep_index: active}, queue: queue} ->
                     sibling = 1 - active

                     Map.has_key?(queue.move_out, sibling) and
                       Map.has_key?(queue.move_in, sibling)

                   _ ->
                     false
                 end
               end)

        send(query, :resume_move_query)

        assert wait_until(fn ->
                 case live_handler(pid) do
                   nil ->
                     true

                   %{views: views} ->
                     Enum.all?(views, fn {_, keys} -> MapSet.member?(keys, 2) end)

                   _ ->
                     false
                 end
               end)

        Postgrex.query!(conn, "INSERT INTO child VALUES (9, 3, 3)", [])
        assert [:must_refetch | snapshot_and_marker] = operations_until_insert(consumer, 9)
        assert Enum.sort(snapshot_and_marker) == [insert: 1, insert: 2, insert: 9]
      end
    end

    # The inserted parent pair can share its legacy tag input with another pair.
    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, a TEXT, b TEXT)",
           "CREATE TABLE child (id INT PRIMARY KEY, a TEXT, b TEXT)",
           "INSERT INTO parent VALUES (1, 'p', 'q')",
           "INSERT INTO child VALUES (1, 'p', 'q'), (2, 'xb:v:y', 'z'), (3, 'm', 'n')"
         ]
    test "an ambiguous exclusion queued behind a move query retires the shape cleanly", %{
      client: client,
      db_conn: conn,
      stack_id: stack_id
    } do
      where = "(a,b) NOT IN (SELECT a,b FROM parent)"
      legacy_handle = create_legacy_shape(stack_id, where)
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert {:ok, [_, _]} = await_count(consumer, 2, match: &match?(%ChangeMessage{}, &1))
        assert %{handle: ^legacy_handle} = assert_up_to_date(consumer)
        name = Consumer.name(stack_id, legacy_handle)
        pid = GenServer.whereis(name)
        gate_first_move_query(name, :before_snapshot)

        Postgrex.query!(conn, "DELETE FROM parent WHERE id = 1", [])
        assert_receive {:move_query_paused, query}

        Postgrex.query!(conn, "INSERT INTO parent VALUES (2, 'xb:v:y', 'z')", [])

        assert [[1], [3]] =
                 Postgrex.query!(conn, "SELECT id FROM child WHERE #{where} ORDER BY id", []).rows

        assert wait_until(fn ->
                 match?(
                   %{active_move: _, queue: %{move_in: %{0 => _}}},
                   :sys.get_state(name).event_handler
                 )
               end)

        ref = Process.monitor(pid)
        send(query, :resume_move_query)

        assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 10_000
        assert reason == Electric.ShapeCache.ShapeCleaner.consumer_cleanup_reason()

        assert {:ok, [_]} =
                 await_count(consumer, 1,
                   match: &match?(%ControlMessage{control: :must_refetch}, &1)
                 )

        assert {:ok, snapshot} = await_count(consumer, 2, match: &match?(%ChangeMessage{}, &1))
        assert snapshot |> Enum.map(& &1.value["id"]) |> Enum.sort() == [1, 3]
        assert_up_to_date(consumer)

        Postgrex.query!(conn, "INSERT INTO child VALUES (9, 'marker', 'marker')", [])
        assert [insert: 9] = operations_until_insert(consumer, 9)
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, k INT)",
           "CREATE TABLE child (id INT PRIMARY KEY, x INT)",
           "INSERT INTO parent VALUES (1, 1), (3, 3)",
           "INSERT INTO child VALUES (1, NULL), (2, 5)"
         ]
    test "pairs an addition with a later commit removal that empties a negated view", %{
      client: client,
      db_conn: conn,
      stack_id: stack_id
    } do
      where = "x NOT IN (SELECT k FROM parent)"
      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert_insert(consumer, %{"id" => 2})
        %{handle: handle} = assert_up_to_date(consumer)
        name = Consumer.name(stack_id, handle)
        gate_first_move_query(name, :before_snapshot)

        Postgrex.query!(conn, "DELETE FROM parent WHERE id = 3", [])
        assert_receive {:move_query_paused, query}

        # The view goes {1}, {1, 2}, {2}: never empty, so child 1 never matches.
        Postgrex.query!(conn, "INSERT INTO parent VALUES (2, 2)", [])
        Postgrex.query!(conn, "DELETE FROM parent WHERE id = 1", [])
        assert [[2]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows

        assert wait_until(fn ->
                 match?(
                   %{active_move: _, queue: %{move_in: %{0 => _}, move_out: %{0 => _}}},
                   :sys.get_state(name).event_handler
                 )
               end)

        send(query, :resume_move_query)

        assert wait_until(fn ->
                 case :sys.get_state(name).event_handler do
                   %{views: views} -> views[["$sublink", "0"]] == MapSet.new([2])
                   _ -> false
                 end
               end)

        Postgrex.query!(conn, "INSERT INTO child VALUES (9, 9)", [])
        assert [insert: 9] = operations_until_insert(consumer, 9)
      end
    end

    @tag with_sql: [
           "CREATE TABLE parent (id INT PRIMARY KEY, grp INT, k INT)",
           "CREATE TABLE child (id INT PRIMARY KEY, a INT, b INT)",
           "INSERT INTO parent VALUES (1, 1, 1), (3, 1, 3), (7, 2, 7)",
           "INSERT INTO child VALUES (1, NULL, 7), (2, 5, 7)"
         ]
    test "refetches a later commit emptying removal beside a queued sibling move", %{
      client: client,
      db_conn: conn,
      stack_id: stack_id
    } do
      where = """
      a NOT IN (SELECT k FROM parent WHERE grp = 1)
      OR b NOT IN (SELECT k FROM parent WHERE grp = 2)
      """

      stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

      with_consumer stream do
        assert_insert(consumer, %{"id" => 2})
        %{handle: old_handle} = assert_up_to_date(consumer)
        name = Consumer.name(stack_id, old_handle)
        gate_first_move_query(name, :before_snapshot)

        Postgrex.query!(conn, "DELETE FROM parent WHERE id = 3", [])
        assert_receive {:move_query_paused, query}

        # The first view goes {1}, {1, 2}, {2}: never empty, so child 1 never matches.
        Postgrex.query!(conn, "INSERT INTO parent VALUES (2, 1, 2), (8, 2, 8)", [])
        Postgrex.query!(conn, "DELETE FROM parent WHERE id = 1", [])
        assert [[2]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows

        assert wait_until(fn ->
                 match?(
                   %{
                     active_move: _,
                     queue: %{move_in: %{0 => _, 1 => _}, move_out: %{0 => _}}
                   },
                   :sys.get_state(name).event_handler
                 )
               end)

        send(query, :resume_move_query)

        assert {:ok, [%ControlMessage{control: :must_refetch}]} =
                 await_count(consumer, 1,
                   match:
                     &(match?(%ControlMessage{control: :must_refetch}, &1) or
                         match?(%ChangeMessage{}, &1))
                 )

        assert [insert: 2] = operations_until_insert(consumer, 2)
        assert %{handle: new_handle} = assert_up_to_date(consumer)
        assert new_handle != old_handle

        Postgrex.query!(conn, "INSERT INTO child VALUES (9, 9, 9)", [])
        assert [insert: 9] = operations_until_insert(consumer, 9)
      end
    end

    for boundary <- [:before_snapshot, :after_snapshot] do
      @tag with_sql: [
             "CREATE TABLE parent (id INT PRIMARY KEY, grp INT, k INT)",
             "CREATE TABLE child (id INT PRIMARY KEY, a INT, b INT, c INT)",
             "INSERT INTO parent VALUES (1, 1, 1), (2, 2, 1), (3, 2, 9), (4, 3, 1), (5, 3, 9)",
             "INSERT INTO child VALUES (1, 2, 1, 9)"
           ]
      test "never pairs changes from distinct commits while paused #{boundary}", %{
        client: client,
        db_conn: conn,
        stack_id: stack_id
      } do
        where = """
        a NOT IN (SELECT k FROM parent WHERE grp = 1)
        OR b NOT IN (SELECT k FROM parent WHERE grp = 2)
        OR c NOT IN (SELECT k FROM parent WHERE grp = 3)
        """

        stream = Client.stream(client, ShapeDefinition.new!("child", where: where), live: true)

        with_consumer stream do
          assert_insert(consumer, %{"id" => 1})
          %{handle: handle} = assert_up_to_date(consumer)
          name = Consumer.name(stack_id, handle)
          gate_first_move_query(name, unquote(boundary))

          Postgrex.query!(conn, "DELETE FROM parent WHERE id = 4", [])
          assert_receive {:move_query_paused, query}
          assert [[1]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows

          Postgrex.query!(conn, "DELETE FROM parent WHERE id IN (1, 2)", [])
          assert [[1]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows

          Postgrex.query!(conn, "INSERT INTO parent VALUES (6, 1, 2)", [])
          assert [[1]] = Postgrex.query!(conn, "SELECT id FROM child WHERE #{where}", []).rows

          assert wait_until(fn ->
                   case :sys.get_state(name).event_handler do
                     %{active_move: _, queue: %{move_out: %{0 => _, 1 => _}, move_in: %{0 => _}}} ->
                       true

                     _ ->
                       false
                   end
                 end)

          send(query, :resume_move_query)

          assert wait_until(fn ->
                   case :sys.get_state(name).event_handler do
                     %{views: views} -> MapSet.member?(views[["$sublink", "0"]], 2)
                     _ -> false
                   end
                 end)

          Postgrex.query!(conn, "INSERT INTO child VALUES (9, 5, 5, 5)", [])
          assert [insert: 9] = operations_until_insert(consumer, 9)
        end
      end
    end
  end

  describe "a removal queued behind a move query" do
    setup [:with_unique_db, :with_sql_execute]
    setup :with_complete_stack
    setup :with_electric_client

    # Paused after its snapshot, the move-in splices the transaction after it, paused before it,
    # the snapshot sees that transaction committed. Either way the employees added with the
    # removal are written against it, which keeps the one the moving firm holds, and the drain
    # then deletes the employee the removal takes out.
    for {boundary, expected} <- [
          after_snapshot: [insert: 1, insert: 4, delete: 2, insert: 9],
          before_snapshot: [insert: 4, insert: 1, delete: 2, insert: 9]
        ] do
      @tag with_sql: [
             "CREATE TABLE firms (id INT PRIMARY KEY, active BOOLEAN NOT NULL)",
             "CREATE TABLE divisions (id INT PRIMARY KEY, active BOOLEAN NOT NULL)",
             ~s|CREATE TABLE employees (id INT PRIMARY KEY, "firmId" INT, "divisionId" INT)|,
             "INSERT INTO firms VALUES (1, false)",
             "INSERT INTO divisions VALUES (2, true)",
             "INSERT INTO employees VALUES (1, 1, NULL), (2, NULL, 2)"
           ]
      test "never sends an employee whose division leaves in its own transaction, paused #{boundary}",
           %{client: client, db_conn: conn, stack_id: stack_id} do
        where =
          ~s|"firmId" IN (SELECT id FROM firms WHERE active) OR | <>
            ~s|"divisionId" IN (SELECT id FROM divisions WHERE active)|

        stream =
          Client.stream(client, ShapeDefinition.new!("employees", where: where), live: true)

        with_consumer stream do
          assert_insert(consumer, %{"id" => 2})
          assert %{handle: handle} = assert_up_to_date(consumer)
          name = Consumer.name(stack_id, handle)
          gate_first_move_query(name, unquote(boundary))

          Postgrex.query!(conn, "UPDATE firms SET active = true WHERE id = 1", [])
          assert_receive {:move_query_paused, query}, 5_000

          assert {:ok, _} =
                   Postgrex.transaction(conn, fn tx ->
                     Postgrex.query!(tx, "UPDATE divisions SET active = false WHERE id = 2", [])

                     Postgrex.query!(
                       tx,
                       "INSERT INTO employees VALUES (3, NULL, 2), (4, 1, 2)",
                       []
                     )
                   end)

          assert [[1], [4]] =
                   Postgrex.query!(
                     conn,
                     "SELECT id FROM employees WHERE #{where} ORDER BY id",
                     []
                   ).rows

          assert wait_until(fn ->
                   case :sys.get_state(name).event_handler do
                     %{active_move: %{buffered_txn_count: count}, queue: queue} ->
                       count > 0 and queue.move_out != %{}

                     _ ->
                       false
                   end
                 end)

          send(query, :resume_move_query)

          # The drained removal runs its own move-out after the splice: the marker follows it.
          assert wait_until(fn ->
                   case live_handler(name) do
                     nil -> true
                     %{active_move: _} -> false
                     %{queue: queue} -> queue.move_in == %{} and queue.move_out == %{}
                   end
                 end)

          Postgrex.query!(conn, "INSERT INTO employees VALUES (9, 1, NULL)", [])
          assert operations_until_insert(consumer, 9) == unquote(expected)
        end
      end
    end

    # The move-in's query reads the divisions beside the firms. The employee added as their
    # division leaves would be in its snapshot, or in the commit spliced after it: the removal
    # drained behind the move takes it out of the splice, so the client never receives it. The
    # employee already there reaches the client only when the query's snapshot predates the
    # division leaving, so that Postgres selected it: the client receives it, then its delete. A
    # snapshot that sees the division leave leaves it out.
    for {boundary, expected} <- [
          after_snapshot: [insert: 1, delete: 1, insert: 9],
          before_snapshot: [insert: 9]
        ] do
      @tag with_sql: [
             "CREATE TABLE firms (id INT PRIMARY KEY, active BOOLEAN NOT NULL)",
             "CREATE TABLE divisions (id INT PRIMARY KEY, active BOOLEAN NOT NULL)",
             ~s|CREATE TABLE employees (id INT PRIMARY KEY, "firmId" INT, "divisionId" INT)|,
             "INSERT INTO firms VALUES (1, false)",
             "INSERT INTO divisions VALUES (2, true), (3, true)",
             "INSERT INTO employees VALUES (1, 1, 2)"
           ]
      test "never sends an employee added as their division leaves a firm query, paused #{boundary}",
           %{client: client, db_conn: conn, stack_id: stack_id} do
        where =
          ~s|"firmId" IN (SELECT id FROM firms WHERE active) AND | <>
            ~s|"divisionId" IN (SELECT id FROM divisions WHERE active)|

        stream =
          Client.stream(client, ShapeDefinition.new!("employees", where: where), live: true)

        with_consumer stream do
          assert %{handle: handle} = assert_up_to_date(consumer)
          name = Consumer.name(stack_id, handle)
          gate_first_move_query(name, unquote(boundary))

          Postgrex.query!(conn, "UPDATE firms SET active = true WHERE id = 1", [])
          assert_receive {:move_query_paused, query}, 5_000

          assert {:ok, _} =
                   Postgrex.transaction(conn, fn tx ->
                     Postgrex.query!(tx, "UPDATE divisions SET active = false WHERE id = 2", [])
                     Postgrex.query!(tx, "INSERT INTO employees VALUES (5, 1, 2)", [])
                   end)

          assert [] = Postgrex.query!(conn, "SELECT id FROM employees WHERE #{where}", []).rows

          assert wait_until(fn ->
                   match?(
                     %{active_move: _, queue: %{move_out: %{1 => _}}},
                     :sys.get_state(name).event_handler
                   )
                 end)

          send(query, :resume_move_query)

          assert wait_until(fn ->
                   case live_handler(name) do
                     nil -> true
                     %{active_move: _} -> false
                     %{queue: queue} -> queue.move_in == %{} and queue.move_out == %{}
                   end
                 end)

          Postgrex.query!(conn, "INSERT INTO employees VALUES (9, 1, 3)", [])
          assert operations_until_insert(consumer, 9) == unquote(expected)
        end
      end
    end

    # Two firms move in together, and one leaves while their query runs, in the transaction that
    # adds an employee to it. That employee names a division too, which the shape does not hold:
    # the move-out of the firm takes it out, so its snapshot row stays out of the log.
    for boundary <- [:after_snapshot, :before_snapshot] do
      @tag with_sql: [
             "CREATE TABLE firms (id INT PRIMARY KEY, active BOOLEAN NOT NULL)",
             "CREATE TABLE divisions (id INT PRIMARY KEY, active BOOLEAN NOT NULL)",
             ~s|CREATE TABLE employees (id INT PRIMARY KEY, "firmId" INT, "divisionId" INT)|,
             "INSERT INTO firms VALUES (1, false), (2, false)",
             "INSERT INTO divisions VALUES (5, false)",
             "INSERT INTO employees VALUES (2, 2, NULL)"
           ]
      test "never sends an employee added to a firm that leaves as it moves in, paused #{boundary}",
           %{client: client, db_conn: conn, stack_id: stack_id} do
        where =
          ~s|"firmId" IN (SELECT id FROM firms WHERE active) OR | <>
            ~s|"divisionId" IN (SELECT id FROM divisions WHERE active)|

        stream =
          Client.stream(client, ShapeDefinition.new!("employees", where: where), live: true)

        with_consumer stream do
          assert %{handle: handle} = assert_up_to_date(consumer)
          name = Consumer.name(stack_id, handle)
          gate_first_move_query(name, unquote(boundary))

          Postgrex.query!(conn, "UPDATE firms SET active = true WHERE id IN (1, 2)", [])
          assert_receive {:move_query_paused, query}, 5_000

          assert {:ok, _} =
                   Postgrex.transaction(conn, fn tx ->
                     Postgrex.query!(tx, "UPDATE firms SET active = false WHERE id = 1", [])
                     Postgrex.query!(tx, "INSERT INTO employees VALUES (1, 1, 5)", [])
                   end)

          assert [[2]] = Postgrex.query!(conn, "SELECT id FROM employees WHERE #{where}", []).rows

          assert wait_until(fn ->
                   match?(
                     %{active_move: _, queue: %{move_out: %{0 => _}}},
                     :sys.get_state(name).event_handler
                   )
                 end)

          send(query, :resume_move_query)

          assert wait_until(fn ->
                   case live_handler(name) do
                     nil -> true
                     %{active_move: _} -> false
                     %{queue: queue} -> queue.move_in == %{} and queue.move_out == %{}
                   end
                 end)

          Postgrex.query!(conn, "INSERT INTO employees VALUES (9, 2, NULL)", [])
          assert operations_until_insert(consumer, 9) == [insert: 2, insert: 9]
        end
      end
    end

    # The firm moves in in the transaction that deactivates the division, so Postgres never selects
    # the employee already there, though the firm query's snapshot reads it against the divisions
    # before the removal queued behind the move. Each dependency's materializer reports the commit
    # on its own, in either order: holding the divisions' one until the firm query runs queues the
    # removal behind the move.
    @tag with_sql: [
           "CREATE TABLE firms (id INT PRIMARY KEY, active BOOLEAN NOT NULL)",
           "CREATE TABLE divisions (id INT PRIMARY KEY, active BOOLEAN NOT NULL)",
           ~s|CREATE TABLE employees (id INT PRIMARY KEY, "firmId" INT, "divisionId" INT)|,
           "INSERT INTO firms VALUES (1, false)",
           "INSERT INTO divisions VALUES (2, true), (3, true)",
           "INSERT INTO employees VALUES (1, 1, 2)"
         ]
    test "never sends an employee whose division leaves as their firm moves in",
         %{client: client, db_conn: conn, stack_id: stack_id} do
      where =
        ~s|"firmId" IN (SELECT id FROM firms WHERE active) AND | <>
          ~s|"divisionId" IN (SELECT id FROM divisions WHERE active)|

      stream = Client.stream(client, ShapeDefinition.new!("employees", where: where), live: true)

      with_consumer stream do
        assert %{handle: handle} = assert_up_to_date(consumer)
        name = Consumer.name(stack_id, handle)
        gate_first_move_query(name, :before_snapshot)

        %{
          shape: %{
            shape_dependencies: [_, divisions],
            shape_dependencies_handles: [_, dep_handle]
          }
        } =
          :sys.get_state(name)

        assert {_, "divisions"} = divisions.root_table
        materializer = Materializer.whereis(stack_id, dep_handle)
        :ok = :sys.suspend(materializer)

        assert {:ok, _} =
                 Postgrex.transaction(conn, fn tx ->
                   Postgrex.query!(tx, "UPDATE firms SET active = true WHERE id = 1", [])
                   Postgrex.query!(tx, "UPDATE divisions SET active = false WHERE id = 2", [])
                 end)

        assert_receive {:move_query_paused, query}, 5_000
        :ok = :sys.resume(materializer)
        assert [] = Postgrex.query!(conn, "SELECT id FROM employees WHERE #{where}", []).rows

        assert wait_until(fn ->
                 match?(
                   %{active_move: _, queue: %{move_out: %{1 => _}}},
                   :sys.get_state(name).event_handler
                 )
               end)

        send(query, :resume_move_query)

        assert wait_until(fn ->
                 case live_handler(name) do
                   nil -> true
                   %{active_move: _} -> false
                   %{queue: queue} -> queue.move_in == %{} and queue.move_out == %{}
                 end
               end)

        Postgrex.query!(conn, "INSERT INTO employees VALUES (9, 1, 3)", [])
        assert operations_until_insert(consumer, 9) == [insert: 9]
      end
    end
  end

  defp live_handler(pid) do
    :sys.get_state(pid).event_handler
  catch
    :exit, {:noproc, _} -> nil
    :exit, {:normal, _} -> nil
    :exit, {:shutdown, _} -> nil
    # A consumer retiring its shape during the call exits with {:shutdown, :cleanup}.
    :exit, {{:shutdown, _}, _} -> nil
  end

  defp gate_first_move_query(consumer, boundary) do
    test = self()
    gate = :atomics.new(1, [])

    pause = fn ->
      send(test, {:move_query_paused, self()})

      receive do
        :resume_move_query -> :ok
      end
    end

    Repatch.patch(SnapshotQuery, :execute_for_shape, [mode: :shared], fn pool,
                                                                         handle,
                                                                         shape,
                                                                         opts ->
      opts =
        if opts[:query_reason] == "move_in_query" and
             :atomics.compare_exchange(gate, 1, 0, 1) == :ok do
          case boundary do
            :before_snapshot ->
              pause.()
              opts

            :after_snapshot ->
              Keyword.update!(opts, :query_fn, fn query ->
                fn conn, snapshot, lsn ->
                  pause.()
                  query.(conn, snapshot, lsn)
                end
              end)
          end
        else
          opts
        end

      Repatch.real(SnapshotQuery.execute_for_shape(pool, handle, shape, opts))
    end)

    # The query task inherits the consumer's callers.
    Repatch.patch(Effects, :query_move_in_async, [mode: :shared], fn supervisor,
                                                                     state,
                                                                     move,
                                                                     pid ->
      callers = Process.get(:"$callers", [])
      Process.put(:"$callers", [test | callers])

      try do
        Repatch.real(Effects.query_move_in_async(supervisor, state, move, pid))
      after
        Process.put(:"$callers", callers)
      end
    end)

    Repatch.allow(test, consumer)
  end

  defp operations_until_insert(consumer, marker, operations \\ []) do
    assert {:ok, [message]} =
             await_count(consumer, 1,
               match: fn
                 %ControlMessage{control: :must_refetch} -> true
                 %ChangeMessage{} -> true
                 _ -> false
               end
             )

    case message do
      %ControlMessage{control: :must_refetch} ->
        operations_until_insert(consumer, marker, [:must_refetch | operations])

      %ChangeMessage{headers: %{operation: :insert}, value: %{"id" => ^marker}} ->
        Enum.reverse([{:insert, marker} | operations])

      %ChangeMessage{headers: %{operation: operation}, value: %{"id" => id}} ->
        operations_until_insert(consumer, marker, [{operation, id} | operations])
    end
  end

  defp composite_view_contains?(views, key) do
    views[["$sublink", "0"]]
    |> then(fn
      %{keys: keys} -> keys
      keys -> keys
    end)
    |> MapSet.member?(key)
  end

  defp create_legacy_shape(stack_id, where) do
    shape =
      Shape.new!("child", inspector: Inspector.for_stack(stack_id), where: where)
      |> Map.delete(:composite_tag_encoding)

    {handle, _} = Shapes.get_or_create_shape_handle(stack_id, shape)
    handle
  end

  # Helper to set up inner/outer tables for subquery tests
  def with_inner_outer_tables(%{db_conn: conn} = _context) do
    statements = [
      """
      CREATE TABLE inner_table (
        id TEXT PRIMARY KEY,
        active BOOLEAN NOT NULL DEFAULT true
      )
      """,
      """
      CREATE TABLE outer_table (
        id TEXT PRIMARY KEY,
        inner_id TEXT NOT NULL REFERENCES inner_table(id) ON DELETE CASCADE,
        value TEXT NOT NULL
      )
      """
    ]

    Enum.each(statements, &Postgrex.query!(conn, &1, []))

    %{tables: [{"public", "inner_table"}, {"public", "outer_table"}]}
  end

  # Helper to set up parent/child tables for subquery tests
  def with_parent_child_tables(%{db_conn: conn} = _context) do
    statements = [
      """
      CREATE TABLE parent (
        id TEXT PRIMARY KEY,
        active BOOLEAN NOT NULL DEFAULT true
      )
      """,
      """
      CREATE TABLE child (
        id TEXT PRIMARY KEY,
        parent_id TEXT NOT NULL REFERENCES parent(id) ON DELETE CASCADE,
        value TEXT NOT NULL
      )
      """
    ]

    Enum.each(statements, &Postgrex.query!(conn, &1, []))

    %{tables: [{"public", "parent"}, {"public", "child"}]}
  end
end
