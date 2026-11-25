defmodule Electric.Postgres.ReplicationClient.ConnectionSetupTest do
  use ExUnit.Case, async: true

  alias Electric.Postgres.ReplicationClient.ConnectionSetup
  alias Electric.Postgres.ReplicationClient.SlotIncarnation
  alias Electric.Postgres.ReplicationClient.State
  alias Electric.Postgres.Lsn

  defp base_state(overrides) do
    Map.merge(
      %State{
        handle_event: fn _, _ -> :ok end,
        publication_name: "test_pub",
        slot_name: "test_slot",
        display_settings: ["SET dummy = 'test'"],
        flushed_wal: 0
      },
      overrides
    )
  end

  describe "process_query_result/2 returns updated state" do
    test "create_slot result includes updated flushed_wal in returned state" do
      slot_lsn = "0/1A2B3C4"
      expected_wal = slot_lsn |> Lsn.from_string() |> Lsn.to_integer()

      prepared = SlotIncarnation.prepare(nil, "stack", "test_slot")
      state = base_state(%{step: :create_slot, slot_incarnation: prepared})

      create_result = [
        %Postgrex.Result{
          command: :create,
          columns: ["slot_name", "consistent_point", "snapshot_name", "output_plugin"],
          rows: [["test_slot", slot_lsn, nil, "pgoutput"]],
          num_rows: 1
        }
      ]

      {_step, _next_step, :created_new_slot, updated_state, _return_val} =
        ConnectionSetup.process_query_result(create_result, state)

      assert updated_state.flushed_wal == expected_wal - 1
      assert updated_state.replay_before_lsn == expected_wal
      assert updated_state.slot_incarnation == prepared.pending
    end

    test "an existing slot keeps its settled incarnation" do
      kv = Electric.PersistentKV.Memory.new!()
      created = SlotIncarnation.prepare(kv, "stack", "test_slot")
      settled = SlotIncarnation.settle(kv, "stack", created, :created)

      state =
        base_state(%{
          step: :create_slot,
          persistent_kv: kv,
          stack_id: "stack",
          slot_incarnation: SlotIncarnation.prepare(kv, "stack", "test_slot")
        })

      already_exists = %Postgrex.Error{
        postgres: %{
          code: :duplicate_object,
          pg_code: "42710",
          message: ~s|replication slot "test_slot" already exists|
        }
      }

      {_step, :query_slot_flushed_lsn, nil, updated_state, _return_val} =
        ConnectionSetup.process_query_result(already_exists, state)

      assert updated_state.slot_incarnation == settled
    end

    test "query_slot_flushed_lsn result includes updated flushed_wal in returned state" do
      slot_lsn = "0/5D6E7F8"
      expected_wal = slot_lsn |> Lsn.from_string() |> Lsn.to_integer()

      state = base_state(%{step: :query_slot_flushed_lsn, flushed_wal: 0})

      query_result = [
        %Postgrex.Result{
          command: :select,
          columns: ["confirmed_flush_lsn"],
          rows: [[slot_lsn]],
          num_rows: 1
        }
      ]

      {_step, _next_step, _extra_info, updated_state, _return_val} =
        ConnectionSetup.process_query_result(query_result, state)

      assert updated_state.flushed_wal == expected_wal - 1
      assert updated_state.replay_before_lsn == expected_wal
    end

    @tag :tmp_dir
    test "keeps the durable journal boundary separate from the source replay cutoff", %{
      tmp_dir: base_path
    } do
      journal =
        Electric.Replication.TxnJournal.open_writer!(base_path)
        |> Electric.Replication.TxnJournal.append_event!({:idle_slot_boundary, 100})
        |> Electric.Replication.TxnJournal.sync!(100)

      File.close(journal.file)

      state =
        base_state(%{
          step: :query_slot_flushed_lsn,
          ack_after_journal?: true,
          journal_base_path: base_path
        })

      result = [
        %Postgrex.Result{
          command: :select,
          columns: ["confirmed_flush_lsn"],
          rows: [["0/C9"]],
          num_rows: 1
        }
      ]

      {_, _, _, updated, _} = ConnectionSetup.process_query_result(result, state)
      assert updated.flushed_wal == 100
      assert updated.replay_before_lsn == 201
    end
  end

  describe "an existing publication" do
    defp publication_row(overrides) do
      row =
        Map.merge(
          %{"pubinsert" => "t", "pubupdate" => "t", "pubdelete" => "t", "pubtruncate" => "t"},
          overrides
        )

      [%Postgrex.Result{num_rows: 1, columns: Map.keys(row), rows: [Map.values(row)]}]
    end

    @already_exists %Postgrex.Error{
      postgres: %{
        code: :duplicate_object,
        pg_code: "42710",
        message: ~s|publication "test_pub" already exists|
      }
    }

    test "is checked before it is used" do
      state = base_state(%{step: :create_publication})

      assert {:create_publication, :check_if_publication_exists, nil, _state, _} =
               ConnectionSetup.process_query_result(@already_exists, state)
    end

    test "that publishes every operation leaves the slot as it is" do
      state = base_state(%{step: :check_if_publication_exists, publication_owner?: true})

      assert {_, :query_slot_flushed_lsn, nil, %State{recreate_slot?: nil}, _} =
               ConnectionSetup.process_query_result(publication_row(%{}), state)
    end

    test "missing an operation drops the slot, gets the operations back, then makes a new slot" do
      state = base_state(%{step: :check_if_publication_exists, publication_owner?: true})

      assert {_, :drop_slot, nil, _, {:query, _, state}} =
               ConnectionSetup.process_query_result(
                 publication_row(%{"pubtruncate" => "f"}),
                 state
               )

      assert {:drop_slot, :repair_publication, nil, _, {:query, query, state}} =
               ConnectionSetup.process_query_result([%Postgrex.Result{command: :select}], state)

      assert query ==
               ~s|ALTER PUBLICATION "test_pub" SET (publish = 'insert, update, delete, truncate')|

      assert {:repair_publication, :create_slot, nil, %State{repair_publication?: false}, _} =
               ConnectionSetup.process_query_result([%Postgrex.Result{command: :alter}], state)
    end

    test "missing an operation fails setup in manual publishing mode" do
      state =
        base_state(%{
          step: :check_if_publication_exists,
          publication_owner?: true,
          manual_table_publishing?: true
        })

      assert_raise Electric.DbConfigurationError,
                   ~r/does not publish all required operations/,
                   fn ->
                     ConnectionSetup.process_query_result(
                       publication_row(%{"pubtruncate" => "f"}),
                       state
                     )
                   end
    end

    test "missing an operation fails setup when Electric cannot change the publication" do
      state = base_state(%{step: :check_if_publication_exists, publication_owner?: false})

      assert_raise Electric.DbConfigurationError, fn ->
        ConnectionSetup.process_query_result(publication_row(%{"pubdelete" => "f"}), state)
      end

      state = base_state(%{step: :repair_publication, publication_owner?: true})

      not_owner = %Postgrex.Error{
        postgres: %{
          code: :insufficient_privilege,
          pg_code: "42501",
          message: ~s|must be owner of publication test_pub|
        }
      }

      assert_raise Electric.DbConfigurationError, fn ->
        ConnectionSetup.process_query_result(not_owner, state)
      end
    end
  end

  # Postgres translates error messages into the server's `lc_messages` language, so setup must
  # recognise each expected error by its code alone. These carry the German translations.
  describe "expected errors from a server that does not report in English" do
    defp pg_error(code, pg_code, message) do
      %Postgrex.Error{postgres: %{code: code, pg_code: pg_code, message: message}}
    end

    test "an existing publication is used" do
      state = base_state(%{step: :create_publication})
      error = pg_error(:duplicate_object, "42710", ~s|Publikation »test_pub« existiert bereits|)

      assert {:create_publication, :check_if_publication_exists, nil,
              %State{publication_owner?: true}, _} =
               ConnectionSetup.process_query_result(error, state)
    end

    test "an existing slot is used" do
      state =
        base_state(%{
          step: :create_slot,
          slot_incarnation: SlotIncarnation.prepare(nil, "stack", "test_slot")
        })

      error =
        pg_error(:duplicate_object, "42710", ~s|Replikations-Slot »test_slot« existiert bereits|)

      assert {:create_slot, :query_slot_flushed_lsn, nil, _state, _} =
               ConnectionSetup.process_query_result(error, state)
    end

    test "a slot that is already gone needs no dropping" do
      state = base_state(%{step: :drop_slot})

      error =
        pg_error(:undefined_object, "42704", ~s|Replikations-Slot »test_slot« existiert nicht|)

      assert {:drop_slot, :create_slot, nil, _state, _} =
               ConnectionSetup.process_query_result(error, state)
    end

    test "a statement timeout while waiting for the lock is retried" do
      state = base_state(%{step: :acquire_lock})

      error =
        pg_error(
          :query_canceled,
          "57014",
          "storniere Anfrage wegen Zeitüberschreitung der Anweisung"
        )

      assert {:acquire_lock, :acquire_lock, {:lock_acquisition_failed, ^error},
              %State{lock_acquired?: false}, _} =
               ConnectionSetup.process_query_result(error, state)
    end
  end

  describe "reading the timeline history" do
    @identify_system [
      %Postgrex.Result{
        command: :identify,
        columns: ["systemid", "timeline", "xlogpos", "dbname"],
        rows: [["7000000000000000001", "3", "0/1000100", "postgres"]],
        num_rows: 1
      }
    ]

    test "reads the history of the connected timeline when keeping shapes on promotion is on" do
      state = base_state(%{step: :identify_system, keep_shapes_on_promotion?: true})

      assert {:identify_system, :query_timeline_history, _info, _state,
              {:query, "TIMELINE_HISTORY 3", _}} =
               ConnectionSetup.process_query_result(@identify_system, state)
    end

    test "skips the history when keeping shapes on promotion is off" do
      state = base_state(%{step: :identify_system, keep_shapes_on_promotion?: false})

      assert {:identify_system, :query_pg_info, _info, _state, {:query, _pg_info, _}} =
               ConnectionSetup.process_query_result(@identify_system, state)
    end

    test "skips the history on timeline 1, which has none" do
      state = base_state(%{step: :identify_system, keep_shapes_on_promotion?: true})

      [%{rows: [[systemid, _timeline, xlogpos, dbname]]} = result] = @identify_system
      first_timeline = [%{result | rows: [[systemid, "1", xlogpos, dbname]]}]

      assert {:identify_system, :query_pg_info, _info, _state, _query} =
               ConnectionSetup.process_query_result(first_timeline, state)
    end

    test "hands the parsed history on" do
      state =
        base_state(%{
          step: :query_timeline_history,
          keep_shapes_on_promotion?: true,
          timeline_id: "3"
        })

      result = [
        %Postgrex.Result{
          command: :timeline,
          columns: ["filename", "content"],
          rows: [
            [
              "00000003.history",
              "1\t0/1000000\tno recovery target specified\n" <>
                "2\t2/20000000\tno recovery target specified\n"
            ]
          ],
          num_rows: 1
        }
      ]

      assert {:query_timeline_history, :query_pg_info, {:ok, history}, _state, _query} =
               ConnectionSetup.process_query_result(result, state)

      assert history == [
               {1, Lsn.to_integer(Lsn.from_string("0/1000000"))},
               {2, Lsn.to_integer(Lsn.from_string("2/20000000"))}
             ]
    end

    # Without a history the manager treats a timeline change as a rewind, as it did before it
    # could read one, so a failure here must not stop the connection.
    test "hands a failure on and carries on connecting" do
      state =
        base_state(%{
          step: :query_timeline_history,
          keep_shapes_on_promotion?: true,
          timeline_id: "3"
        })

      error = %Postgrex.Error{
        postgres: %{
          code: :undefined_file,
          pg_code: "58P01",
          message: ~s|could not open file "pg_wal/00000003.history": No such file or directory|
        }
      }

      assert {:query_timeline_history, :query_pg_info, {:error, message}, _state, _query} =
               ConnectionSetup.process_query_result(error, state)

      assert message =~ "00000003.history"
    end
  end
end
