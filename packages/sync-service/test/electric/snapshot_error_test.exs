defmodule Electric.SnapshotErrorTest do
  use ExUnit.Case, async: true

  alias Electric.SnapshotError

  describe "table_lock_timeout/0" do
    test "returns table lock timeout error" do
      assert %SnapshotError{
               type: :table_lock_timeout,
               message: "Snapshot timed out while waiting for a table lock"
             } = SnapshotError.table_lock_timeout()
    end
  end

  describe "from_error/1" do
    test "with a queue timeout DBConnection error" do
      error = %DBConnection.ConnectionError{
        reason: :queue_timeout,
        message: "client queue timeout",
        severity: :error
      }

      assert %SnapshotError{
               type: :queue_timeout,
               message: "Snapshot creation failed because of a connection pool queue timeout",
               original_error: ^error
             } = SnapshotError.from_error(error)
    end

    test "with schema changed (undefined_table) error" do
      error = %Postgrex.Error{
        postgres: %{code: :undefined_table, message: "relation \"items\" does not exist"}
      }

      assert %SnapshotError{
               type: :schema_changed,
               message: "Schema changed while creating snapshot",
               original_error: ^error
             } = SnapshotError.from_error(error)
    end

    test "with insufficient privilege error" do
      error = %Postgrex.Error{
        postgres: %{code: :insufficient_privilege, message: "permission denied for table items"}
      }

      assert %SnapshotError{
               type: :missing_privilege,
               message: "Unable to create initial snapshot: permission denied for table items",
               original_error: ^error
             } = SnapshotError.from_error(error)
    end

    test "with missing publication error" do
      error = %Postgrex.Error{
        postgres: %{
          code: :undefined_object,
          message: "publication \"pub_foo\" does not exist",
          severity: "ERROR",
          pg_code: "42704"
        }
      }

      assert %SnapshotError{
               type: :missing_publication,
               message: "publication \"pub_foo\" does not exist",
               original_error: ^error
             } = SnapshotError.from_error(error)
    end

    test "with generic Postgrex error" do
      error = %Postgrex.Error{postgres: %{code: :some_other, message: "some other message"}}

      assert %SnapshotError{
               type: :unknown,
               message: "some other message",
               original_error: ^error
             } = SnapshotError.from_error(error)
    end

    test "with DbConfigurationError" do
      error = %Electric.DbConfigurationError{
        type: :tables_missing_from_publication,
        message: "tables missing"
      }

      assert %SnapshotError{
               type: :tables_missing_from_publication,
               message: "tables missing",
               original_error: ^error
             } = SnapshotError.from_error(error)
    end

    test "with arbitrary error" do
      error = %RuntimeError{message: "runtime oops"}

      assert %SnapshotError{
               type: :unknown,
               message: "runtime oops",
               original_error: ^error
             } = SnapshotError.from_error(error)
    end

    test "with a File.Error" do
      error = %File.Error{
        reason: :eexist,
        path: "./persistent/shapes/single_stack/49493699-17",
        action: "open"
      }

      assert %SnapshotError{
               type: :storage,
               message:
                 "could not open \"./persistent/shapes/single_stack/49493699-17\": file already exists",
               original_error: ^error
             } = SnapshotError.from_error(error)
    end

    defmodule MyError do
      defexception [:reason]
      def message(%{reason: reason}), do: "exception due to: #{reason}"
    end

    test "with any exception" do
      error = %MyError{reason: "belief"}

      assert %SnapshotError{
               type: :unknown,
               message: "exception due to: belief",
               original_error: ^error
             } = SnapshotError.from_error(error)
    end
  end

  describe "exited/1" do
    test "classifies shutdown-class exits as a retryable :shutdown" do
      for reason <- [
            :shutdown,
            :normal,
            :killed,
            :noproc,
            {:shutdown, :db_gone},
            {:noproc, {GenServer, :call, []}},
            {:timeout, {GenServer, :call, []}}
          ] do
        assert %SnapshotError{type: :shutdown, original_error: ^reason} =
                 SnapshotError.exited(reason)
      end
    end

    test "classifies an unrecognised exit reason as :unknown" do
      assert %SnapshotError{type: :unknown, original_error: :boom} = SnapshotError.exited(:boom)
    end
  end

  describe "retryable?/1" do
    test "is true for environment-transient failures" do
      assert SnapshotError.retryable?(SnapshotError.exited(:shutdown))

      assert SnapshotError.retryable?(
               SnapshotError.from_error(%DBConnection.ConnectionError{
                 reason: :queue_timeout,
                 message: "queue timeout",
                 severity: :error
               })
             )

      assert SnapshotError.retryable?(SnapshotError.connection_not_available())
    end

    test "is false for failures that indicate the shape itself is invalid" do
      refute SnapshotError.retryable?(SnapshotError.exited(:boom))
      refute SnapshotError.retryable?(%SnapshotError{type: :schema_changed})
      refute SnapshotError.retryable?(%SnapshotError{type: :missing_privilege})
      refute SnapshotError.retryable?(%SnapshotError{type: :unknown})
    end
  end
end
