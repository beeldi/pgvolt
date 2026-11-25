defmodule Electric.SnapshotError do
  alias Electric.SnapshotError

  defexception [:message, :type, :original_error]

  def table_lock_timeout do
    %SnapshotError{
      type: :table_lock_timeout,
      message: "Snapshot timed out while waiting for a table lock"
    }
  end

  def connection_not_available do
    %SnapshotError{
      type: :connection_not_available,
      message: "Database connection not available"
    }
  end

  def publication_admission_pending do
    %SnapshotError{
      type: :publication_admission_pending,
      message: "Waiting for the table to be admitted to the publication"
    }
  end

  def publication_admission_timeout do
    %{
      publication_admission_pending()
      | message: "Snapshot timed out while waiting for a table lock"
    }
  end

  def exited(reason) do
    %SnapshotError{
      type: exit_type(reason),
      message: "Snapshot creation exited: #{inspect(reason)}",
      original_error: reason
    }
  end

  # A snapshot task that exits because the connection subsystem (or the shape
  # subsystem) is shutting down or the pooled call raced a restart is a
  # transient failure, not an invalid shape. Classifying it retryable keeps the
  # shape in place for the next request instead of deleting it and triggering a
  # full re-snapshot fan-out at the moment the connection recovers.
  defp exit_type(:shutdown), do: :shutdown
  defp exit_type(:normal), do: :shutdown
  defp exit_type(:killed), do: :shutdown
  defp exit_type(:noproc), do: :shutdown
  defp exit_type({:shutdown, _}), do: :shutdown
  defp exit_type({:noproc, _}), do: :shutdown
  defp exit_type({:timeout, _}), do: :shutdown
  defp exit_type(_), do: :unknown

  def slow_snapshot_query(ttf_ms) do
    %SnapshotError{
      type: :slow_snapshot_query,
      message:
        "Snapshot query took too long to return data (cancelled after #{ttf_ms}ms). Please ensure snapshot queries are using an index to avoid taking up all database connections."
    }
  end

  def slow_snapshot_start do
    %SnapshotError{
      type: :slow_snapshot_start,
      message: "Snapshot query took too long to start reading from the database"
    }
  end

  def from_error(%DBConnection.ConnectionError{reason: :queue_timeout} = error) do
    %SnapshotError{
      type: :queue_timeout,
      message: "Snapshot creation failed because of a connection pool queue timeout",
      original_error: error
    }
  end

  def from_error(%Postgrex.Error{postgres: %{code: code}} = error)
      when code in ~w|undefined_function undefined_table undefined_column|a do
    %SnapshotError{
      type: :schema_changed,
      message: "Schema changed while creating snapshot",
      original_error: error
    }
  end

  def from_error(%Postgrex.Error{postgres: %{code: :insufficient_privilege}} = error) do
    %SnapshotError{
      type: :missing_privilege,
      message: "Unable to create initial snapshot: " <> error.postgres.message,
      original_error: error
    }
  end

  def from_error(
        %Postgrex.Error{
          postgres: %{
            code: :undefined_object,
            message: "publication " <> _ = message,
            severity: "ERROR",
            pg_code: "42704"
          }
        } = error
      ) do
    %SnapshotError{
      type: :missing_publication,
      message: message,
      original_error: error
    }
  end

  def from_error(%Postgrex.Error{} = error) do
    %SnapshotError{
      type: :unknown,
      message: error.postgres.message,
      original_error: error
    }
  end

  def from_error(%Electric.DbConfigurationError{} = error) do
    %SnapshotError{
      type: error.type,
      message: error.message,
      original_error: error
    }
  end

  def from_error(%File.Error{} = error) do
    %SnapshotError{
      type: :storage,
      message: Exception.message(error),
      original_error: error
    }
  end

  def from_error(%SnapshotError{} = error), do: error

  def from_error(error) do
    %SnapshotError{
      type: :unknown,
      message: Exception.message(error),
      original_error: error
    }
  end

  # Failures caused by the environment going away rather than by the shape being
  # invalid. The consumer keeps the shape in place for these and retries on the
  # next request instead of deleting it.
  @retryable_types [
    :queue_timeout,
    :connection_not_available,
    :publication_admission_pending,
    :shutdown
  ]

  def retryable?(%SnapshotError{type: type}), do: type in @retryable_types
end
