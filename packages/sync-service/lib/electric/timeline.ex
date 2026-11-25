defmodule Electric.Timeline do
  @moduledoc """
  Module exporting functions for handling Postgres timelines.
  Verifies the Postgres ID and its timeline.
  """
  require Logger
  alias Electric.PersistentKV
  alias Electric.Postgres.Lsn
  alias Electric.Postgres.ReplicationClient.SlotIncarnation

  @type pg_id :: non_neg_integer()
  @type timeline_id :: integer()
  @type timeline :: {pg_id(), timeline_id()} | nil

  @type stored_timeline :: timeline() | :unreadable

  @type check_result :: :ok | :timeline_changed | :no_previous_timeline | :timeline_continued

  @typedoc """
  The current timeline's history file as `TIMELINE_HISTORY` returns it: each ancestor timeline
  with the LSN where its successor branched off it. WAL below that LSN is the same on both.
  """
  @type history :: [{timeline_id(), switchpoint_lsn :: non_neg_integer()}]

  @typedoc """
  What a timeline change needs to be read as a promotion that kept everything Electric took from
  the old timeline. `processed_lsn` returns the highest LSN Electric may have taken from it (a
  commit, a fragment of one, or a keepalive position), or why that is unknown. It is only called
  when the timeline changed.
  """
  @type promotion :: %{
          history: {:ok, history()} | {:error, term()},
          processed_lsn: (-> {:ok, non_neg_integer()} | {:error, String.t()})
        }

  @doc """
  Checks that we're connected to the same Postgres DB as before and on the same timeline.
  TO this end, it checks the provided `pg_id` against the persisted PG ID.
  If the PG IDs match, it also checks the provided `pg_timeline` against the persisted timeline.
  Normally, Postgres and Electric are on the same timeline and nothing must be done.
  If the timelines differ, that indicates that a Point In Time Recovery (PITR) has occurred and all shapes must be cleaned.
  If we fail to fetch timeline information, we also clean all shapes for safety as we can't be sure that Postgres and Electric are on the same timeline.
  On `:timeline_changed` the new timeline is not stored yet: the record keeps the purge owed,
  and every later check returns `:timeline_changed` until the caller has purged the shapes and
  stored the new timeline with `store_timeline/2`.

  A check whose write fails returns `{:error, result, reason}`: on `:timeline_changed`, no purge
  is owed on disk then, and the caller must purge nothing until a later check stores it.

  With `missing_owes_purge?: true` in `opts`, a missing record is a change too, and the check
  stores the purge it owes: the caller, or a caller before it, found a purge owed that it has not
  finished, and a first start would keep the shapes it left. With `rewrite_owed_purge?: true`, an
  owed purge the check reads is written again: the caller's write of it failed, maybe once its
  rename had landed, and it is not durable until a write of it succeeds. On a change, the
  `before_owing_purge` function in `opts`, if any, runs before the check writes anything.
  On first boot, `before_initializing` runs before the check stores the first timeline.

  A promotion also starts a new timeline, without rewinding anything: the new timeline branches
  off the stored one at a switch point, and WAL below it is the same on both. Given the
  `promotion` evidence, a change is `:timeline_continued` when the stored timeline is an ancestor
  of the current one and Electric took nothing from the stored timeline at or past the switch
  point. Shapes stay valid then, and the stored record keeps naming the timeline the stream
  started on, which `stream_timeline_id/2` reports.
  """
  @spec check(timeline(), keyword(), promotion() | nil) ::
          check_result() | {:error, check_result(), term()}
  def check({pg_id, timeline_id} = pg_timeline, opts, promotion \\ nil) do
    record = opts |> load_record() |> owed_when_missing(opts)
    electric_timeline = stored_timeline(record)
    result = verify_timeline(pg_timeline, electric_timeline, promotion)

    # A change stops the caller serving the old timeline's shapes first: a caller that died once
    # the purge was written, before it purged, left them served until another one purged them.
    before_owing_purge = Keyword.get(opts, :before_owing_purge, fn -> :ok end)
    if result == :timeline_changed, do: before_owing_purge.()

    # Whenever the newly fetched timeline differs from the stored one, overwrite it. A
    # continuation keeps the timeline the stream started on; anything else starts a new stream.
    # A change stores the purge it owes instead: the caller stores the new timeline once the
    # shapes are purged, so a restart in between purges again instead of keeping them.
    cond do
      result == :timeline_continued ->
        store_record([pg_id, timeline_id, stream_timeline(record)], opts)

      # A purge already owed is not written again: a retry rewrote it on the full disk that had
      # failed the purge step, and raised out of the backoff the caller retries it under.
      result == :timeline_changed and electric_timeline == {pg_id, -1} and
          not Keyword.get(opts, :rewrite_owed_purge?, false) ->
        :ok

      result == :timeline_changed ->
        store_timeline({pg_id, -1}, opts)

      result == :no_previous_timeline ->
        before_initializing = Keyword.get(opts, :before_initializing, fn -> :ok end)
        with :ok <- before_initializing.(), do: store_timeline(pg_timeline, opts)

      true ->
        :ok
    end
    |> checked(result)
  end

  # A write that raised restarted the caller at once, on the full disk that had failed it, until
  # its supervisors gave up. It is returned with the result for the caller to retry after a pause.
  defp checked(:ok, result), do: result
  defp checked({:error, reason}, result), do: {:error, result, reason}

  # Decided on the record the check reads: a caller that read it first, and took a missing record
  # for a purge it owed, could read it once more there, and the check stored the connected
  # timeline over it.
  defp owed_when_missing(nil, opts),
    do: if(Keyword.get(opts, :missing_owes_purge?, false), do: :missing, else: nil)

  defp owed_when_missing(record, _opts), do: record

  @spec verify_timeline(timeline(), stored_timeline() | :missing, promotion() | nil) ::
          check_result()
  defp verify_timeline({pg_id, timeline_id} = timeline, timeline, _promotion) do
    Logger.notice("Connected to Postgres #{pg_id} and timeline #{timeline_id}")
    :ok
  end

  defp verify_timeline({pg_id, timeline_id}, nil, _promotion) do
    Logger.notice("No previous timeline detected.")
    Logger.notice("Connected to Postgres #{pg_id} and timeline #{timeline_id}")
    :no_previous_timeline
  end

  defp verify_timeline({pg_id, _}, {electric_pg_id, _}, _promotion)
       when pg_id != electric_pg_id do
    Logger.warning(
      "Detected different Postgres DB, with ID: #{pg_id}. Old Postgres DB had ID #{electric_pg_id}."
    )

    :timeline_changed
  end

  defp verify_timeline({_, _}, {_, -1}, _promotion) do
    :timeline_changed
  end

  # A record we cannot read is not a record that was never written. The second means first boot,
  # which deliberately skips the purge and suppresses an explicit one -- so reading one as the
  # other lets a PITR through unnoticed. Degrade to what this module already documents for missing
  # timeline information: assume it changed.
  defp verify_timeline({_, timeline_id}, :unreadable, _promotion) do
    Logger.warning(
      "Could not read the stored timeline. Assuming timeline #{timeline_id} is a change and purging shapes."
    )

    :timeline_changed
  end

  # A record gone while a purge the caller or one before it found owed is unfinished is not a
  # first start: one stored the connected timeline before the purge, and a restart in between
  # loaded the old timeline's shapes again with nothing owed.
  defp verify_timeline({_, timeline_id}, :missing, _promotion) do
    Logger.warning(
      "The stored timeline is gone while a purge of every shape may be owed. " <>
        "Assuming timeline #{timeline_id} is a change and purging shapes."
    )

    :timeline_changed
  end

  defp verify_timeline({_, timeline_id}, {_, electric_timeline_id}, promotion) do
    case continuation(electric_timeline_id, timeline_id, promotion) do
      {:continued, switchpoint, processed_lsn} ->
        Logger.notice(
          "Timeline #{timeline_id} continues timeline #{electric_timeline_id}: it branched off at " <>
            "#{Lsn.from_integer(switchpoint)} and Electric took nothing past " <>
            "#{Lsn.from_integer(processed_lsn)}. Keeping shapes."
        )

        :timeline_continued

      {:purge, nil} ->
        Logger.warning("Detected PITR to timeline #{timeline_id}")
        :timeline_changed

      {:purge, reason} ->
        Logger.warning("Detected PITR to timeline #{timeline_id}: #{reason}. Purging shapes.")
        :timeline_changed
    end
  end

  defp continuation(_electric_timeline_id, _timeline_id, nil), do: {:purge, nil}

  defp continuation(electric_timeline_id, timeline_id, %{history: history} = promotion) do
    with {:ok, entries} <- history_entries(history),
         {:ok, switchpoint} <- switchpoint(entries, electric_timeline_id, timeline_id),
         {:ok, processed_lsn} <- processed_lsn(promotion),
         :ok <- processed_before_switchpoint(processed_lsn, switchpoint, electric_timeline_id) do
      {:continued, switchpoint, processed_lsn}
    end
  end

  defp history_entries({:ok, entries}), do: {:ok, entries}

  defp history_entries({:error, reason}),
    do: {:purge, "its history is unavailable (#{inspect(reason)})"}

  defp switchpoint(entries, electric_timeline_id, timeline_id) do
    case Enum.find(entries, fn {parent, _} ->
           parent == parse_timeline_id(electric_timeline_id)
         end) do
      {_, switchpoint} ->
        {:ok, switchpoint}

      nil ->
        {:purge, "timeline #{electric_timeline_id} is not an ancestor of timeline #{timeline_id}"}
    end
  end

  defp processed_lsn(%{processed_lsn: processed_lsn}) do
    case processed_lsn.() do
      {:ok, lsn} when is_integer(lsn) and lsn >= 0 -> {:ok, lsn}
      {:error, reason} -> {:purge, reason}
    end
  end

  # A commit LSN names where its commit record starts, and the switch point is where the new
  # timeline's first record starts: a commit at the switch point is the old timeline's alone.
  defp processed_before_switchpoint(processed_lsn, switchpoint, _)
       when processed_lsn < switchpoint,
       do: :ok

  defp processed_before_switchpoint(processed_lsn, switchpoint, electric_timeline_id) do
    {:purge,
     "Electric took #{Lsn.from_integer(processed_lsn)} from timeline #{electric_timeline_id}, " <>
       "at or past the switch point #{Lsn.from_integer(switchpoint)}"}
  end

  @doc """
  Parses the content of a timeline history file: one `parentTLI<TAB>switchpoint<TAB>reason` line
  per ancestor, `#` comments and blank lines ignored, as PostgreSQL itself reads it.
  """
  @spec parse_history(String.t()) ::
          {:ok, history()} | {:error, {:malformed_history_line, String.t()}}
  def parse_history(content) when is_binary(content) do
    content
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == "" or String.starts_with?(&1, "#")))
    |> Enum.reduce_while({:ok, []}, fn line, {:ok, acc} ->
      case Regex.run(~r/^(\d+)\s+([0-9A-Fa-f]{1,8}\/[0-9A-Fa-f]{1,8})(?:\s|$)/, line) do
        [_, parent, switchpoint] ->
          entry =
            {String.to_integer(parent), switchpoint |> Lsn.from_string() |> Lsn.to_integer()}

          {:cont, {:ok, [entry | acc]}}

        nil ->
          {:halt, {:error, {:malformed_history_line, line}}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      error -> error
    end
  end

  @doc """
  The timeline the current replication stream started on. It is the connected timeline unless
  `check/3` read a promotion as a continuation of an earlier one, in which case it names the
  earliest timeline of that unbroken stream.
  """
  @spec stream_timeline_id(Keyword.t(), {pg_id(), timeline_id()}) :: timeline_id()
  def stream_timeline_id(opts, {pg_id, timeline_id}) do
    case load_record(opts) do
      {^pg_id, ^timeline_id, stream_timeline_id} when not is_nil(stream_timeline_id) ->
        stream_timeline_id

      _ ->
        timeline_id
    end
  end

  # Timeline IDs reach here as IDENTIFY_SYSTEM reports them, which is text on the replication
  # protocol, and history parent IDs as integers.
  defp parse_timeline_id(timeline_id) when is_integer(timeline_id), do: timeline_id

  defp parse_timeline_id(timeline_id) when is_binary(timeline_id) do
    case Integer.parse(timeline_id) do
      {parsed, ""} -> parsed
      _ -> nil
    end
  end

  # Loads the PG ID and timeline ID from persistent storage
  @spec load_timeline(Keyword.t()) :: stored_timeline()
  def load_timeline(opts), do: opts |> load_record() |> stored_timeline()

  @doc """
  Whether the stored record says that a purge of every shape is owed: the marker a timeline
  change or a recreated replication slot stores, until the purge ends with `store_timeline/2`.
  A record that cannot be read owes one too, as `check/3` reads it as a change. The slot's
  own record can already owe the purge before the timeline marker is written. A provisional
  slot creation by the lock holder leaves read-only serving available to a standby.
  """
  @spec purge_owed?(Keyword.t()) :: boolean()
  def purge_owed?(opts), do: purge_owed?(opts, true)

  @doc """
  Whether the stored records say that a purge of every shape is owed, as `purge_owed?/1` does,
  except that a record that cannot be read says nothing: a standby that serves its shapes while it
  waits for the lock reads the records again later, and stops serving only on a purge they owe.
  """
  @spec purge_confirmed?(Keyword.t()) :: boolean()
  def purge_confirmed?(opts), do: purge_owed?(opts, false)

  defp purge_owed?(opts, unreadable) do
    case load_timeline(opts) do
      {_pg_id, -1} -> true
      :unreadable -> unreadable
      timeline -> slot_purge_owed?(opts, timeline, unreadable)
    end
  end

  defp slot_purge_owed?(opts, timeline, unreadable) do
    kv = Keyword.fetch!(opts, :persistent_kv)
    stack_id = Keyword.fetch!(opts, :stack_id)
    unreadable_purge = if unreadable, do: :required

    case SlotIncarnation.shape_purge(kv, stack_id, unreadable_purge) do
      :required -> true
      :if_previous_timeline -> not is_nil(timeline)
      nil -> false
    end
  end

  defp stored_timeline({pg_id, timeline_id, _stream_timeline_id}), do: {pg_id, timeline_id}
  defp stored_timeline(other), do: other

  defp stream_timeline({_pg_id, timeline_id, nil}), do: timeline_id
  defp stream_timeline({_pg_id, _timeline_id, stream_timeline_id}), do: stream_timeline_id

  defp load_record(opts) do
    kv = make_serialized_kv(opts)

    case PersistentKV.get(kv, timeline_key(opts)) do
      {:ok, [pg_id, timeline_id]} ->
        {pg_id, timeline_id, nil}

      {:ok, [pg_id, timeline_id, stream_timeline_id]} ->
        {pg_id, timeline_id, stream_timeline_id}

      {:error, :not_found} ->
        nil

      error ->
        Logger.warning("Failed to load timeline ID from persistent storage: #{inspect(error)}")
        :unreadable
    end
  end

  # A write that fails is returned: the purge that stores the new timeline retries it.
  @spec store_timeline(timeline(), Keyword.t()) :: :ok | {:error, term()}
  def store_timeline({pg_id, timeline_id}, opts) do
    store_record([pg_id, timeline_id], opts)
  end

  # The filesystem backend raises when it cannot create the directory or sync it after the rename.
  defp store_record(record, opts) do
    kv = make_serialized_kv(opts)
    PersistentKV.set(kv, timeline_key(opts), record)
  rescue
    error in File.Error -> {:error, error}
  end

  @doc """
  Stores a timeline that is irrecoverable.

  We use the fact that timelines are positive to store a special timeline
  that's guaranteed to not match thus ensuring shape cleanup.
  """
  @spec store_irrecoverable_timeline(pg_id(), Keyword.t()) :: :ok
  def store_irrecoverable_timeline(pg_id, opts) do
    :ok = store_timeline({pg_id, -1}, opts)
  end

  defp make_serialized_kv(opts) do
    kv_backend = Keyword.fetch!(opts, :persistent_kv)
    # defaults to using Jason encoder and decoder
    PersistentKV.Serialized.new!(backend: kv_backend)
  end

  defp timeline_key(opts) do
    stack_id = Access.fetch!(opts, :stack_id)
    "timeline_id_#{stack_id}"
  end
end
