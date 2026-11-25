defmodule Electric.ShapeCache.ShapeStatus.ShapeDb.Query do
  # A page of `list_shapes` with the oid of each shape's root table, in one row: one record a shape,
  # the sizes of its handle and shape and the oid (-1 for none) on fixed widths, then the handle and
  # the shape. See `list_encoded_shape_stream/1`.
  @encoded_shapes_page """
  SELECT CAST(group_concat(
    printf('%010d%010d%011d', length(handle), length(shape), ifnull(oid, -1)) || handle || shape,
    ''
  ) AS BLOB)
  FROM (
    SELECT s.handle AS handle, s.shape AS shape,
      (SELECT r.oid FROM relations AS r WHERE r.handle = s.handle LIMIT 1) AS oid
    FROM shapes AS s /* after */ ORDER BY s.handle LIMIT ?1
  )
  """

  @read_queries [
    list_shapes: """
    SELECT handle, shape FROM shapes ORDER BY handle
    """,
    list_shapes_page_after: """
    SELECT handle, shape FROM shapes WHERE handle > ?1 ORDER BY handle LIMIT ?2
    """,
    # The oid is that of the one relation the store files a shape under (`Shape.list_relations/1`),
    # so that the collector's restore can hand the shape on by its table without decoding it.
    list_encoded_shapes_page: @encoded_shapes_page,
    list_encoded_shapes_page_after:
      String.replace(@encoded_shapes_page, "/* after */", "WHERE s.handle > ?2"),
    # Both indexes hold every column they are asked for, so no shape is read. Going through the
    # table instead walks each row past its `shape` blob to reach the two columns behind it: the
    # whole table, read in handle order.
    list_shape_meta: """
    SELECT s.handle, s.comparable, incomplete.handle IS NULL
    FROM shapes AS s INDEXED BY shapes_comparable_cover_idx
    LEFT JOIN shapes AS incomplete INDEXED BY shapes_snapshot_idx
      ON incomplete.snapshot_complete = 0 AND incomplete.handle = s.handle
    """,
    handle_exists: """
    SELECT 1 FROM shapes WHERE handle = ?1
    """,
    snapshot_complete: """
    SELECT 1 FROM shapes WHERE handle = ?1 AND snapshot_complete = 1
    """,
    handle_lookup: """
    SELECT handle FROM shapes WHERE comparable = ?1 LIMIT 1
    """,
    shape_lookup: """
    SELECT shape FROM shapes WHERE handle = ?1 LIMIT 1
    """,
    relation_handle_lookup: """
    SELECT handle FROM relations WHERE oid = ?1 LIMIT 1
    """,
    shape_count: """
    SELECT count FROM shape_count WHERE id = 1 LIMIT 1
    """
  ]
  @write_queries Keyword.merge(
                   [
                     insert_shape: """
                     INSERT INTO shapes (handle, shape, comparable, hash) VALUES (?1, ?2, ?3, ?4)
                     """,
                     insert_relation: """
                     INSERT INTO relations (handle, oid) VALUES (?1, ?2)
                     """,
                     increment_counter: """
                     UPDATE shape_count SET count = count + ?1 WHERE id = 1
                     """,
                     delete_shape: """
                     DELETE FROM shapes WHERE handle = ?1
                     """,
                     delete_relation: """
                     DELETE FROM relations WHERE handle = ?1
                     """,
                     mark_snapshot_complete: """
                     UPDATE shapes SET snapshot_complete = 1 WHERE handle = ?1
                     """,
                     select_invalid: """
                     SELECT handle, shape, snapshot_complete FROM shapes ORDER BY handle
                     """,
                     select_incomplete: """
                     SELECT handle FROM shapes INDEXED BY shapes_snapshot_idx
                     WHERE snapshot_complete = 0 ORDER BY handle
                     """
                   ],
                   # need the handle_lookup query in the write connection to
                   # allow for handle lookup to definitely read all writes
                   # (and validation to count the catalog it has just written)
                   Keyword.take(@read_queries, [:handle_lookup, :shape_count])
                 )

  @stmt_names Enum.uniq(Keyword.keys(@read_queries) ++ Keyword.keys(@write_queries))

  defstruct @stmt_names

  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Connection, as: Conn

  # alias Exqlite.Sqlite3
  alias Electric.ShapeCache.ShapeStatus.ShapeDb.Sqlite3

  import Conn,
    only: [
      execute: 2,
      execute_all: 2,
      fetch_all: 4,
      fetch_all: 3,
      fetch_one: 3,
      modify: 3,
      stream_query: 3
    ]

  def active_stmts(nil) do
    []
  end

  def active_stmts(%__MODULE__{} = query) do
    @stmt_names
    |> Enum.map(&{&1, Map.fetch!(query, &1)})
    |> Enum.reject(&is_nil(elem(&1, 1)))
  end

  def prepare!(conn, mode) do
    case mode do
      :readwrite ->
        struct(__MODULE__, prepare_stmts!(conn, @read_queries ++ @write_queries))

      :read ->
        struct(__MODULE__, prepare_stmts!(conn, @read_queries))

      :write ->
        struct(__MODULE__, prepare_stmts!(conn, @write_queries))
    end
  end

  defp prepare_stmts!(conn, sqls) do
    Enum.map(sqls, fn {name, sql} ->
      case Sqlite3.prepare(conn, sql) do
        {:ok, stmt} ->
          {name, stmt}

        {:error, _reason} = error ->
          raise Electric.ShapeCache.ShapeStatus.ShapeDb.Error,
            action: :prepare_stmts,
            error: error
      end
    end)
  end

  def add_shape(
        %Conn{conn: conn, stmts: stmts},
        shape_handle,
        shape,
        comparable_shape,
        shape_hash,
        relations
      ) do
    %{
      insert_shape: insert_shape,
      insert_relation: insert_relation,
      increment_counter: increment_counter
    } = stmts

    with {:ok, 1} <-
           modify(conn, insert_shape, [
             {:blob, shape_handle},
             {:blob, term_to_binary(shape)},
             {:blob, comparable_to_binary(comparable_shape)},
             shape_hash
           ]),
         :ok <- increment_counter(conn, increment_counter, 1),
         :ok <-
           Enum.reduce_while(relations, :ok, fn {oid, _name}, :ok ->
             case modify(conn, insert_relation, [{:blob, shape_handle}, oid]) do
               {:ok, 1} -> {:cont, :ok}
               error -> {:halt, error}
             end
           end) do
      :ok
    end
  end

  def list_shapes(%Conn{conn: conn, stmts: %{list_shapes: stmt}}) do
    fetch_all(conn, stmt, [], fn [handle, serialized_shape] ->
      {handle, :erlang.binary_to_term(serialized_shape)}
    end)
  end

  # SQLite orders every TEXT above every BLOB, so a cursor bound as anything but a blob compares
  # greater than every handle and the first page comes back empty.
  def list_shapes_page_after(
        %Conn{conn: conn, stmts: %{list_shapes_page_after: stmt}},
        after_handle,
        limit
      ) do
    fetch_all(conn, stmt, [{:blob, after_handle}, limit], fn [handle, serialized_shape] ->
      {handle, :erlang.binary_to_term(serialized_shape)}
    end)
  end

  def handle_for_shape(%Conn{conn: conn, stmts: %{handle_lookup: stmt}}, comparable_shape) do
    with {:ok, [handle]} <-
           fetch_one(conn, stmt, [{:blob, comparable_to_binary(comparable_shape)}]) do
      {:ok, handle}
    end
  end

  def shape_for_handle(%Conn{conn: conn, stmts: %{shape_lookup: stmt}}, shape_handle) do
    with {:ok, [serialized_shape]} <- fetch_one(conn, stmt, [{:blob, shape_handle}]) do
      {:ok, :erlang.binary_to_term(serialized_shape)}
    end
  end

  # A read that fails, such as one that meets another connection's lock, answers neither way.
  def handle_exists?(%Conn{conn: conn, stmts: %{handle_exists: stmt}}, shape_handle) do
    case fetch_one(conn, stmt, [{:blob, shape_handle}]) do
      {:ok, [1]} -> true
      :error -> false
      {:error, _} = error -> error
    end
  end

  # A read that fails, such as one that meets another connection's lock, answers neither way.
  def snapshot_complete?(%Conn{conn: conn, stmts: %{snapshot_complete: stmt}}, shape_handle) do
    case fetch_one(conn, stmt, [{:blob, shape_handle}]) do
      {:ok, [1]} -> true
      :error -> false
      {:error, _} = error -> error
    end
  end

  def shape_handles_for_relations(%Conn{conn: conn}, relations) do
    {placeholders, binds} =
      Enum.map_reduce(Enum.with_index(relations, 1), %{}, fn {{oid, _relation}, idx}, binds ->
        {"@oid#{idx}", Map.put(binds, "@oid#{idx}", oid)}
      end)

    sql =
      "SELECT handle FROM relations WHERE oid IN (#{Enum.join(placeholders, ", ")}) ORDER BY handle"

    fetch_all(conn, sql, binds, fn [handle] -> handle end)
  end

  def count_shapes(%Conn{conn: conn, stmts: %{shape_count: stmt}}) do
    with {:ok, [count]} <- fetch_one(conn, stmt, []) do
      {:ok, count}
    end
  end

  def mark_snapshot_complete(
        %Conn{conn: conn, stmts: %{mark_snapshot_complete: stmt}},
        shape_handle
      ) do
    with {:ok, n} <- modify(conn, stmt, [{:blob, shape_handle}]) do
      if n == 1, do: :ok, else: :error
    end
  end

  def remove_shape(%Conn{conn: conn, stmts: stmts}, shape_handle) do
    %{
      delete_shape: delete_shape,
      delete_relation: delete_relation,
      increment_counter: increment_counter
    } = stmts

    case modify(conn, delete_shape, [{:blob, shape_handle}]) do
      {:ok, 0} ->
        {:error, {:enoshape, shape_handle}}

      {:ok, 1} ->
        with :ok <- increment_counter(conn, increment_counter, -1),
             {:ok, _} <- modify(conn, delete_relation, [{:blob, shape_handle}]) do
          :ok
        end
    end
  end

  defp increment_counter(conn, stmt, incr) do
    case modify(conn, stmt, [incr]) do
      {:ok, 1} -> :ok
      {:ok, _} -> {:error, "Failed to increment shape count by #{incr}"}
      error -> error
    end
  end

  # With full-xid positions enabled no stored shape is unsupported, which leaves the snapshot flag,
  # and `shapes_snapshot_idx` answers that without reading a shape. The general statement below
  # hands every stored shape to the caller at once.
  def select_invalid(
        %Conn{conn: conn, stmts: %{select_incomplete: select_incomplete}},
        true = _full_xid_enabled?
      ) do
    fetch_all(conn, select_incomplete, [], fn [handle] -> handle end)
  end

  def select_invalid(
        %Conn{conn: conn, stmts: %{select_invalid: select_invalid}},
        false = _full_xid_enabled?
      ) do
    with {:ok, handles} <-
           fetch_all(conn, select_invalid, [], fn [handle, serialized_shape, complete] ->
             unsupported? =
               Map.get(:erlang.binary_to_term(serialized_shape), :source_positions, :legacy) ==
                 :full_xid

             if complete == 0 or unsupported?, do: handle, else: nil
           end) do
      {:ok, Enum.reject(handles, &is_nil/1)}
    end
  end

  def list_shape_stream(%Conn{conn: conn, stmts: %{list_shapes: stmt}}) do
    stream_query(conn, stmt, fn [handle, shape] ->
      {handle, :erlang.binary_to_term(shape)}
    end)
  end

  # The boot restore's read. esqlite steps a statement a row per call into its NIF, on a dirty
  # scheduler, and back from each call the reader waits for a turn on a normal scheduler, all of
  # them busy with the restore's processes: ~17 µs a row on the bench, against ~5 µs with the
  # schedulers idle. Here SQLite steps a whole page of rows within one call.
  #
  # The pages run in one read transaction, so that the read sees the store as one statement does.
  # `group_concat` leaves the order of a page's rows to SQLite, so each page is sorted here by
  # handle, which Erlang compares as SQLite compares blobs, byte by byte, the next page starting
  # after the last of them. Each handle and shape is copied out of its page: a row waiting in the
  # restore for its batch to fill would otherwise keep the whole page alive.
  @encoded_shapes_page_rows 100

  def list_encoded_shape_stream(%Conn{conn: conn, stmts: stmts}) do
    %{list_encoded_shapes_page: first, list_encoded_shapes_page_after: next} = stmts

    Stream.resource(
      fn ->
        :ok = execute(conn, "BEGIN")
        {first, [@encoded_shapes_page_rows]}
      end,
      fn
        :done ->
          {:halt, :done}

        {stmt, binds} ->
          case fetch_one(conn, stmt, binds) do
            {:ok, [:undefined]} ->
              {:halt, :done}

            {:ok, [page]} ->
              rows = page |> decode_page([]) |> Enum.sort_by(&elem(&1, 0))
              {last, _oid, _shape} = List.last(rows)
              {rows, {next, [@encoded_shapes_page_rows, {:blob, last}]}}

            error ->
              raise Electric.ShapeCache.ShapeStatus.ShapeDb.Error, action: :read, error: error
          end
      end,
      # ends the read transaction whatever stopped the stream, a raise in the caller's reducer
      # included; there is nothing for it to undo
      fn _ -> _ = execute(conn, "ROLLBACK") end
    )
  end

  defp decode_page(<<>>, rows), do: rows

  defp decode_page(
         <<handle_size::binary-size(10), shape_size::binary-size(10), oid::binary-size(11),
           rest::binary>>,
         rows
       ) do
    {handle, rest} = :erlang.split_binary(rest, String.to_integer(handle_size))
    {shape, rest} = :erlang.split_binary(rest, String.to_integer(shape_size))
    oid = with -1 <- String.to_integer(oid), do: nil
    decode_page(rest, [{:binary.copy(handle), oid, :binary.copy(shape)} | rows])
  end

  def list_shape_meta_stream(%Conn{conn: conn, stmts: %{list_shape_meta: stmt}}) do
    stream_query(conn, stmt, fn [handle, digest, snapshot_complete] ->
      {handle, digest, snapshot_complete == 1}
    end)
  end

  def reset(%Conn{conn: conn}) do
    execute_all(conn, [
      "DELETE FROM shapes",
      "DELETE FROM relations",
      "UPDATE shape_count SET count = 0"
    ])
  end

  def explain(%Conn{} = conn, :read) do
    do_explain(conn, :read, @read_queries)
  end

  def explain(%Conn{} = conn, :write) do
    do_explain(conn, :write, @write_queries)
  end

  defp do_explain(%Conn{conn: conn, stmts: stmts}, mode, mode_queries) do
    IO.puts(
      IO.ANSI.format([
        [:bright, "\n============================\n", :reset],
        [:bright, "#{mode}\n", :reset],
        [:bright, "============================\n", :reset]
      ])
    )

    for {name, sql} <- mode_queries do
      binds =
        Stream.repeatedly(fn -> "" end)
        |> Enum.take(Sqlite3.bind_parameter_count(Map.fetch!(stmts, name)))

      with {:ok, rows} <- fetch_all(conn, "EXPLAIN QUERY PLAN " <> sql, binds) do
        plan =
          rows
          |> Enum.map(fn [_, _, _, s] -> s end)
          |> Enum.map(fn plan ->
            colour =
              cond do
                String.contains?(plan, "USING COVERING INDEX") -> :green
                String.contains?(plan, "USING INDEX") -> :cyan
                true -> :red
              end

            [:bright, colour, plan, :reset]
          end)

        IO.puts(
          IO.ANSI.format([
            [:bright, "#{name}", :reset],
            ": [#{String.trim_trailing(sql)}] ",
            plan |> Enum.intersperse(", ")
          ])
        )
      end
    end
  end

  # A shape is stored compressed. Its blob is mostly its where clause's tree, whose atoms the
  # external format writes out in full each time: on the bench's shapes, 10.5 KB as is, 1.4 KB
  # compressed. The store's file holds each blob twice, in the table and in
  # `shapes_handle_cover_idx`, which a boot reads through in its read-ahead and walks in its
  # integrity check. `binary_to_term/1` reads either form, so the rows stored before are read as
  # they are, and an image from before reads the rows stored now.
  defp term_to_binary(term), do: :erlang.term_to_binary(term, [:deterministic, compressed: 1])

  defp comparable_to_binary(comparable_shape) do
    Electric.Shapes.Shape.digest_comparable(comparable_shape)
  end
end
