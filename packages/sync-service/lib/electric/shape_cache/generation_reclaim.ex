defmodule Electric.ShapeCache.GenerationReclaim do
  @moduledoc """
  Removes the shapes of superseded rotation generations. A client or proxy that appends a no-op
  generation marker such as `AND '<prefix><N>' = '<prefix><N>'` to every where clause and
  increments `N` to rotate shapes leaves earlier generations in the store with no reader. Nothing
  else expires them before the store reaches its shape ceiling. Only that trailing marker selects
  a shape: the same text inside a string literal has its quotes doubled, and a token anywhere else
  in the clause does not match.

  One call reads one page of the store after a cursor and removes what it matched there, so a
  caller drives the pass page by page within its request timeout. The page read and each removal
  take their own checkout of the shape store's pool, whose 10 s timeout restarts the whole stack
  when a stack process exceeds it; the page size, the removal chunk and the pause between chunks
  keep each checkout short.
  """

  alias Electric.ShapeCache.ShapeCleaner
  alias Electric.ShapeCache.ShapeStatus.ShapeDb
  alias Electric.Shapes.Consumer.Materializer

  require Logger

  @reason "shape_generation_gc"
  @default_token_prefix "shape-rotation:db"
  @page_size 1_000
  @removal_chunk 25
  @removal_pause_ms 50

  @type request :: %{
          required(:current_generation) => pos_integer(),
          required(:newest_collectable_generation) => pos_integer(),
          required(:budget_ms) => pos_integer(),
          required(:after) => binary(),
          optional(:token_prefix) => binary()
        }

  @type result :: %{
          scanned: non_neg_integer(),
          matched: non_neg_integer(),
          removed: non_neg_integer(),
          outcome: :ok | :aborted,
          next_after: binary() | nil
        }

  @spec default_token_prefix() :: binary()
  def default_token_prefix, do: @default_token_prefix

  @spec reclaim(Electric.stack_id(), request()) ::
          {:ok, result()} | {:error, {:live_shapes, [binary()]}} | {:error, term()}
  def reclaim(
        stack_id,
        %{
          current_generation: current,
          newest_collectable_generation: newest_collectable,
          budget_ms: budget_ms,
          after: after_handle
        } = request
      )
      when is_integer(current) and is_integer(newest_collectable) and newest_collectable >= 1 and
             newest_collectable < current and is_integer(budget_ms) and budget_ms > 0 and
             is_binary(after_handle) do
    prefix = Map.get(request, :token_prefix, @default_token_prefix)
    suffix_regex = rotation_suffix_regex(prefix)
    current_token = "#{prefix}#{current}"
    deadline = System.monotonic_time(:millisecond) + budget_ms

    with {:ok, shapes, last_handle} <-
           ShapeDb.list_shapes_page_after(stack_id, after_handle, @page_size) do
      matched =
        Enum.filter(shapes, fn {_handle, shape} ->
          case generation_of(shape, suffix_regex) do
            nil -> false
            generation -> generation <= newest_collectable
          end
        end)

      case for(
             {handle, shape} <- matched,
             in_use?(stack_id, handle, shape, current_token),
             do: handle
           ) do
        [] ->
          handles = Enum.map(matched, &elem(&1, 0))
          {removed, outcome} = remove_paced(stack_id, handles, deadline)

          {:ok,
           %{
             scanned: length(shapes),
             matched: length(handles),
             removed: removed,
             outcome: outcome,
             next_after: next_after(handles, removed, outcome, after_handle, last_handle)
           }}

        live ->
          {:error, {:live_shapes, Enum.take(live, 5)}}
      end
    end
  end

  # Handles come back in handle order, so a pass that stops early has dealt with every handle up to
  # the last one it removed, and the next call resumes after it. An aborted pass hands back no
  # cursor: the same page would fail the same way, and a caller following cursors would loop.
  defp next_after(_handles, _removed, :aborted, _after_handle, _last_handle), do: nil

  defp next_after(handles, removed, :ok, _after_handle, last_handle)
       when removed == length(handles),
       do: last_handle

  defp next_after(handles, removed, :ok, _after_handle, _last_handle) when removed > 0,
    do: Enum.at(handles, removed - 1)

  defp next_after(_handles, 0, :ok, after_handle, _last_handle), do: after_handle

  # The deadline is read between chunks and never before the first, so every call that matched
  # something makes progress however small its budget. Each handle is its own removal: a batch
  # that exits partway leaves the handles it already took out of the store without the
  # publication cleanup and client rotation that run only once the whole batch returns.
  defp remove_paced(stack_id, handles, deadline) do
    handles
    |> Enum.chunk_every(@removal_chunk)
    |> Enum.reduce_while({0, :ok}, fn chunk, {removed, :ok} ->
      if removed > 0 and System.monotonic_time(:millisecond) >= deadline do
        {:halt, {removed, :ok}}
      else
        case remove_chunk(stack_id, chunk, removed) do
          {:cont, _} = continue ->
            Process.sleep(@removal_pause_ms)
            continue

          halt ->
            halt
        end
      end
    end)
  end

  defp remove_chunk(stack_id, chunk, removed) do
    Enum.reduce_while(chunk, {:cont, {removed, :ok}}, fn handle, {:cont, {count, :ok}} ->
      case remove_one(stack_id, handle) do
        :ok -> {:cont, {:cont, {count + 1, :ok}}}
        {:error, reason} -> {:halt, abort(count, reason)}
      end
    end)
  end

  defp remove_one(stack_id, handle) do
    ShapeCleaner.remove_shapes(stack_id, [handle], @reason)
  rescue
    error -> {:error, Exception.message(error)}
  catch
    # A pool checkout timeout arrives as an exit, which `rescue` alone would miss.
    :exit, reason -> {:error, reason}
  end

  defp abort(removed, reason) do
    Logger.warning("Shape generation reclaim stopped after #{removed} shapes: #{inspect(reason)}")
    {:halt, {removed, :aborted}}
  end

  # Screened by different means than the pattern that selected it, so one bug cannot defeat both:
  # a comparison of the current generation token anywhere in the clause, found by reading its
  # string literals, or a running materializer, which only a dependency some other shape still
  # reads has. Removing a dependency stops every shape that reads it.
  #
  # The comparison is matched as two literals around `=`, never as raw text: filter values reach
  # the clause as literals, so a value can hold the token, or be the token, but cannot add the
  # comparison. A plain substring check would let one such shape return `live_shapes` on every
  # pass, blocking reclaim of the whole page.
  defp in_use?(stack_id, handle, shape, current_token) do
    compares_token?(where_query(shape), current_token) or
      Materializer.whereis(stack_id, handle) != nil
  end

  # An unterminated literal is treated conservatively: keep that shape.
  defp compares_token?(query, token) do
    case sql_pieces(query, "", []) do
      :unterminated -> true
      pieces -> token_comparison?(pieces, token)
    end
  end

  defp token_comparison?(
         [{:literal, token}, {:text, between}, {:literal, token} | _] = pieces,
         token
       ),
       do: String.trim(between) == "=" or token_comparison?(tl(pieces), token)

  defp token_comparison?([_piece | rest], token), do: token_comparison?(rest, token)
  defp token_comparison?([], _token), do: false

  # Splits the clause into the text between standard-conforming SQL string literals and the
  # literals' values: inside a single-quoted literal, a doubled quote is one quote and a single
  # quote ends it.
  defp sql_pieces(<<>>, text, acc), do: Enum.reverse([{:text, text} | acc])

  defp sql_pieces(<<?', rest::binary>>, text, acc),
    do: sql_literal(rest, "", [{:text, text} | acc])

  defp sql_pieces(<<c, rest::binary>>, text, acc), do: sql_pieces(rest, <<text::binary, c>>, acc)

  defp sql_literal(<<?', ?', rest::binary>>, value, acc),
    do: sql_literal(rest, <<value::binary, ?'>>, acc)

  defp sql_literal(<<?', rest::binary>>, value, acc),
    do: sql_pieces(rest, "", [{:literal, value} | acc])

  defp sql_literal(<<c, rest::binary>>, value, acc),
    do: sql_literal(rest, <<value::binary, c>>, acc)

  defp sql_literal(<<>>, _value, _acc), do: :unterminated

  defp rotation_suffix_regex(prefix) do
    escaped = Regex.escape(prefix)
    Regex.compile!("\\sAND '#{escaped}(\\d+)' = '#{escaped}\\1'\\z")
  end

  defp generation_of(shape, suffix_regex) do
    case Regex.run(suffix_regex, where_query(shape)) do
      [_suffix, digits] -> String.to_integer(digits)
      nil -> nil
    end
  end

  # Never `inspect/1` the where clause: it truncates at 4096 characters, a long clause runs past
  # that, and the rotation token sits at its end.
  defp where_query(%{where: %{query: query}}) when is_binary(query), do: query
  defp where_query(_shape), do: ""
end
