defmodule Electric.ShapeCache.PureFileStorage.ActionFileTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.LogOffset
  alias Electric.ShapeCache.PureFileStorage.ActionFile
  alias Electric.ShapeCache.PureFileStorage.KeyIndex

  @moduletag :tmp_dir

  # One compaction entry stores how many positions it merges in 16 bits.
  @max_positions 65_535

  test "reads back a key updated more times than one compaction entry can hold",
       %{tmp_dir: tmp_dir} do
    update_count = @max_positions + 2
    key = ~s|"public"."items"/"1"|

    key_index_path = Path.join(tmp_dir, "key_index")

    File.write!(
      key_index_path,
      for n <- 1..update_count do
        item = {LogOffset.new(n, 0), byte_size(key), key, ?u, nil, 10, nil}
        {entry, _size} = KeyIndex.make_entry(item, n * 100)
        entry
      end
    )

    actions =
      key_index_path
      |> ActionFile.create_from_key_index(Path.join(tmp_dir, "actions"))
      |> ActionFile.stream()
      |> Enum.to_list()

    assert length(actions) == update_count

    merged_counts =
      for {_offset, _label, _entry_start, {:compact, positions}} <- actions,
          do: length(positions)

    assert Enum.all?(merged_counts, &(&1 <= @max_positions))
    assert Enum.sum(merged_counts) == update_count
  end
end
