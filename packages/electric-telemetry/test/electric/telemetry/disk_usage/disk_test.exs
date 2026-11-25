defmodule ElectricTelemetry.DiskUsage.DiskTest do
  use ExUnit.Case, async: true

  alias ElectricTelemetry.DiskUsage.Disk

  @moduletag :tmp_dir

  # The order a directory lists its entries in decides whether a walk of the whole tree loses
  # anything, so these call the walker on the entry itself with a total already counted.
  describe "do_recursive_usage/3" do
    test "keeps the total past an entry that is gone by the time it is read", %{tmp_dir: dir} do
      dangling = Path.join(dir, "dangling")
      File.ln_s!(Path.join(dir, "deleted"), dangling)

      assert Disk.do_recursive_usage(dangling, MapSet.new(), 500) == 500
    end

    test "keeps the total past an entry that is neither a file nor a directory",
         %{tmp_dir: dir} do
      fifo = Path.join(dir, "fifo")
      {_, 0} = System.cmd("mkfifo", [fifo])

      assert Disk.do_recursive_usage(fifo, MapSet.new(), 500) == 500
    end
  end
end
