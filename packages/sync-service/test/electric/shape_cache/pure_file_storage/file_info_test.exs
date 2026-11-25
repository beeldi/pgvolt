defmodule Electric.ShapeCache.PureFileStorage.FileInfoTest do
  use ExUnit.Case, async: true
  alias Electric.ShapeCache.PureFileStorage.FileInfo

  @moduletag :tmp_dir

  describe "truncate/2" do
    test "cuts the file at the size", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "file")
      File.write!(path, "0123456789")

      assert :ok = FileInfo.truncate(path, 4)
      assert File.read!(path) == "0123"
    end

    test "leaves the file whole when it cannot seek to the size", %{tmp_dir: tmp_dir} do
      path = Path.join(tmp_dir, "file")
      File.write!(path, "0123456789")

      assert {:error, :einval} = FileInfo.truncate(path, -1)
      assert File.read!(path) == "0123456789"
    end
  end

  describe "recursive_disk_usage/2" do
    test "sums every regular file under the path", %{tmp_dir: tmp_dir} do
      # Arrange
      File.mkdir_p!(Path.join(tmp_dir, "shape/chunks"))
      File.write!(Path.join(tmp_dir, "shape/log"), String.duplicate("a", 100))
      File.write!(Path.join(tmp_dir, "shape/chunks/0"), String.duplicate("b", 250))

      # Act / Assert
      assert FileInfo.recursive_disk_usage(tmp_dir) == 350
    end

    # The walk threads its running total through a reduce, so a branch that returns 0 instead of the
    # accumulator throws away the size of every sibling already counted. This directory is under
    # constant churn from compaction and the async deleter, so an entry disappearing between `ls` and
    # `stat` is routine — and the metric this feeds can then only be wrong in the direction that keeps
    # a filling volume quiet.
    test "keeps the running total when a path cannot be accounted for", %{tmp_dir: tmp_dir} do
      # Arrange — a symlink whose target is gone stats exactly like a file deleted mid-scan.
      vanished = Path.join(tmp_dir, "vanished")
      File.ln_s!(Path.join(tmp_dir, "never_existed"), vanished)

      # Act / Assert
      assert FileInfo.recursive_disk_usage(vanished, 4_096) == 4_096
      assert FileInfo.recursive_disk_usage(Path.join(tmp_dir, "no_such_dir"), 4_096) == 4_096
    end

    test "keeps the running total for an entry that is neither a file nor a directory", %{
      tmp_dir: tmp_dir
    } do
      # Arrange
      fifo = Path.join(tmp_dir, "fifo")
      {_, 0} = System.cmd("mkfifo", [fifo])

      # Act / Assert
      assert FileInfo.recursive_disk_usage(fifo, 4_096) == 4_096
    end
  end
end
