defmodule ElectricTelemetry.DiskUsage.Disk do
  require Record

  Record.defrecord(:file_info, Record.extract(:file_info, from_lib: "kernel/include/file.hrl"))

  def recursive_usage(path, exclude) do
    do_recursive_usage(path, MapSet.new(exclude), 0)
  end

  # An entry that cannot be read adds nothing and keeps the total counted so far: the storage
  # directory loses files while it is walked, and answering 0 dropped everything walked before it.
  def do_recursive_usage(path, exclude, acc) do
    case stat(path) do
      {:ok, file_info(size: size, type: :regular)} ->
        if MapSet.member?(exclude, path) do
          acc
        else
          size + acc
        end

      {:ok, file_info(type: :directory)} ->
        case ls(path) do
          {:ok, files} ->
            Enum.reduce(files, acc, &do_recursive_usage(Path.join(path, &1), exclude, &2))

          {:error, _} ->
            acc
        end

      {:ok, _} ->
        acc

      {:error, _} ->
        acc
    end
  end

  defdelegate stat(path), to: :prim_file, as: :read_file_info

  defp ls(path) do
    :prim_file.list_dir(path)
  end
end
