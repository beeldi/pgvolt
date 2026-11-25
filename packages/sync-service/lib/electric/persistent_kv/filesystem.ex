defmodule Electric.PersistentKV.Filesystem do
  require Logger

  alias Electric.Utils

  defstruct root: "."

  @schema NimbleOptions.new!(root: [type: :string, required: true])

  def new!(opts) do
    {:ok, config} = NimbleOptions.validate(opts, @schema)
    struct(__MODULE__, config)
  end

  defimpl Electric.PersistentKV do
    def get(fs, key) do
      path = join(fs, key)

      case File.read(path) do
        # A damaged term and a plain string written before this module encoded at all both fail
        # `binary_to_term/1` with the same `ArgumentError`. The version byte tells them apart: 131
        # leads every encoded term and no UTF-8 string, 0x83 being a continuation byte. Nothing
        # here writes an empty file, so one of those is damage too.
        {:ok, <<131, _::binary>> = data} ->
          try do
            {:ok, :erlang.binary_to_term(data)}
          rescue
            ArgumentError -> {:error, :corrupted}
          end

        {:ok, ""} ->
          {:error, :corrupted}

        {:ok, data} ->
          {:ok, data}

        {:error, :enoent} ->
          {:error, :not_found}

        error ->
          error
      end
    end

    def set(fs, key, value) when is_binary(key) do
      with file_path = mkdir(fs, key),
           :ok <- atomic_write(file_path, value) do
        Logger.debug("[SET] #{file_path}")
        :ok
      end
    end

    # Rename on one filesystem is atomic: no reader ever sees a half-written file. It is not
    # durable, and it makes that promise about an in-memory directory -- without `:sync` the new
    # name can reach the disk ahead of the bytes it points at, and without the directory sync the
    # name need not reach the disk at all. Either way `set/3` returned `:ok` for a value that is
    # not there afterwards, which the read side can only report as never written.
    defp atomic_write(final_path, data) do
      dir = Path.dirname(final_path)

      tmp_file =
        Path.join(
          dir,
          "." <> (:crypto.strong_rand_bytes(10) |> Base.encode32(case: :lower, padding: false))
        )

      # A failed write or rename otherwise leaves the temp file behind as a hidden dotfile no later
      # call reaches -- on the fork's per-cache-fill writer, one orphan per failed fill on the disk
      # whose free space already ran short. The success path renamed it away.
      with :ok <- File.write(tmp_file, :erlang.term_to_binary(data), [:binary, :sync]),
           :ok <- File.rename(tmp_file, final_path) do
        Utils.sync_dir!(dir)
      else
        error ->
          _ = File.rm(tmp_file)
          error
      end
    end

    defp mkdir(fs, key) do
      path = join(fs, key)
      dir = Path.dirname(path)
      File.mkdir_p!(dir)
      path
    end

    defp join(fs, key), do: Path.join(fs.root, key) |> Path.expand()
  end
end
