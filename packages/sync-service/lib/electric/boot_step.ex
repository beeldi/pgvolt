defmodule Electric.BootStep do
  @moduledoc false

  # A restart spends minutes in steps that each read the whole shape store or the whole shape
  # storage tree, and most of them logged nothing, so a slow restart gave nothing to say which step
  # held it. Every such step reports its own duration here, so a slow boot can be read off the log.

  require Logger

  @spec measure(String.t(), (-> result)) :: result when result: var
  def measure(step, fun) when is_binary(step) and is_function(fun, 0) do
    start = System.monotonic_time()

    try do
      fun.()
    after
      duration = System.monotonic_time() - start

      Logger.notice(
        "Boot step #{step} took #{System.convert_time_unit(duration, :native, :millisecond)}ms"
      )
    end
  end
end
