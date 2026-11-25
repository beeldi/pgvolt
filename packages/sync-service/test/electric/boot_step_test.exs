defmodule Electric.BootStepTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Electric.BootStep

  test "returns the step's result and logs how long the step took" do
    log =
      capture_log(fn ->
        assert BootStep.measure("store.some_step", fn -> {:ok, 3} end) == {:ok, 3}
      end)

    assert log =~ ~r/Boot step store\.some_step took \d+ms/
  end

  test "logs the duration of a step that raises" do
    log =
      capture_log(fn ->
        assert_raise RuntimeError, "boom", fn ->
          BootStep.measure("store.some_step", fn -> raise "boom" end)
        end
      end)

    assert log =~ ~r/Boot step store\.some_step took \d+ms/
  end
end
