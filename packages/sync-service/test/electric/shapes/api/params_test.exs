defmodule Electric.Shapes.Api.ParamsTest do
  use ExUnit.Case, async: true

  alias Electric.Shapes.Api.Params.JsonOrMapStringParams

  describe "JsonOrMapStringParams.cast/1" do
    test "stringifies scalar values from a JSON object" do
      assert {:ok, %{"1" => "value", "2" => "3", "3" => "true", "4" => ""}} =
               JsonOrMapStringParams.cast(~s|{"1": "value", "2": 3, "3": true, "4": null}|)
    end

    test "stringifies scalar values from a map" do
      assert {:ok, %{"1" => "value", "2" => "3"}} =
               JsonOrMapStringParams.cast(%{"1" => "value", "2" => 3})
    end

    test "rejects a nested object or array instead of raising on it" do
      # `to_string/1` raises on a map rather than answering with something a changeset can carry,
      # so casting one used to come back as a crash rather than as a rejected request.
      assert {:error, message: message} = JsonOrMapStringParams.cast(~s|{"1": {"a": "b"}}|)
      assert message =~ "must be strings, numbers, booleans or null"

      assert {:error, message: _} = JsonOrMapStringParams.cast(%{"1" => %{"a" => "b"}})
      assert {:error, message: _} = JsonOrMapStringParams.cast(~s|{"1": ["a", "b"]}|)
    end

    test "rejects JSON that is not an object" do
      assert {:error, message: "params must be a JSON object"} =
               JsonOrMapStringParams.cast(~s|["a"]|)

      assert {:error, message: "params must be valid JSON"} = JsonOrMapStringParams.cast("{")
    end
  end
end
