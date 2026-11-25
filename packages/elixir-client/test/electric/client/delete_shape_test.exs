defmodule Electric.Client.DeleteShapeTest do
  use ExUnit.Case, async: true

  alias Electric.Client
  alias Electric.Client.ShapeDefinition
  alias Electric.Client.Fetch

  defmodule CapturingPool do
    def request(_client, %Fetch.Request{} = request, opts) do
      send(Keyword.fetch!(opts, :reply_to), {:request, request})
      %Fetch.Response{status: 202}
    end
  end

  test "delete_shape carries the shape definition so an unbound client is not rejected" do
    client =
      Client.new!(
        base_url: "http://localhost:3000",
        pool: {CapturingPool, reply_to: self()}
      )

    shape = ShapeDefinition.new!("delete_me")

    Client.delete_shape(client, shape)

    assert_receive {:request, request}
    assert request.method == :delete
    assert request.params["table"] == "delete_me"
  end
end
