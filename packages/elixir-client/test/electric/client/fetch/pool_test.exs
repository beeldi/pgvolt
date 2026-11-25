defmodule Electric.Client.Fetch.PoolTest do
  use ExUnit.Case, async: true

  alias Electric.Client
  alias Electric.Client.Authenticator.MockAuthenticator
  alias Electric.Client.Fetch.Pool

  defp client!(opts \\ []) do
    Client.new!(Keyword.merge([base_url: "https://cloud.electric.com"], opts))
  end

  describe "request_id/2" do
    test "differs when only the authenticator differs" do
      client_a = client!(authenticator: {MockAuthenticator, salt: "tenant-a"})
      client_b = client!(authenticator: {MockAuthenticator, salt: "tenant-b"})
      request_a = Client.request(client_a, [])
      request_b = Client.request(client_b, [])

      refute Pool.request_id(client_a, request_a) == Pool.request_id(client_b, request_b)
    end

    test "differs when only the fetch options differ" do
      client_a = client!(fetch: {Client.Fetch.HTTP, request: [headers: [{"x-tenant", "a"}]]})
      client_b = client!(fetch: {Client.Fetch.HTTP, request: [headers: [{"x-tenant", "b"}]]})
      request_a = Client.request(client_a, [])
      request_b = Client.request(client_b, [])

      refute Pool.request_id(client_a, request_a) == Pool.request_id(client_b, request_b)
    end

    test "differs when only the HTTP method differs" do
      client = client!()
      get = Client.request(client, method: :get)
      delete = Client.request(client, method: :delete)

      refute Pool.request_id(client, get) == Pool.request_id(client, delete)
    end

    test "coalesces two truly identical requests" do
      client = client!(authenticator: {MockAuthenticator, salt: "tenant-a"})
      request = Client.request(client, method: :get)

      assert Pool.request_id(client, request) == Pool.request_id(client, request)
    end
  end
end
