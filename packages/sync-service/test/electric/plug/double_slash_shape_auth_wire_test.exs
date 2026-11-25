defmodule Electric.Plug.DoubleSlashShapeAuthWireTest do
  @moduledoc """
  Wire-level companion to `Electric.Plug.ApiSecretGateTest`.

  `Plug.Test.conn/3` runs its target through `URI.parse/1`, which reads the
  leading `//` of `//v1/shape` as an authority: host `v1`, path `/shape`. An
  in-process conn therefore never routes `//v1/shape` to the shape handler, so
  the path-normalization probes that gate every other spelling of `/v1/shape`
  cannot reach this one. A real socket lets Bandit normalize the raw target,
  which splits `//v1/shape` back to `["v1", "shape"]` and dispatches to the
  shape handler -- the authentication case the in-process coverage was missing.

  The gate keys off the matched route (`plug_route`), so the double-slash form
  is authenticated exactly like `/v1/shape`. A regression that keyed the gate
  off the raw `request_path` instead would fall through the secret check for
  `//v1/shape` while dispatch still served it: this test goes red on that.
  """
  use ExUnit.Case, async: false

  @secret "super-secret-value"

  setup do
    router_opts = [secret: @secret, stack_id: "double-slash-shape-auth-wire-test", api: nil]

    {:ok, server_pid} =
      start_supervised(
        {Bandit,
         plug: {Electric.Plug.Router, router_opts},
         port: 0,
         ip: :loopback,
         thousand_island_options: [num_acceptors: 1]}
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server_pid)
    %{port: port}
  end

  describe "the leading-double-slash shape target is authenticated over the wire" do
    test "unauthenticated //v1/shape is rejected exactly like /v1/shape", %{port: port} do
      assert wire_status(port, "/v1/shape?table=users") == 401
      assert wire_status(port, "//v1/shape?table=users") == 401
    end

    test "authenticated //v1/shape is accepted exactly like /v1/shape", %{port: port} do
      canonical = wire_status(port, "/v1/shape?table=users&secret=#{@secret}")
      double_slash = wire_status(port, "//v1/shape?table=users&secret=#{@secret}")

      assert canonical != 401
      assert double_slash == canonical
    end
  end

  defp wire_status(port, target) do
    {:ok, socket} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :http_bin], 5_000)

    :ok =
      :gen_tcp.send(
        socket,
        "GET #{target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
      )

    {:ok, {:http_response, _version, status, _phrase}} = :gen_tcp.recv(socket, 0, 5_000)
    :gen_tcp.close(socket)
    status
  end
end
