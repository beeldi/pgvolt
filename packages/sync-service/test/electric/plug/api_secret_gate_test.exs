defmodule Electric.Plug.ApiSecretGateTest do
  use ExUnit.Case, async: true

  import Plug.Test

  @secret "super-secret-value"

  # Driven through the real router, not `authenticate/2` directly: the defect lived in the gap
  # between gate and dispatch, which an isolated call cannot see. `api: nil` makes anything reaching
  # a handler crash, so a 401 proves the gate held and anything else proves it did not.
  defp request(method, path, opts \\ []) do
    secret = Keyword.get(opts, :secret, @secret)
    router_opts = Electric.Plug.Router.init(secret: secret, stack_id: "gate-test", api: nil)

    conn =
      Enum.reduce(Keyword.get(opts, :headers, []), conn(method, path), fn {name, value}, conn ->
        Plug.Conn.put_req_header(conn, name, value)
      end)

    try do
      conn = Electric.Plug.Router.call(conn, router_opts)
      {:completed, conn.status}
    catch
      kind, _reason -> {:reached_handler, kind}
    end
  end

  describe "the API secret gate keys off the routed path, not the raw request path" do
    test "the canonical path is gated" do
      assert {:completed, 401} = request(:get, "/v1/shape?table=users")
    end

    test "a trailing slash routes to the same handler and must be gated too" do
      # Plug splits "/v1/shape/" to ["v1", "shape"], so this dispatches to ServeShapePlug. Matching
      # on the raw request_path missed it and served shape data with no secret at all.
      assert {:completed, 401} = request(:get, "/v1/shape/?table=users")
    end

    test "a percent-encoded spelling routes to the same handler and must be gated too" do
      assert {:completed, 401} = request(:get, "/v1/%73hape?table=users")
    end

    test "an empty segment routes to the same handler and must be gated too" do
      assert {:completed, 401} = request(:get, "/v1//shape?table=users")
    end

    test "canonical DELETE is gated" do
      assert {:completed, 401} = request(:delete, "/v1/shape?table=users")
    end

    test "a trailing slash on DELETE is gated" do
      # Ungated, this reached DeleteShapePlug: unauthenticated shape destruction.
      assert {:completed, 401} = request(:delete, "/v1/shape/?table=users")
    end

    test "a correct secret is still accepted" do
      # Past the gate, so the nil api makes the handler fail -- which is the proof it got through.
      assert {:reached_handler, _} = request(:get, "/v1/shape?table=users&secret=#{@secret}")
    end

    test "a wrong secret is rejected" do
      assert {:completed, 401} = request(:get, "/v1/shape?table=users&secret=wrong")
    end

    test "health checks stay ungated" do
      assert {:completed, status} = request(:get, "/v1/health")
      assert status != 401
    end
  end

  describe "a blank secret is never a valid secret" do
    test "config validation refuses to boot on a blank secret" do
      # An unset env var expanding to "" is not nil, so this used to pass validation and the service
      # came up reporting itself secured.
      assert_raise RuntimeError, ~r/blank/, fn ->
        Electric.Config.validate_security_config!("", false)
      end

      assert_raise RuntimeError, ~r/blank/, fn ->
        Electric.Config.validate_security_config!("   ", false)
      end
    end

    test "a real secret still passes validation" do
      assert :ok = Electric.Config.validate_security_config!(@secret, false)
    end

    test "the gate rejects `?secret=` even if a blank secret reaches runtime" do
      # Defence in depth: validation stops this at boot, but the gate must not depend on that.
      assert {:completed, 401} = request(:get, "/v1/shape?table=users&secret=", secret: "")
    end

    test "insecure mode is still reachable and ungated" do
      assert :ok = Electric.Config.validate_security_config!(nil, true)
      assert {:reached_handler, _} = request(:get, "/v1/shape?table=users", secret: nil)
    end
  end

  describe "admin routes take the secret from the authorization header only" do
    @reclaim "/v1/admin/shape-generations/reclaim"

    test "a request without credentials is rejected" do
      assert {:completed, 401} = request(:post, @reclaim)
    end

    test "the secret in the query string is rejected" do
      assert {:completed, 401} = request(:post, "#{@reclaim}?secret=#{@secret}")
      assert {:completed, 401} = request(:post, "#{@reclaim}?api_secret=#{@secret}")
    end

    test "a wrong bearer secret is rejected" do
      assert {:completed, 401} =
               request(:post, @reclaim, headers: [{"authorization", "Bearer wrong"}])
    end

    test "a percent-encoded spelling of an admin route is gated too" do
      assert {:completed, 401} = request(:post, "/v1/%61dmin/shape-generations/reclaim")
    end

    test "the right bearer secret gets past the gate" do
      # No body, so the handler answers 400: anything but 401 proves the gate let it through.
      assert {:completed, 400} =
               request(:post, @reclaim, headers: [{"authorization", "Bearer #{@secret}"}])
    end

    test "admin routes do not exist in insecure mode" do
      assert {:completed, 401} =
               request(:post, @reclaim,
                 secret: nil,
                 headers: [{"authorization", "Bearer anything"}]
               )
    end
  end
end
