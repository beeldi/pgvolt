defmodule Electric.Replication.Eval.EnvTest do
  use ExUnit.Case, async: true

  alias Electric.Replication.Eval.Env

  describe "parse_const/3" do
    setup do
      env = Env.new()
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      {:ok, uuid_bytes} = Ecto.UUID.dump(uuid)
      %{env: env, uuid: uuid, uuid_bytes: uuid_bytes}
    end

    # `PgInterop.Array.parse/2` signals a malformed literal by raising, and every caller of
    # `parse_const/3` treats a return of `:error` as "this predicate cannot be evaluated". The
    # rescue between the two is what keeps a malformed literal a refused clause instead of a
    # crashed router, and nothing exercised it.
    for literal <- [~S|{a,b|, ~S|{a,,b}|, ~S|{a"b}|, ~S|{{1},{2,3}}|, ~S|NULL|] do
      test "refuses the malformed array literal #{inspect(literal)}", %{env: env} do
        # Assert
        assert :error = Env.parse_const(env, unquote(literal), {:array, :text})
      end
    end

    test "parses uuid values as compact 16-byte binaries", %{
      env: env,
      uuid: uuid,
      uuid_bytes: uuid_bytes
    } do
      assert {:ok, ^uuid_bytes} = Env.parse_const(env, uuid, :uuid)
      assert byte_size(uuid_bytes) == 16
    end

    test "rejects invalid 16-byte uuid text", %{env: env} do
      assert :error = Env.parse_const(env, "abcdefghijklmnop", :uuid)
    end

    test "parses uuid arrays as compact 16-byte binaries", %{
      env: env,
      uuid: uuid,
      uuid_bytes: uuid_bytes
    } do
      assert {:ok, [^uuid_bytes]} = Env.parse_const(env, "{#{uuid}}", {:array, :uuid})
    end

    test "rejects invalid 16-byte uuid text inside arrays", %{env: env} do
      assert :error = Env.parse_const(env, "{abcdefghijklmnop}", {:array, :uuid})
    end
  end

  describe "uncastable_refs/2" do
    setup do: %{env: Env.new()}

    test "names a column whose type has no text input function", %{env: env} do
      # Act
      uncastable = Env.uncastable_refs(%{["location"] => :point, ["host"] => :inet}, env)

      # Assert
      assert Enum.sort(uncastable) == [{["host"], :inet}, {["location"], :point}]
    end

    test "judges an enum by whether this environment admits one", %{env: env} do
      # Assert: the default environment cannot parse an enum, and consumer-side evaluation runs on
      # exactly that environment, so a filter over one drops every live change it is asked about.
      assert Env.uncastable_refs(%{["status"] => {:enum, "job_status"}}, env) ==
               [{["status"], {:enum, "job_status"}}]

      assert Env.uncastable_refs(
               %{["status"] => {:enum, "job_status"}},
               %{env | allow_enums: true}
             ) == []
    end

    test "passes the types a firm filter is built from", %{env: env} do
      # Assert
      assert Env.uncastable_refs(
               %{
                 ["firmExternalKey"] => :varchar,
                 ["deletedAt"] => :timestamp,
                 ["id"] => :text,
                 ["payload"] => :jsonb
               },
               env
             ) == []
    end

    test "ignores refs that are not columns of the filtered row", %{env: env} do
      # Assert: longer paths belong to subquery dependencies, which are resolved elsewhere.
      assert Env.uncastable_refs(%{["sq0", "location"] => :point}, env) == []
    end

    test "agrees with parse_const about every type it judges", %{env: env} do
      # Arrange
      samples = [
        {:text, "x"},
        {:int4, "1"},
        {:bool, "true"},
        {:uuid, "550e8400-e29b-41d4-a716-446655440000"},
        {:jsonb, ~s|{"a":1}|},
        {:point, "(1,2)"},
        {:inet, "127.0.0.1"},
        {:oid, "16384"}
      ]

      # Assert: the two share their type lookup precisely so they can never disagree about which
      # types this environment can cast.
      Enum.each(samples, fn {type, sample} ->
        reported? = Env.uncastable_refs([{["c"], type}], env) != []
        parses? = match?({:ok, _}, Env.parse_const(env, sample, type))

        assert parses? == not reported?, "disagreed on #{inspect(type)}"
      end)
    end
  end
end
