defmodule Electric.Client.PollTest do
  use ExUnit.Case, async: true

  alias Electric.Client.Mock
  alias Electric.Client.Poll
  alias Electric.Client.ShapeState
  alias Electric.Client.Message.ChangeMessage
  alias Electric.Client.Message.ResumeMessage
  alias Electric.Client.Offset

  describe "ShapeState" do
    test "new/0 creates initial state" do
      state = ShapeState.new()

      assert state.offset == Offset.before_all()
      assert state.up_to_date? == false
      assert state.shape_handle == nil
      assert state.schema == nil
      assert state.value_mapper_fun == nil
      assert state.tag_to_keys == %{}
      assert state.key_data == %{}
    end

    test "new/1 accepts options" do
      state = ShapeState.new(shape_handle: "test-handle", offset: "123_0")

      assert state.shape_handle == "test-handle"
      assert state.offset == "123_0"
      assert state.up_to_date? == false
    end

    test "from_resume/1 creates state from ResumeMessage" do
      resume = %ResumeMessage{
        shape_handle: "resume-handle",
        offset: "456_1",
        schema: %{"id" => %{type: "int8"}},
        tag_to_keys: %{"tag1" => MapSet.new(["key1", "key2"])},
        key_data: %{"key1" => %{tags: MapSet.new(["tag1"]), msg: :some_msg}}
      }

      state = ShapeState.from_resume(resume)

      assert state.shape_handle == "resume-handle"
      assert state.offset == "456_1"
      assert state.schema == %{"id" => %{type: "int8"}}
      assert state.up_to_date? == true
      assert state.tag_to_keys == %{"tag1" => MapSet.new(["key1", "key2"])}
      assert state.key_data == %{"key1" => %{tags: MapSet.new(["tag1"]), msg: :some_msg}}
    end

    test "to_resume/1 converts state to ResumeMessage" do
      state = %ShapeState{
        shape_handle: "my-handle",
        offset: "789_2",
        schema: %{"name" => %{type: "text"}},
        tag_to_keys: %{"tagX" => MapSet.new(["keyA"])},
        key_data: %{"keyA" => %{tags: MapSet.new(["tagX"]), msg: :another_msg}}
      }

      resume = ShapeState.to_resume(state)

      assert %ResumeMessage{} = resume
      assert resume.shape_handle == "my-handle"
      assert resume.offset == "789_2"
      assert resume.schema == %{"name" => %{type: "text"}}
      assert resume.tag_to_keys == %{"tagX" => MapSet.new(["keyA"])}
      assert resume.key_data == %{"keyA" => %{tags: MapSet.new(["tagX"]), msg: :another_msg}}
    end

    test "round-trip through ResumeMessage preserves essential state" do
      original =
        ShapeState.new(
          shape_handle: "round-trip",
          offset: "100_5"
        )

      original = %{original | schema: %{"col" => %{type: "text"}}, up_to_date?: true}

      resumed = original |> ShapeState.to_resume() |> ShapeState.from_resume()

      assert resumed.shape_handle == original.shape_handle
      assert resumed.offset == original.offset
      assert resumed.schema == original.schema
      assert resumed.up_to_date? == true
    end

    test "new/0 initializes stale retry fields" do
      state = ShapeState.new()

      assert state.stale_cache_buster == nil
      assert state.stale_cache_retry_count == 0
    end

    test "enter_stale_retry/1 sets cache buster and increments count" do
      state = ShapeState.new()

      state = ShapeState.enter_stale_retry(state)

      assert is_binary(state.stale_cache_buster)
      assert String.length(state.stale_cache_buster) == 16
      assert state.stale_cache_retry_count == 1

      # Calling again increments count and generates new cache buster
      old_buster = state.stale_cache_buster
      state = ShapeState.enter_stale_retry(state)

      assert state.stale_cache_retry_count == 2
      assert state.stale_cache_buster != old_buster
    end

    test "clear_stale_retry/1 resets stale retry state" do
      state =
        ShapeState.new()
        |> ShapeState.enter_stale_retry()
        |> ShapeState.enter_stale_retry()

      assert state.stale_cache_retry_count == 2
      assert state.stale_cache_buster != nil

      state = ShapeState.clear_stale_retry(state)

      assert state.stale_cache_buster == nil
      assert state.stale_cache_retry_count == 0
    end

    test "generate_cache_buster/0 returns 16-character hex string" do
      buster = ShapeState.generate_cache_buster()

      assert is_binary(buster)
      assert String.length(buster) == 16
      assert String.match?(buster, ~r/^[0-9a-f]+$/)
    end

    test "generate_cache_buster/0 returns unique values" do
      busters = for _ <- 1..100, do: ShapeState.generate_cache_buster()
      unique_busters = Enum.uniq(busters)

      assert length(unique_busters) == 100
    end
  end

  describe "request/3 on a resumed stream" do
    test "derives the value mapper from the resumed schema before mapping live rows" do
      {:ok, client} = Mock.new()

      resume = %ResumeMessage{
        shape_handle: "resume-handle",
        offset: Offset.new(0, 1),
        schema: %{"id" => %{type: "int8"}, "name" => %{type: "text"}}
      }

      state = ShapeState.from_resume(resume)

      ref =
        Mock.async_response(client,
          status: 200,
          shape_handle: "resume-handle",
          last_offset: Offset.new(0, 2),
          body: Mock.transaction([%{id: "42", name: "Ada"}], operation: :insert, lsn: 2)
        )

      {:ok, messages, new_state} = Poll.request(client, state)
      _request = Mock.async_await(ref)

      assert [%ChangeMessage{value: %{"id" => 42, "name" => "Ada"}} | _] = messages
      assert is_function(new_state.value_mapper_fun, 1)
    end
  end
end
