defmodule Electric.Client.EctoAdapter.ArrayDecoderTest do
  use ExUnit.Case, async: true
  alias Electric.Client.EctoAdapter.ArrayDecoder

  test "simple string arrays" do
    assert ArrayDecoder.decode!(~s[{}], :string) == []
    assert ArrayDecoder.decode!(~s[{a,b,c}], :string) == ["a", "b", "c"]
    assert ArrayDecoder.decode!(~s[{"a","b","c"}], :string) == ["a", "b", "c"]
    assert ArrayDecoder.decode!(~s[{"a{}","b","c"}], :string) == ["a{}", "b", "c"]
    assert ArrayDecoder.decode!(~S[{"a\"","b","c"}], :string) == ["a\"", "b", "c"]

    assert ArrayDecoder.decode!(~S[{"this isn't here","b","c"}], :string) == [
             "this isn't here",
             "b",
             "c"
           ]

    assert ArrayDecoder.decode!(~S[{NULL,NULL,"NULL"}], :string) == [nil, nil, "NULL"]
  end

  test "decodes quoted elements with escaped backslashes" do
    # PostgreSQL writes a backslash inside a quoted element as `\\` and a double quote as `\"`.
    assert ArrayDecoder.decode!(~S[{"a\\b"}], :string) == ["a\\b"]
    assert ArrayDecoder.decode!(~S[{"a\\\\b"}], :string) == ["a\\\\b"]

    # elements that end in a backslash must not swallow the closing quote
    assert ArrayDecoder.decode!(~S[{"a\\"}], :string) == ["a\\"]
    assert ArrayDecoder.decode!(~S[{"a\\","b"}], :string) == ["a\\", "b"]
    assert ArrayDecoder.decode!(~S[{"\\"}], :string) == ["\\"]
    assert ArrayDecoder.decode!(~S[{"a\\\"","b"}], :string) == ["a\\\"", "b"]

    # multibyte characters, and the same inside a nested array
    assert ArrayDecoder.decode!(~S[{"é\\ü","\\\\"}], :string) == ["é\\ü", "\\\\"]
    assert ArrayDecoder.decode!(~S[{{"a\\"},{"\\b"}}], :string) == [["a\\"], ["\\b"]]
  end

  test "raises when a quoted element ends in an unpaired backslash" do
    assert_raise Ecto.CastError, fn -> ArrayDecoder.decode!(~S[{"a\"}], :string) end
    assert_raise Ecto.CastError, fn -> ArrayDecoder.decode!(~S[{"a\\\"}], :string) end
  end

  test "nested string arrays" do
    assert ArrayDecoder.decode!(~s[{{},{}}], :string) == [[], []]

    assert ArrayDecoder.decode!(~s[{{"a","b","c"},{},{c,d,e}], :string) == [
             ["a", "b", "c"],
             [],
             ["c", "d", "e"]
           ]
  end

  test "simple integer arrays" do
    assert ArrayDecoder.decode!(~s[{}], :integer) == []
    assert ArrayDecoder.decode!(~s[{1,2,3}], :integer) == [1, 2, 3]
    assert ArrayDecoder.decode!(~S[{NULL,NULL,23}], :integer) == [nil, nil, 23]
  end

  test "nested integer arrays" do
    assert ArrayDecoder.decode!(~s[{{},{}}], :integer) == [[], []]

    assert ArrayDecoder.decode!(~s[{{1,2,3},{},{4,5,6}], :integer) == [
             [1, 2, 3],
             [],
             [4, 5, 6]
           ]

    assert ArrayDecoder.decode!(~s[{{{1,{2,{7,8}},3},{},{4,5,6}}], :integer) == [
             [
               [1, [2, [7, 8]], 3],
               [],
               [4, 5, 6]
             ]
           ]
  end

  test "nested jsonb arrays" do
    assert ArrayDecoder.decode!(~s[{{{"{\\\"this\\\":4}"},"{}"},{},{"{}","{}"}], :map) == [
             [[%{"this" => 4}], %{}],
             [],
             [%{}, %{}]
           ]
  end

  test "decodes jsonb elements whose strings contain backslashes" do
    # The string C:\dir inside a jsonb value is written C:\\dir in the JSON text, and the array
    # literal then escapes the quotes and both backslashes of that text a second time.
    assert ArrayDecoder.decode!(~S[{"{\"p\": \"C:\\\\dir\"}"}], :map) == [%{"p" => "C:\\dir"}]
  end

  test "raises if the array is invalid" do
    assert_raise Ecto.CastError, fn ->
      ArrayDecoder.decode!(~s[{a,b,c"], :string)
    end

    assert_raise Ecto.CastError, fn ->
      ArrayDecoder.decode!(~s[{a,b,"], :string)
    end
  end

  describe "nesting depth" do
    test "decodes an array of the 6 dimensions PostgreSQL allows" do
      assert ArrayDecoder.decode!(~s[{{{{{{1}}}}}}], :integer) == [[[[[[1]]]]]]
    end

    test "raises an error that names the limit when nested deeper" do
      error =
        assert_raise Ecto.CastError, fn ->
          ArrayDecoder.decode!(~s[{{{{{{{1}}}}}}}], :integer)
        end

      assert Exception.message(error) =~ "deeper than the 6 dimensions"
    end

    test "refuses a very deep value without decoding it" do
      depth = 1_000_000
      value = String.duplicate("{", depth) <> String.duplicate("}", depth)

      error = assert_raise Ecto.CastError, fn -> ArrayDecoder.decode!(value, :string) end

      assert Exception.message(error) =~ "deeper than the 6 dimensions"
    end

    test "does not count braces inside quoted elements" do
      assert ArrayDecoder.decode!(~s[{"{{{{{{{{x}}}}}}}}"}], :string) == ["{{{{{{{{x}}}}}}}}"]
    end
  end
end
