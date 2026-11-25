defmodule Electric.Replication.PostgresInterop.CastingLikeTest do
  use ExUnit.Case, async: true

  import Electric.Replication.PostgresInterop.Casting, only: [like?: 2, like?: 3]

  # Every expectation below was read off PostgreSQL 18.6 (en_US.utf8, libc provider) rather than
  # from the SQL standard, so a case that goes red here is a real disagreement with the database
  # the snapshot half of a shape is served from. `!~~` is `not like?/2`, so a false negative in
  # like?/2 is a row a `NOT LIKE` filter admits when it was meant to withhold it.

  describe "like?/2 spans newlines the way Postgres does" do
    test "% and _ cross a newline" do
      assert like?("note\nsecret\nnote", "%secret%")
      assert like?("a\nb", "a_b")
      assert like?("a\nb", "___")
      assert like?("a\nb", "%")
      assert like?("a\r\nb", "a%b")
    end

    test "the pattern is anchored past a trailing newline" do
      # A `$` anchor also matches immediately before a final newline, which admitted values that
      # merely started with the pattern.
      refute like?("ab\n", "ab")
      refute like?("ab\n", "%b")
      refute like?("secret\n", "%secret")
      assert like?("ab\n", "ab_")
      assert like?("ab\n", "ab%")
    end
  end

  describe "NOT LIKE withholds the rows it is meant to" do
    test "a marker on its own line is still matched, so the negation excludes the row" do
      # This is the fail-open: `body NOT LIKE '%CONFIDENTIAL%'` delivered the row whenever the
      # marker sat on its own line, because like?/2 answered false and `!~~` inverted it to true.
      refute not like?("note\nsecret\nnote", "%secret%")
      refute not like?("public\nCONFIDENTIAL", "%CONFIDENTIAL%")
      refute not like?("CAFÉ", "%café%", true)
    end
  end

  describe "like?/2 counts characters, not bytes" do
    test "_ matches one multi-byte character" do
      assert like?("é", "_")
      refute like?("é", "__")
      assert like?("日本", "__")
      assert like?("naïve", "na_ve")
      assert like?("a😀b", "a_b")
    end
  end

  describe "like?/2 honours the backslash escape" do
    test "an escaped wildcard matches that literal character" do
      assert like?("a%b", "a\\%b")
      refute like?("axb", "a\\%b")
      assert like?("a_b", "a\\_b")
      refute like?("axb", "a\\_b")
      assert like?("100%", "%\\%")
      assert like?("%a", "\\%a")
    end

    test "an escaped backslash matches one backslash" do
      assert like?("a\\b", "a\\\\b")
      refute like?("a\\b", "a\\b")
    end

    test "the escape makes any following character literal" do
      assert like?("ab", "a\\b")
    end

    test "a pattern ending mid-escape matches nothing" do
      refute like?("a", "a\\")
      refute like?("a\\", "a\\")
    end
  end

  describe "ilike?/2 folds case beyond ASCII" do
    test "accented characters fold" do
      assert like?("CAFÉ", "%café%", true)
      assert like?("HÉLLO", "héllo", true)
      assert like?("ÀÉÎÕÜ", "àéîõü", true)
    end

    test "ASCII folding still holds" do
      assert like?("HELLO", "hello", true)
      refute like?("ab\n", "%B", true)
    end
  end

  describe "like?/2 keeps regex metacharacters literal" do
    test "a pattern character with regex meaning matches only itself" do
      assert like?("a.c", "a.c")
      refute like?("abc", "a.c")
      assert like?("a+b", "a+b")
      refute like?("aab", "a+b")
      assert like?("[a]", "[a]")
      refute like?("a", "[a]")
      assert like?("a{2}", "a{2}")
      refute like?("aa", "a{2}")
    end
  end
end
