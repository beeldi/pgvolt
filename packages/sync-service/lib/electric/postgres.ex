defmodule Electric.Postgres do
  @doc """
  All types currently supported by Electric

  ## Tests

      iex> :bool in supported_types()
      true
  """
  def supported_types do
    ~w[
      bool
      bytea
      date
      float4 float8
      int2 int4 int8
      jsonb
      text
      time
      timestamp timestamptz
      uuid
      varchar
    ]a
  end

  @doc """
  All types currently supported by Electric only in functions

  ## Tests

      iex> :interval in supported_types_only_in_functions()
      true
  """
  def supported_types_only_in_functions, do: ~w|interval|a

  @display_setting_values [
    {"bytea_output", "hex"},
    {"DateStyle", "ISO, DMY"},
    {"TimeZone", "UTC"},
    {"extra_float_digits", "1"},
    {"IntervalStyle", "iso_8601"},
    {"standard_conforming_strings", "on"}
  ]

  @display_settings Enum.map(@display_setting_values, fn {name, value} ->
                      "SET #{name} = '#{value}'"
                    end)

  @display_settings_select_list Enum.map_join(@display_setting_values, ", ", fn {name, value} ->
                                  "set_config('#{name}', '#{value}', false)"
                                end)

  @doc """
  Configuration settings that affect formatting of values of certain types.

  These settings should be set for the current session before executing any queries or
  statements to safe-guard against non-standard configuration being used in the Postgres
  database cluster or even the specific database Electric is configured to connect to.

  The settings Electric is sensitive to are:

    * `bytea_output`       - determines how Postgres encodes bytea values. It can use either Hex- or
                             Escape-based encoding.

    * `DateStyle`          - determines how Postgres interprets date values.

    * `TimeZone`           - affects the time zone offset Postgres uses for timestamptz and timetz values.

    * `extra_float_digits` - determines whether floating-point values are rounded or are encoded precisely.

    * `IntervalStyle`      - determines how Postgres interprets and formats interval values.

    * `standard_conforming_strings` - determines whether a backslash inside a plain string literal
                             is an escape character. A shape's own where clause is embedded in the
                             queries Electric generates, and it was parsed as a standard string, so
                             a session that reads backslashes as escapes gives it a second meaning.
  """
  @spec display_settings :: [String.t()]
  def display_settings, do: @display_settings

  @doc """
  The same settings as `display_settings/0`, as `set_config(name, value, false)` calls for the
  select list of a query that runs anyway. They take effect for the session like `SET` does,
  without a round trip each.
  """
  @spec display_settings_select_list :: String.t()
  def display_settings_select_list, do: @display_settings_select_list
end
