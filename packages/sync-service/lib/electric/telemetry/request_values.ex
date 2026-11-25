defmodule Electric.Telemetry.RequestValues do
  @moduledoc false

  def enabled?, do: Electric.Config.get_env(:telemetry_redact_request_values?)

  def summary(prefix, value) do
    %{"#{prefix}.present" => not is_nil(value), "#{prefix}.bytes" => bytes(value)}
  end

  def bytes(nil), do: 0
  def bytes(value) when is_binary(value), do: byte_size(value)
  def bytes(value), do: :erlang.external_size(value)

  def query_summary(query_string), do: "#{byte_size(query_string)} bytes"

  def url_header?(name),
    do:
      String.downcase(name, :ascii) in ~w(referer x-forwarded-uri x-original-uri x-original-url x-rewrite-url)

  def error_type(%module{}, _kind), do: Atom.to_string(module)
  def error_type(_reason, kind), do: Atom.to_string(kind)

  def inspect_error(reason) do
    if enabled?(), do: error_type(reason, :error), else: inspect(reason)
  end

  def format_error(kind, reason, stacktrace \\ []) do
    if enabled?(),
      do: error_type(reason, kind),
      else: Exception.format(kind, reason, stacktrace)
  end
end
