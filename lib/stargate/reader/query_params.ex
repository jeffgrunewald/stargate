defmodule Stargate.Reader.QueryParams do
  @moduledoc """
  This module provides the function to generate query parameters
  for establishing a reader connection to a topic with Pulsar.
  """

  @doc """
  Generates a query parameter string to apped to the URL and path
  parameters when creating a Stargate.Receiver reader connection.

  Stargate does not generate explicit query parameters for default
  values when not supplied by the calling application as Pulsar itself
  assumes default values when not supplied.

  Query parameters with nil values are removed from the resulting
  connection string so only those with explicit values will be
  passed to Pulsar when creating a connection.
  """
  @spec build_params(map() | nil) :: String.t()
  def build_params(nil), do: ""

  def build_params(config) when is_map(config) do
    starting_message =
      case Map.get(config, :starting_message) do
        :earliest -> "earliest"
        :latest -> "latest"
        id when is_binary(id) -> id
        _ -> ""
      end

    %{
      "readerName" => Map.get(config, :name),
      "receiverQueueSize" => Map.get(config, :queue_size),
      "messageId" => starting_message
    }
    |> Enum.map(fn {key, value} -> key <> "=" <> to_string(value) end)
    |> Enum.filter(fn param ->
      String.last(param) != "=" or String.match?(param, ~r/^messageId=[[:alnum:]]+/)
    end)
    |> Enum.join("&")
  end
end
