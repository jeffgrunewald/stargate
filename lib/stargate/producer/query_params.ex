defmodule Stargate.Producer.QueryParams do
  @moduledoc """
  This modules provides the function to generate query parameters
  for establishing a producer connection to a topic with Pulsar.
  """

  @doc """
  Generates a query parameter string to apped to the URL and path
  parameters when creating a Stargate.Producer connection.

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
    routing_mode = get_param(config, :routing_mode)
    compression_type = get_param(config, :compression_type)
    hashing_scheme = get_param(config, :hashing_scheme)

    %{
      "sendTimeoutMillis" => Map.get(config, :send_timeout),
      "batchingEnabled" => Map.get(config, :batch_enabled),
      "batchingMaxMessages" => Map.get(config, :batch_max_msg),
      "maxPendingMessages" => Map.get(config, :max_pending_msg),
      "batchingMaxPublishDelay" => Map.get(config, :batch_max_delay),
      "messageRoutingMode" => routing_mode,
      "compressionType" => compression_type,
      "producerName" => Map.get(config, :name),
      "initialSequenceId" => Map.get(config, :initial_seq_id),
      "hashingScheme" => hashing_scheme
    }
    |> Enum.map(fn {key, value} -> key <> "=" <> to_string(value) end)
    |> Enum.filter(fn param -> String.last(param) != "=" end)
    |> Enum.join("&")
  end

  defp get_param(config, :routing_mode) do
    case Map.get(config, :routing_mode) do
      :round_robin -> "RoundRobinPartition"
      :single -> "SinglePartition"
      _ -> ""
    end
  end

  defp get_param(config, :compression_type) do
    case Map.get(config, :compression_type) do
      :lz4 -> "LZ4"
      :zlib -> "ZLIB"
      :none -> "NONE"
      _ -> ""
    end
  end

  defp get_param(config, :hashing_scheme) do
    case Map.get(config, :hashing_scheme) do
      :java_string -> "JavaStringHash"
      :murmur3 -> "Murmur3_32Hash"
      _ -> ""
    end
  end
end
