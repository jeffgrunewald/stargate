defmodule Stargate.Producer.QueryParams do
  @moduledoc """
  TODO
  """

  @doc """
  TODO
  """
  @spec build_params(map() | nil) :: String.t()
  def build_params(nil), do: ""

  def build_params(config) when is_map(config) do
    routing_mode =
      case Map.get(config, :routing_mode) do
        :round_robin -> "RoundRobinPartition"
        :single -> "SinglePartition"
        _ -> ""
      end

    compression_type =
      case Map.get(config, :compression_type) do
        :lz4 -> "LZ4"
        :zlib -> "ZLIB"
        _ -> "NONE"
      end

    hashing_scheme =
      case Map.get(config, :hashing_scheme) do
        :java_string -> "JavaStringHash"
        :murmur3 -> "Murmur3_32Hash"
        _ -> ""
      end

    %{
      "sendTimeoutMillis" => Map.get(config, :send_timeout),
      "batchingEnabled" => Map.get(config, :batch_enabled),
      "batchingMaxMessages" => Map.get(config, :batch_max_msg),
      "maxPendingMessages" => Map.get(config, :max_pending_msg),
      "batchingMaxPublishDelay" => Map.get(config, :batch_max_delay),
      "messageRoutingMode" => routing_mode,
      "compressionType" => compression_type,
      "producerName" => Map.get(config, :producer_name),
      "initialSequenceId" => Map.get(config, :initial_seq_id),
      "hashingScheme" => hashing_scheme
    }
    |> Enum.map(fn {key, value} -> key <> "=" <> to_string(value) end)
    |> Enum.filter(fn param -> String.last(param) != "=" end)
    |> Enum.join("&")
  end
end
