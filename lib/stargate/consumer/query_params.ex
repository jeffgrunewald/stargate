defmodule Stargate.Consumer.QueryParams do
  @moduledoc """
  TODO
  """

  @doc """
  TODO
  """
  @spec build_params(map() | nil) :: String.t()
  def build_params(nil), do: ""

  def build_params(config) when is_map(config) do
    subscription_type =
      case Map.get(config, :subscription_type) do
        :exclusive -> "Exclusive"
        :failover -> "Failover"
        :shared -> "Shared"
        _ -> ""
      end

    %{
      "ackTimeoutMillis" => Map.get(config, :ack_timeout),
      "subscriptionType" => subscription_type,
      "receiverQueueSize" => Map.get(config, :queue_size),
      "consumerName" => Map.get(config, :name),
      "priorityLevel" => Map.get(config, :priority),
      "maxRedeliverCount" => Map.get(config, :max_redeliver_count),
      "deadLetterTopic" => Map.get(config, :dead_letter_topic),
      "pullMode" => Map.get(config, :pull_mode)
    }
    |> Enum.map(fn {key, value} -> key <> "=" <> to_string(value) end)
    |> Enum.filter(fn param -> String.last(param) != "=" end)
    |> Enum.join("&")
  end
end
