defmodule Stargate.Consumer.QueryParams do
  @moduledoc """
  This module provides the function to generate query parameters
  for establishing a consumer connection to a topic and subscription
  with Pulsar.
  """

  @doc """
  Generates a query parameter string to append to the URL and path
  parameters when creating a Stargate.Receiver consumer connection.

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
