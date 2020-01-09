defmodule Stargate.Reader.QueryParams do
  @moduledoc """
  TODO
  """

  @doc """
  TODO
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
