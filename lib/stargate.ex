defmodule Stargate do
  @moduledoc """
  Documentation for Stargate.
  """

  defdelegate produce(url_or_connection, message), to: Stargate.Producer
  defdelegate produce(connection, message, mfa), to: Stargate.Producer

  defmodule Message do
    @moduledoc """
    TODO
    """

    @type t :: %__MODULE__{
            topic: String.t(),
            namespace: String.t(),
            tenant: String.t(),
            persistence: String.t(),
            message_id: String.t(),
            payload: String.t(),
            key: String.t(),
            properties: map(),
            publish_time: DateTime.t()
          }

    defstruct [
      :topic,
      :namespace,
      :tenant,
      :persistence,
      :message_id,
      :payload,
      :key,
      :properties,
      :publish_time
    ]

    @doc """
    TODO
    """
    @spec new(map(), String.t(), String.t(), String.t(), String.t()) :: Stargate.Message.t()
    def new(message, persistence, tenant, namespace, topic) do
      {:ok, timestamp, _} =
        message
        |> Map.get("publishTime")
        |> DateTime.from_iso8601()

      payload =
        message
        |> Map.get("payload")
        |> Base.decode64!()

      %Message{
        topic: topic,
        namespace: namespace,
        tenant: tenant,
        persistence: persistence,
        message_id: message["messageId"],
        payload: payload,
        key: Map.get(message, "key", ""),
        properties: Map.get(message, "properties", %{}),
        publish_time: timestamp
      }
    end
  end
end
