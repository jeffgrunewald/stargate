defmodule Stargate do
  @moduledoc """
  Documentation for Stargate.
  """

  defmodule Message do
    @moduledoc """
    TODO
    """

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
