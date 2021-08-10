defmodule Stargate do
  @moduledoc """
  Stargate provides an Elixir client for the Apache Pulsar distributed message
  log service, based on the Pulsar project's websocket API.

  ### Producer
  Create a producer process under your application's supervision tree with the following:

      options = [
          name: :pulsar_app,
          host: [{:"broker-url.com", 8080}],
          producer: [
              persistence: "non-persistent",
              tenant: "marketing",
              namespace: "public",
              topic: "new-stuff"
          ]
      ]

      Stargate.Supervisor.start_link(options)

  Once the producer is running, pass messages to the client by pid or by the named
  registry entry:

      Stargate.produce(producer, [{"key", "value"}])

  If you won't be producing frequently you can choose to run ad hoc produce commands against
  the url of the Pulsar cluster/topic as follows:

      url = "ws://broker-url.com:8080/ws/v2/producer/non-persistent/marketing/public/new-stuff"

      Stargate.produce(url, [{"key, "value"}])

  ### Consumer and Reader
  Both consumers and readers connected to Pulsar via Stargate process received messages the
  same way. Stargate takes care of receiving the messages and sending acknowledgements back
  to the cluster so all you need to do is start a process and define a module in your application
  that invokes `use Stargate.Receiver.MessageHandler` and has a `handle_message/1` or `handle_message/2`
  function as follows:

      defmodule Publicize.MessageHandler do
          use Stargate.Receiver.MessageHandler

          def handle_message(%{context: context, payload: payload}) do
              publish_to_channel(payload, context)

              :ack
          end

          defp publish_to_channel(payload, context) do
              ...do stuff...
          end
      end

  The `handle_message/1` must return either `:ack` or `:continue` in order to ack successful
  processing of the message back to the cluster or continue processing without ack (in the event
  you want to do a bulk/cumulative ack at a later time). If using the `handle_message/2` callback
  for handlers that keep state across messages handled, it must return `{:ack, state}` or
  `{:continue, state}`.

  Then, create a consumer or reader process under your application's supevision tree with the following:

      options = [
          name: :pulsar_app,
          host: [{:"broker-url.com", 8080}]
          consumer: [                        <====== replace with `:reader` for a reader client
              tenant: "internal",
              namespace: "research",
              topic: "ready-to-release",
              subscription: "rss-feed",      <====== required for a `:consumer`
              handler: Publicizer.MessageHandler
          ]
      ]

      Stargate.Supervisor.start_link(options)

  Readers and Consumers share the same configuration API with the two key differences that the
  `:consumer` key in the options differentiates from the `:reader` key, as well as the requirement
  to provide a `"subscription"` to a consumer for the cluster to manage messages.
  """

  defdelegate produce(url_or_connection, message), to: Stargate.Producer
  defdelegate produce(connection, message, mfa), to: Stargate.Producer

  @type tenant :: String.t()
  @type namespace :: String.t()
  @type topic :: String.t()
  @type persistence :: "persistent" | "non-persistent"
  @type component :: :producer | :producer_ack | :consumer | :consumer_ack | :reader | :reader_ack
  @type key_opt :: {:persistence, persistence()} | {:name, atom()} | {:component, component()}

  @spec registry_key(tenant(), namespace(), topic(), [key_opt]) ::
          {:via, Registry, {atom(), {component(), persistence(), tenant(), namespace(), topic()}}}
  def registry_key(tenant, namespace, topic, opts \\ []) do
    name = Keyword.get(opts, :name, :default)
    component = Keyword.get(opts, :component, :producer)
    persistence = Keyword.get(opts, :persistence, "persistent")

    {:via, Registry, {:"sg_reg_#{name}", {component, persistence, tenant, namespace, topic}}}
  end

  defmodule Message do
    @moduledoc """
    Defines the Elixir Struct that represents the structure of a Pulsar message.
    The struct combines the "location" data of the received messages (persistent vs. non-persistent,
    tenant, namespace, topic) with the payload, any key and/or properties provided with the message,
    and the publication timestamp as an DateTime struct, and the messageId assigned by the cluster.

    ### Example
        message = %Stargate.Message{
            topic: "ready-for-release",
            namespace: "research",
            tenant: "internal",
            persistence: "persistent",
            message_id: "CAAQAw==",
            payload: "Hello World",
            key: "1234",
            properties: nil,
            publish_time: ~U[2020-01-10 18:13:34.443264Z]
        }
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
    Create a %Stargate.Message{} struct from a list of arguments. Takes the map decoded from
    the json message payload received from Pulsar and adds the tenant, namespace, topic, persistence
    information to maintain "location awareness" of a message's source topic.

    Creating a %Stargate.Message{} via the `new/5` function automatically converts the ISO8601-formatted
    publish timestamp to a DateTime struct and decodes the message payload from the Base64 encoding
    received from the cluster.
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
