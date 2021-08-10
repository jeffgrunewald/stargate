defmodule Stargate.Receiver do
  @moduledoc """
  Provides a Stargate websocket process that can be either a
  reader or consumer connection based on the configuration passed
  when starting the process.
  """
  require Logger
  use Stargate.Connection
  import Stargate.Supervisor, only: [via: 2]
  alias Stargate.{Consumer, Reader}
  alias Stargate.Receiver.Dispatcher

  @typedoc "A string identifier assigned to each message by the cluster"
  @type message_id :: String.t()

  @doc """
  Sends an acknowledgement of the given message ID back to the Pulsar
  cluster via the provided websocket process connection. This is required
  for all Stargate consumers and readers where acknowledgement signals the
  cluster to delete messages from the topic/subscription and send more while
  readers require acknowledgement to signal readiness for more messages.
  """
  @spec ack(GenServer.server(), message_id()) :: :ok | {:error, term()}
  def ack(receiver, message_id) do
    ack = construct_response(message_id)

    WebSockex.send_frame(receiver, {:text, ack})
  end

  @doc """
  Sends a permit request to the Pulsar cluster via the provided websocket process
  connection. Used for consumers in pull mode to release up to the requested number
  of messages to be returned when available.
  """
  @spec pull_permit(GenServer.server(), non_neg_integer()) :: :ok | {:error, term()}
  def pull_permit(receiver, count) do
    permit = construct_permit(count)

    WebSockex.send_frame(receiver, {:text, permit})
  end

  defmodule State do
    @moduledoc """
    Defines the state stored by the consumer or reader websocket process. The
    Stargate receiver records the registry name associated to its supervision tree,
    the URL of the cluster and topic it connects to, as well as the individual
    components that make up the URL including the host, protocol (ws or wss), topic
    path parameters (persistent or non-persistent, tenant, namespace, and topic)
    and any query parameters configuing the connection.
    """
    defstruct [
      :registry,
      :url,
      :host,
      :protocol,
      :persistence,
      :tenant,
      :namespace,
      :topic,
      :query_params
    ]
  end

  @doc """
  Start a consumer or reader websocket process and link it to the current process.

  Consumer and Receiver options require, at minimum:

      * `host` is a tuple of the address or URL of the Pulsar cluster (broker service)
        and the port on which the service is exposed.
      * `tenant` is a string representing the tenant portion of the receiver URL path parameter.
      * `namespace` is a string representing the topic portion of the receiver URL path parameter.
      * `topic` is a string representing the topic portion of the receiver URL path parameter.
      * `subscription` (for consumers) is a string representing the subscription portion of the
        receiver URL path paramater.
      * `registry` is the name of the process registry associated to the client's supervision tee.
        Stargate uses this to subscribe to the stages of the receiver and to send messages back
        and forth between them.
      * `handler` is the name of the handler module that implements the
        `Stargate.Receiver.MessageHandler` behaviour.

  Additional optional parameters to a consumer and reader are:

      * `protocol` can be one of "ws" or "wss"; defaults to "ws".
      * `persistence` can be one of "persistent" orr "non-persistent" per the Pulsar specification
        of topics as being in-memory only or persisted to the brokers' disks. Defaults to "persistent".
      * `processors` is the number of GenStage processes in the "processor" stage to be created.
        This is the stage that performs the work of the message handler to perform processing logic
        on the received messages. Defaults to 1.
      * `handler_init_args` is any term that will be passed to the message handler to initialize
        its state when a stateful handler is desired. Defaults to an empty list.
      * `query_params` is a map containing any or all of the following:

      # Consumer

          * `ack_timeout` sets the timeout for unacked messages. Defaults to 0.
          * `subscription_type` can be one of `:exclusive`, `:failover`, or `:shared` to tell
            the Pulsar cluster if one or more consumers will be receiving messages on this topic
            and subscription. Defaults to exclusive.
          * `queue_size` sets the number of messages in the consumer's receive queue. Defaults to 1000.
          * `name` registers a name for the consumer client with the Pulsar cluster.
          * `priority` sets the priority with the cluster for the consumer client to receive messages.
          * `max_redeliver_count` defines a maximum number of times to attempt redelivery of a message
            to the consumer before sending it to a dead letter queue. Activates the dead letter topic feature.
          * `dead_letter_topic` defines a name for a topic's corresponding dead letter topic. Activates
            the dead letter topic feature. Defaults to "{topic}-{subscription}-DLQ".
          * `pull_mode` can be `true` or `false`. When a consumer is in pull mode, the cluster will hold
            messages on the subscription until it receives a permit request with an explicit number
            of desired messages to fulfill.

      # Reader

          * `name` registers a name for the reader client with the Pulsar cluster.
          * `queue_size` is the size of the queue maintained for the reader; defaults to 1000.
          * `starting_message` can be one of `:earliest`, `:latest`, or a message ID.
            Sets the reader's cursor to the desired message within the stream. Defaults to latest.
  """

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args) do
    type = Keyword.fetch!(args, :type)
    registry = Keyword.fetch!(args, :registry)
    query_params_config = Keyword.get(args, :query_params)

    query_params =
      case type do
        :consumer -> Consumer.QueryParams.build_params(query_params_config)
        :reader -> Reader.QueryParams.build_params(query_params_config)
      end

    setup_state = %{
      registry: registry,
      query_params: query_params_config
    }

    state =
      args
      |> Stargate.Connection.connection_settings(type, query_params)
      |> Map.merge(setup_state)
      |> (fn fields -> struct(State, fields) end).()

    server_opts =
      args
      |> Stargate.Connection.auth_settings()
      |> Keyword.put(
        :name,
        via(
          state.registry,
          {:"#{type}", "#{state.persistence}", "#{state.tenant}", "#{state.namespace}",
           "#{state.topic}"}
        )
      )

    WebSockex.start_link(state.url, __MODULE__, state, server_opts)
  end

  @impl WebSockex
  def handle_frame(
        {:text, msg},
        %{persistence: persistence, tenant: tenant, namespace: ns, topic: topic} = state
      ) do
    Logger.debug("Received frame : #{inspect(msg)}")

    :ok =
      state.registry
      |> via({:dispatcher, "#{persistence}", "#{tenant}", "#{ns}", "#{topic}"})
      |> Dispatcher.push(msg)

    {:ok, state}
  end

  defp construct_response(id), do: "{\"messageId\":\"#{id}\"}"

  defp construct_permit(count), do: "{\"type\":\"permit\",\"permitMessages\":#{count}}"
end
