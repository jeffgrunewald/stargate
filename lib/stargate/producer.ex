defmodule Stargate.Producer do
  @moduledoc """
  Provides a producer websocket process and functions for producing
  messages to the cluster.

  Pass a keyword list of configuration options to the `start_link/1`
  function or simply call `produce/2` passing a valid
  Pulsar producer URL in place of a producer process.
  """
  require Logger
  use Stargate.Connection
  use Puid
  import Stargate.Supervisor, only: [via: 2]
  alias Stargate.Producer.{Acknowledger, QueryParams}

  @typedoc """
  A URL defining the host and topic to which a Stargate producer can
  connect for sending messages.
  """
  @type url() :: String.t()

  @typedoc """
  A producer websocket process identified by a pid or via tuple.
  The atom key for identifying the producer in the via tuple is of
  the form `:"sg_prod_<tenant>_<namespace>_<topic>`.
  """
  @type producer :: GenServer.server()

  @typedoc """
  Pulsar messages produced by Stargate can be any of the following forms:

      * raw binary payload (must be encodable to base64)
      * a {key, value} tuple where key is the optional message key and value is the payload
      * a map with a "payload" field and optional fields for a key, context, properties (key/value
        pairs as a map), and list of strings identifying replication clusters.

  Stargate uses the `context` field on a message produced to Pulsar to correlate receipt messages
  from the cluster to sent messages. If you do not define a context in your message, Stargate
  generates one automatically.
  """
  @type message ::
          String.t()
          | {String.t(), String.t()}
          | %{
              required(:payload) => String.t(),
              optional(:key) => String.t(),
              optional(:context) => String.t(),
              optional(:properties) => map(),
              optional(:replicationClusters) => [String.t()]
            }

  @doc """
  Produce a message or list of messages to the cluster by producer URL or producer process.
  Messages can be any of the accepted forms (see `message` type).

  Producing by URL is good for irregular and/or ad hoc producer needs that do not require
  a persistent websocket connection and ideally with few to no query parameters
  to configure producer options from the default. For higher volume producing, a persistent
  connection with an addressable producer process is recommended.

  Once the message(s) is produced, the calling process automatically blocks until
  it receives acknowledgement from the cluster that the message(s) has been received.
  """
  @spec produce(url() | producer(), message() | [message()]) :: :ok | {:error, term()}
  def produce(url, messages) when is_binary(url) do
    with [protocol, _, host, _, _, _, persistence, tenant, ns, topic | _] <-
           String.split(url, "/"),
         opts <- temp_producer_opts(:temp, protocol, host, persistence, tenant, ns, topic),
         {:ok, temp_producer} <- Stargate.Supervisor.start_link(opts),
         :ok <- produce(via(:sg_reg_temp, {:producer, persistence, tenant, ns, topic}), messages) do
      Process.unlink(temp_producer)
      Supervisor.stop(temp_producer)
      :ok
    else
      {:error, reason} -> {:error, reason}
      error -> {:error, error}
    end
  end

  def produce(producer, messages) when is_list(messages) do
    Enum.each(messages, &produce(producer, &1))
  end

  def produce(producer, message) do
    {payload, ctx} = construct_payload(message)
    ref = make_ref()
    WebSockex.cast(producer, {:send, payload, ctx, {self(), ref}})

    receive do
      {^ref, :ack} -> :ok
      {^ref, :error, reason} -> {:error, reason}
    end
  end

  @doc """
  Produce a list of messages to a Stargate producer process. Messages can be any
  of the accepted forms (see `message` type).

  When calling `produce/3` the third argument must be an MFA tuple which is used by
  the producer's acknowledger process to asynchronously perform acknowledgement that the
  message was received by the cluster successfully. This is used to avoid blocking the
  calling process for performance reasons.
  """
  @spec produce(producer(), message() | [message()], {module(), atom(), [term()]}) ::
          :ok | {:error, term()}
  def produce(producer, messages, mfa) when is_list(messages) do
    Enum.each(messages, &produce(producer, &1, mfa))
  end

  def produce(producer, message, mfa) do
    {payload, ctx} = construct_payload(message)

    WebSockex.cast(producer, {:send, payload, ctx, mfa})
  end

  defmodule State do
    @moduledoc """
    Defines the state stored by the producer websocket process. The Stargate producer
    records the registry name associated to its supervision tree, the URL of the cluster and topic
    it connects to, as well as the individual components that make up the URL including the
    host, protocol (ws or wss), topic path parameters (persistent or non-persistent, tenant,
    namespace, and topic) and any query parameters configuring the connection.
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
  Start a producer websocket process and link it to the current process.

  Producer options require, at minimum:

      * `host` is a tuple of the address or URL of the Pulsar cluster (broker service)
        and the port on which the service is exposed.
      * `tenant` is a string representing the tenant portion of the producer URL path parameter.
      * `namespace` is a string representing the namespace portion of the producer URL path parameter.
      * `topic` is a string representing the topic portion of the producer URL path parameter.
      * `registry` is the name of the process registry associated to the client's supervision tree.
        Stargate uses this to send messages back and forth between the producer and its acknowledger.

  Additional optional parameters to a producer are:

      * `protocol` can be one of "ws" or "wss"; defaults to "ws"
      * `persistence` can be one of "persistent" or "non-persistent" per the Pulsar
        specification of topics as being in-memory only or persisted to the brokers' disks.
        Defaults to "persistent".
      * `query_params` is a map containing any or all of the following:

          * `send_timeout` the time at which a produce operation will time out; defaults to 30 seconds
          * `batch_enabled` can be true or false to enable/disable the batching of messages.
            Defaults to "false".
          * `batch_max_msg` defines the maximum number of messages in a batch (if enabled).
            Defaults to 1000.
          * `max_pending_msg` defines the maximum size of the internal queue holding messages. Defaults
            to 1000.
          * `batch_max_delay` sets the time period within which message batches will be published.
            Defaults to 10 milliseconds.
          * `routing_mode` can be one of :round_robin or :single. _Pulsar has deprecated this parameter_.
          * `compression_type` can be one of :lz4, :zlib, or :none. Defaults to :none
          * `name` is used to enforce only one producer with the given name is publishing to
            connected topic.
          * `initial_seq_id` sets the baseline for the sequence ids assigned to published messages.
          * `hashing_scheme` can be one of :java_string or :murmur3 when defining a hashing function to
            use with partitioned topics. _Pulsar has deprecated this parameter_.

  Note, the `producer` key of the supervisor config options will accept a list of producer configuration options to allow a
  single superviser to start and manage multiple producers.

  ```
  [
      name: :my_app,
      host: [{"localhost", 8080}],
      producer: [
            [
              persistence: "persistent",
              tenant: "na_sales",
              namespace: "electronics",
              topic: "events"
            ],
            [
              persistence: "persistent",
              tenant: "eu_sales",
              namespace: "textiles",
              topic: "events"
            ]
      ]
  ]
  ```

  While this is perfectly possible, you should carefully
  consider your supervision strategy and only group producers to different namespaces/tenants/topics/etc under a single
  supervisor where it makes sense to have the process lifecycle of each producer tightly linked to one another and
  consider the implications of a failure/death of one producer process on the others. Depending on your specific use case
  it is often equally if not more correct to have each unique producer under a dedicated supervisor, perhaps managed by
  a still higher-level supervisor in your own application. Plan accordingly based on your expected failure scenarios.

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args) do
    query_params_config = Keyword.get(args, :query_params)
    query_params = QueryParams.build_params(query_params_config)
    registry = Keyword.fetch!(args, :registry)

    state =
      args
      |> Stargate.Connection.connection_settings(:producer, query_params)
      |> Map.put(:query_params, query_params_config)
      |> Map.put(:registry, registry)
      |> (fn fields -> struct(State, fields) end).()

    server_opts =
      args
      |> Stargate.Connection.auth_settings()
      |> Keyword.put(
        :name,
        via(
          state.registry,
          {:producer, "#{state.persistence}", "#{state.tenant}", "#{state.namespace}",
           "#{state.topic}"}
        )
      )

    WebSockex.start_link(state.url, __MODULE__, state, server_opts)
  end

  @impl WebSockex
  def handle_cast({:send, payload, ctx, ack}, state) do
    Acknowledger.produce(
      via(
        state.registry,
        {:producer_ack, "#{state.persistence}", "#{state.tenant}", "#{state.namespace}",
         "#{state.topic}"}
      ),
      ctx,
      ack
    )

    {:reply, {:text, payload}, state}
  end

  @impl WebSockex
  def handle_frame({:text, msg}, state) do
    Logger.debug("Received response : #{inspect(msg)}")

    response =
      msg
      |> Jason.decode!()
      |> format_response()

    :ok =
      state.registry
      |> via(
        {:producer_ack, "#{state.persistence}", "#{state.tenant}", "#{state.namespace}",
         "#{state.topic}"}
      )
      |> Acknowledger.ack(response)

    {:ok, state}
  end

  defp construct_payload(%{"payload" => _payload, "context" => context} = message) do
    encoded_message =
      message
      |> Map.update!("payload", &Base.encode64(&1))
      |> Jason.encode!()

    {encoded_message, context}
  end

  defp construct_payload(%{"payload" => _payload} = message) do
    context = generate()

    encoded_message =
      message
      |> Map.put("context", context)
      |> Map.update!("payload", &Base.encode64(&1))
      |> Jason.encode!()

    {encoded_message, context}
  end

  defp construct_payload({key, payload}) do
    context = generate()

    encoded_message =
      %{
        "key" => key,
        "payload" => Base.encode64(payload),
        "context" => context
      }
      |> Jason.encode!()

    {encoded_message, context}
  end

  defp construct_payload(message) when is_binary(message) do
    context = generate()

    encoded_message =
      %{
        "payload" => Base.encode64(message),
        "context" => context
      }
      |> Jason.encode!()

    {encoded_message, context}
  end

  defp format_response(%{"result" => "ok", "context" => ctx}) do
    {:ack, ctx}
  end

  defp format_response(%{"result" => error, "errorMsg" => explanation, "context" => ctx}) do
    Logger.debug("[Stargate] error of type : #{error} occurred; #{explanation}")
    {:error, error, ctx}
  end

  defp temp_producer_opts(name, protocol, host, persistence, tenant, namespace, topic) do
    [
      name: name,
      host: host,
      protocol: String.trim(protocol, ":"),
      producer: [
        persistence: persistence,
        tenant: tenant,
        namespace: namespace,
        topic: topic
      ]
    ]
  end
end
