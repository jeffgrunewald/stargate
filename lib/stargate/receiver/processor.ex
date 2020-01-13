defmodule Stargate.Receiver.Processor do
  @moduledoc """
  Defines a `Stargate.Receiver.Processor` module as a GenStage
  process under the consumer or reader supervision tree.

  The processor stage performs the message handling step for all
  messages received on the connection by storing and calling the
  application's handler module on each message received.

  To better handle complex or long-running operations when handling
  messages, the processor stage can be scaled horizontally and takes
  care of the necessary subscriptions both upstream and downstream
  within the GenStage pipeline.

  During initialization, the processor stage stores several
  pieces of information in its process dictionary that are
  available to the application's message handler module when
  handling messages if necessary including the topicc, namespace,
  tenant, and persistence of the connection.
  """
  use GenStage
  import Stargate.Supervisor, only: [via: 2]

  @type raw_message :: String.t()

  defmodule State do
    @moduledoc """
    Defines the struct `Stargate.Receiver.Processor` uses
    to store its state.

    Records the name of the process registry, the path parameters
    (persistence, tenant, namespace, and topic) as well as the
    handler module defined by the calling application, any init
    args rrequired for a stateful handler and the state of the handler.
    """
    defstruct [
      :registry,
      :topic,
      :namespace,
      :tenant,
      :persistence,
      :handler,
      :handler_init_args,
      :handler_state
    ]
  end

  @doc """
  Starts a `Stargate.Receiver.Processor` GenStage process and
  links it to the calling process.

  Passes the configuration from the supervisors to the stage to
  initialize its state and setup subscription to the
  `Stargate.Receiver.Dispatcher` producer stage.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(init_args) do
    registry = Keyword.fetch!(init_args, :registry)
    name = Keyword.fetch!(init_args, :processor_name)

    GenStage.start_link(__MODULE__, init_args, name: via(registry, name))
  end

  @impl GenStage
  def init(init_args) do
    state = %State{
      registry: Keyword.fetch!(init_args, :registry),
      topic: Keyword.fetch!(init_args, :topic),
      namespace: Keyword.fetch!(init_args, :namespace),
      tenant: Keyword.fetch!(init_args, :tenant),
      persistence: Keyword.get(init_args, :persistence, "persistent"),
      handler: Keyword.fetch!(init_args, :handler),
      handler_init_args: Keyword.get(init_args, :handler_init_args, [])
    }

    Process.put(:sg_topic, state.topic)
    Process.put(:sg_namespace, state.namespace)
    Process.put(:sg_tenant, state.tenant)
    Process.put(:sg_persistence, state.persistence)

    dispatcher =
      via(state.registry, :"sg_dispatcher_#{state.tenant}_#{state.namespace}_#{state.topic}")

    {:ok, handler_state} = state.handler.init(state.handler_init_args)

    {:producer_consumer, %{state | handler_state: handler_state},
     subscribe_to: [{dispatcher, []}]}
  end

  @impl GenStage
  def handle_events(messages, _from, state) do
    decoded_messages =
      decode_messages(messages, state.persistence, state.tenant, state.namespace, state.topic)

    {_, new_handler_state, responses} = handle_messages(decoded_messages, state)

    message_ids = Enum.map(decoded_messages, fn message -> message.message_id end)
    tagged_responses = Enum.zip(Enum.reverse(responses), message_ids)

    {:noreply, tagged_responses, %{state | handler_state: new_handler_state}}
  end

  @impl GenStage
  def handle_info(_, state), do: {:noreply, [], state}

  defp decode_messages(messages, persistence, tenant, namespace, topic) do
    Enum.map(messages, &decode_message(&1, persistence, tenant, namespace, topic))
  end

  defp decode_message(message, persistence, tenant, namespace, topic) do
    message |> Jason.decode!() |> Stargate.Message.new(persistence, tenant, namespace, topic)
  end

  defp handle_messages(messages, %{handler: handler, handler_state: state}) do
    Enum.reduce(messages, {handler, state, []}, &process_handler/2)
  end

  defp process_handler(message, {handler, state, responses}) do
    {response, new_state} = handler.handle_message(message, state)

    {handler, new_state, [response | responses]}
  end
end
