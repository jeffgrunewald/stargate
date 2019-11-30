defmodule Stargate.Receiver.Worker do
  @moduledoc """
  TODO
  """
  use GenServer

  @type raw_messages :: String.t()

  defmodule State do
    @moduledoc """
    TODO
    """
    defstruct [
      :registry,
      :receiver,
      :id,
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
  TODO
  """
  @spec process(GenServer.server(), [raw_messages()]) :: :ok
  def process(worker, messages), do: GenServer.cast(worker, {:process, messages})

  @doc """
  TODO
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl GenServer
  def init(args) do
    state = %State{
      registry: Keyword.fetch!(args, :registry),
      receiver: Keyword.fetch!(args, :receiver),
      id: Keyword.fetch!(args, :id),
      topic: Keyword.fetch!(args, :topic),
      namespace: Keyword.fetch!(args, :namespace),
      tenant: Keyword.fetch!(args, :tenant),
      persistence: Keyword.get(args, :persistence, "persistent"),
      handler: Keyword.fetch!(args, :handler),
      handler_init_args: Keyword.get(args, :handler_init_args, [])
    }

    Process.put(:sg_topic, state.topic)
    Process.put(:sg_namespace, state.namespace)
    Process.put(:sg_tenant, state.tenant)
    Process.put(:sg_persistence, state.persistence)

    Registry.register(
      state.registry,
      :"sg_worker_#{state.tenant}_#{state.namespace}_#{state.topic}_#{state.id}",
      self()
    )

    {:ok, handler_state} = state.handler.init(state.handler_init_args)

    {:ok, %{state | handler_state: handler_state}}
  end

  @impl GenServer
  def handle_cast({:process, messages}, %{receiver: receiver} = state) do
    decoded_messages =
      decode_messages(messages, state.persistence, state.tenant, state.namespace, state.topic)

    case send_messages_to_handler(decoded_messages, state) do
      {:ack, new_handler_state} ->
        ack_messages(decoded_messages, receiver)
        {:noreply, %{state | handler_state: new_handler_state}}

      {:continue, new_handler_state} ->
        {:noreply, %{state | handler_state: new_handler_state}}
    end
  end

  defp decode_messages(messages, persistence, tenant, namespace, topic) do
    Enum.map(messages, &decode_message(&1, persistence, tenant, namespace, topic))
  end

  defp decode_message(message, persistence, tenant, namespace, topic) do
    message |> Jason.decode!() |> Stargate.Message.new(persistence, tenant, namespace, topic)
  end

  defp send_messages_to_handler(message, state) do
    state.handler.handle_messages(message, state.handler_state)
  end

  defp ack_messages(messages, receiver) do
    Enum.each(messages, &Stargate.Reader.ack(receiver, &1))
  end
end
