defmodule Stargate.Receiver.Acknowledger do
  @moduledoc """
  TODO
  """

  use GenStage
  import Stargate.Supervisor, only: [via: 2]

  defmodule State do
    @moduledoc """
    TODO
    """

    defstruct [
      :type,
      :registry,
      :tenant,
      :namespace,
      :topic
    ]
  end

  @doc """
  TODO
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(init_args) do
    registry = Keyword.fetch!(init_args, :registry)
    type = Keyword.fetch!(init_args, :type)
    tenant = Keyword.fetch!(init_args, :tenant)
    ns = Keyword.fetch!(init_args, :namespace)
    topic = Keyword.fetch!(init_args, :topic)

    GenStage.start_link(__MODULE__, init_args,
      name: via(registry, :"sg_#{type}_ack_#{tenant}_#{ns}_#{topic}")
    )
  end

  @impl GenStage
  def init(init_args) do
    type = Keyword.fetch!(init_args, :type)
    registry = Keyword.fetch!(init_args, :registry)
    tenant = Keyword.fetch!(init_args, :tenant)
    ns = Keyword.fetch!(init_args, :namespace)
    topic = Keyword.fetch!(init_args, :topic)
    processors = Keyword.get(init_args, :processors, 1)

    state = %State{
      type: type,
      registry: registry,
      tenant: tenant,
      namespace: ns,
      topic: topic
    }

    subscriptions = subscriptions(registry, tenant, ns, topic, processors)

    {:consumer, state, subscribe_to: subscriptions}
  end

  @impl GenStage
  def handle_events(messages, _from, state) do
    receiver =
      via(state.registry, :"sg_#{state.type}_#{state.tenant}_#{state.namespace}_#{state.topic}")

    messages
    |> Enum.filter(fn {action, _id} -> action == :ack end)
    |> Enum.map(fn {_action, id} -> id end)
    |> ack_messages(receiver)

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(_, state), do: {:noreply, [], state}

  defp ack_messages([], _receiver), do: nil
  defp ack_messages(messages, receiver) do
    Enum.each(messages, &Stargate.Receiver.ack(receiver, &1))
  end

  defp subscriptions(registry, tenant, namespace, topic, count) do
    Enum.map(0..(count - 1), &subscription_spec(&1, registry, tenant, namespace, topic))
  end

  defp subscription_spec(number, registry, tenant, namespace, topic) do
    producer = via(registry, :"sg_processor_#{tenant}_#{namespace}_#{topic}_#{number}")
    {producer, []}
  end
end
