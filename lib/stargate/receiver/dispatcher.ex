defmodule Stargate.Receiver.Dispatcher do
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
      :topic,
      :pull_mode,
      :receiver
    ]
  end

  @type raw_message :: String.t()

  @doc """
  TODO
  """
  @spec push(GenServer.server(), [raw_message()] | raw_message()) :: :ok
  def push(dispatcher, messages), do: GenServer.cast(dispatcher, {:push, messages})

  @doc """
  TODO
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(init_args) do
    registry = Keyword.fetch!(init_args, :registry)
    tenant = Keyword.fetch!(init_args, :tenant)
    ns = Keyword.fetch!(init_args, :namespace)
    topic = Keyword.fetch!(init_args, :topic)

    GenStage.start_link(__MODULE__, init_args,
      name: via(registry, :"sg_dispatcher_#{tenant}_#{ns}_#{topic}")
    )
  end

  @impl GenStage
  def init(init_args) do
    type = Keyword.fetch!(init_args, :type)
    tenant = Keyword.fetch!(init_args, :tenant)
    ns = Keyword.fetch!(init_args, :namespace)
    topic = Keyword.fetch!(init_args, :topic)

    pull =
      case get_in(init_args, [:query_params, :pull_mode]) do
        true -> true
        _ -> false
      end

    state = %State{
      registry: Keyword.fetch!(init_args, :registry),
      tenant: tenant,
      namespace: ns,
      topic: topic,
      pull_mode: pull,
      receiver: :"sg_#{type}_#{tenant}_#{ns}_#{topic}"
    }

    {:ok, _receiver} = Stargate.Receiver.start_link(init_args)

    {:producer, state}
  end

  @impl GenStage
  def handle_cast({:push, messages}, state) when is_list(messages) do
    {:noreply, messages, state}
  end

  @impl GenStage
  def handle_cast({:push, message}, state), do: {:noreply, [message], state}

  @impl GenStage
  def handle_demand(demand, %{pull_mode: true} = state) do
    receiver = via(state.registry, state.receiver)

    Stargate.Receiver.pull_permit(receiver, demand)

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_demand(_, state), do: {:noreply, [], state}

  @impl GenStage
  def handle_info(_, state), do: {:noreply, [], state}
end
