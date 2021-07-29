defmodule Stargate.Receiver.Dispatcher do
  @moduledoc """
  Defines the `Stargate.Receiver.Dispatcher` GenStage process
  that functions as the producer in the pipeline, receiving messages
  pushed from the reader or consumer socket and dispatching to the
  rest of the pipeline.
  """
  use GenStage
  import Stargate.Supervisor, only: [via: 2]

  defmodule State do
    @moduledoc """
    Defines the struct used by a `Stargate.Receiver.Dispatcher`
    to store its state.

    Includes the type of the receiver (consumer or reader), the name
    of the process registry associated to the supervision tree, the
    path parameters of the topic (tenant, namespace, topic), the atom
    key of the websocket connection within the process registry, and
    whether or not the receiver is in push or pull mode if it's consumer.
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
  Push messages received over the reader or consumer connection into the
  GenStage processing pipeline for handling and acknowledgement. This is normally
  handled automatically by the websocket connection but can also be called directly
  for testing the receive pipeline.
  """
  @spec push(GenServer.server(), [raw_message()] | raw_message()) :: :ok
  def push(dispatcher, messages), do: GenServer.cast(dispatcher, {:push, messages})

  @doc """
  Starts a `Stargate.Receiver.Dispatcher` GenStage process and links it to
  the calling process.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(init_args) do
    registry = Keyword.fetch!(init_args, :registry)
    tenant = Keyword.fetch!(init_args, :tenant)
    ns = Keyword.fetch!(init_args, :namespace)
    topic = Keyword.fetch!(init_args, :topic)

    GenStage.start_link(__MODULE__, init_args,
      name: via(registry, {:dispatcher, "#{tenant}", "#{ns}", "#{topic}"})
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
      receiver: {:"#{type}", "#{tenant}", "#{ns}", "#{topic}"}
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
