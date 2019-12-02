defmodule Stargate.Receiver.WorkerManager do
  @moduledoc """
  TODO
  """
  require Logger
  use GenServer
  import Stargate.Supervisor, only: [via: 2]

  defmodule State do
    @moduledoc """
    TODO
    """
    defstruct [
      :init_args,
      :registry,
      :receiver,
      :tenant,
      :namespace,
      :topic,
      :worker_count,
      :workers
    ]
  end

  @doc """
  TODO
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args) do
    registry = Keyword.fetch!(args, :registry)
    tenant = Keyword.fetch!(args, :tenant)
    namespace = Keyword.fetch!(args, :namespace)
    topic = Keyword.fetch!(args, :topic)

    GenServer.start_link(__MODULE__, args,
      name: via(registry, :"sg_mgr_#{tenant}_#{namespace}_#{topic}")
    )
  end

  @impl GenServer
  def init(args) do
    Process.flag(:trap_exit, true)

    state = %State{
      init_args: args,
      registry: Keyword.fetch!(args, :registry),
      receiver: nil,
      tenant: Keyword.fetch!(args, :tenant),
      namespace: Keyword.fetch!(args, :namespace),
      topic: Keyword.fetch!(args, :topic),
      worker_count: Keyword.get(args, :worker_count, 1),
      workers: %{}
    }

    {:ok, state, {:continue, :start_workers}}
  end

  @impl GenServer
  def handle_continue(:start_workers, %{registry: registry} = state) do
    [{receiver, _}] =
      Registry.lookup(
        registry,
        :"sg_read_#{state.tenant}_#{state.namespace}_#{state.topic}"
      )

    worker_init_args = state.init_args |> Keyword.put(:receiver, receiver)

    workers =
      Enum.reduce(
        0..(state.worker_count - 1),
        state.workers,
        fn _, acc -> start_worker(acc, registry, worker_init_args) end
      )

    :ok =
      Stargate.Receiver.register_workers(
        receiver,
        Map.values(workers)
      )

    {:noreply, %{state | receiver: receiver, workers: workers}}
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _object, _reason}, %{receiver: receiver} = state) do
    new_workers =
      state.workers
      |> Map.delete(ref)
      |> start_worker(state.registry, Keyword.put(state.init_args, :receiver, receiver))

    :ok = Stargate.Receiver.register_workers(receiver, Map.values(new_workers))

    {:noreply, %{state | workers: new_workers}}
  end

  defp start_worker(workers, registry, args) do
    {:ok, pid} =
      DynamicSupervisor.start_child(
        via(registry, :sg_worker_sup),
        {Stargate.Receiver.Worker, args}
      )

    ref = Process.monitor(pid)

    Map.put(workers, ref, pid)
  end
end
