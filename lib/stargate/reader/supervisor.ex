defmodule Stargate.Reader.Supervisor do
  @moduledoc """
  TODO
  """
  use Supervisor
  import Stargate.Supervisor, only: [via: 2]

  @doc """
  TODO
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args) do
    registry = Keyword.fetch!(args, :registry)
    tenant = Keyword.fetch!(args, :tenant)
    namespace = Keyword.fetch!(args, :namespace)
    topic = Keyword.fetch!(args, :topic)

    Supervisor.start_link(__MODULE__, args,
      name: via(registry, :"sg_read_sup_#{tenant}_#{namespace}_#{topic}")
    )
  end

  @doc """
  TODO
  """
  @impl Supervisor
  def init(args) do
    registry = Keyword.fetch!(args, :registry)

    children = [
      {DynamicSupervisor, [strategy: :one_for_one, name: via(registry, :sg_worker_sup)]},
      {Stargate.Receiver.WorkerManager, args},
      {Stargate.Reader, args}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
