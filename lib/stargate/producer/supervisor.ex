defmodule Stargate.Producer.Supervisor do
  @moduledoc """
  TODO
  """
  use Supervisor
  import Stargate.Supervisor

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
      name: via(registry, :"sg_prod_sup_#{tenant}_#{namespace}_#{topic}")
    )
  end

  @doc """
  TODO
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(args) do
    tenant = Keyword.fetch!(args, :tenant)
    namespace = Keyword.fetch!(args, :namespace)
    topic = Keyword.fetch!(args, :topic)
    Supervisor.child_spec(super(args), id: :"sg_prod_sup_#{tenant}_#{namespace}_#{topic}")
  end

  @doc """
  TODO
  """
  @impl Supervisor
  def init(args) do
    children = [
      {Stargate.Producer.Acknowledger, [args]},
      {Stargate.Producer, [args]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
