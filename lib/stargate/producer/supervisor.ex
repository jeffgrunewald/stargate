defmodule Stargate.Producer.Supervisor do
  @moduledoc """
  Creates and manages a supervisor process for the Stargate
  producer websocket process and acknowledger process.
  """
  use Supervisor
  import Stargate.Supervisor, only: [via: 2]

  @doc """
  Create a `Stargate.Producer.Supervisor` process and link it
  to the calling process.

  Passes the shared and `:producer` configurations from the
  top-level supervisor to the producer and acknowledger and
  starts them under a :one_for_one strategy.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args) do
    registry = Keyword.fetch!(args, :registry)
    persistence = Keyword.get(args, :persistence, "persistent")
    tenant = Keyword.fetch!(args, :tenant)
    namespace = Keyword.fetch!(args, :namespace)
    topic = Keyword.fetch!(args, :topic)

    Supervisor.start_link(__MODULE__, args,
      name:
        via(registry, {:producer_sup, "#{persistence}", "#{tenant}", "#{namespace}", "#{topic}"})
    )
  end

  @doc """
  Generates a child specification for creating producer supervisor
  trees.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(args) do
    persistence = Keyword.get(args, :persistence, "persistent")
    tenant = Keyword.fetch!(args, :tenant)
    namespace = Keyword.fetch!(args, :namespace)
    topic = Keyword.fetch!(args, :topic)

    Supervisor.child_spec(super(args),
      id: {:producer_sup, "#{persistence}", "#{tenant}", "#{namespace}", "#{topic}"}
    )
  end

  @impl Supervisor
  def init(args) do
    children = [
      {Stargate.Producer.Acknowledger, args},
      {Stargate.Producer, args}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
