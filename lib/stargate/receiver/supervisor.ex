defmodule Stargate.Receiver.Supervisor do
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
    type = Keyword.fetch!(args, :type)
    registry = Keyword.fetch!(args, :registry)
    tenant = Keyword.fetch!(args, :tenant)
    namespace = Keyword.fetch!(args, :namespace)
    topic = Keyword.fetch!(args, :topic)

    Supervisor.start_link(__MODULE__, args,
      name: via(registry, :"sg_#{type}_sup_#{tenant}_#{namespace}_#{topic}")
    )
  end

  @doc """
  TODO
  """
  @impl Supervisor
  def init(init_args) do
    children =
      [
        {Stargate.Receiver, init_args},
        {Stargate.Receiver.Dispatcher, init_args},
        processors(init_args),
        {Stargate.Receiver.Acknowledger, init_args}
      ]
      |> List.flatten()

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp processors(args) do
    count = Keyword.get(args, :processors, 1)
    Enum.map(0..(count - 1), &to_child_spec(&1, args))
  end

  defp to_child_spec(number, init_args) do
    tenant = Keyword.fetch!(init_args, :tenant)
    ns = Keyword.fetch!(init_args, :namespace)
    topic = Keyword.fetch!(init_args, :topic)
    name = :"sg_processor_#{tenant}_#{ns}_#{topic}_#{number}"
    named_args = Keyword.put(init_args, :processor_name, name)

    Supervisor.child_spec({Stargate.Receiver.Processor, named_args}, id: name)
  end
end
