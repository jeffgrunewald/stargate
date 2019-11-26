defmodule Stargate.Supervisor do
  @moduledoc """
  TODO
  """
  use Supervisor

  @doc """
  TODO
  """
  @spec via(atom(), atom()) :: pid()
  def via(registry, name) do
    {:via, Registry, {:"sg_reg_#{registry}", name}}
  end

  @doc """
  TODO
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args) do
    name = Keyword.fetch!(args, :name)

    Supervisor.start_link(__MODULE__, args, name: :"sg_sup_#{name}")
  end

  @impl Supervisor
  def init(args) do
    name = Keyword.fetch!(args, :name)
    registry = :"sg_reg_#{name}"

    children =
      [
        {Registry, name: registry},
        start_producer(registry, Keyword.get(args, :producer)),
        start_consumer(registry, Keyword.get(args, :consumer)),
        start_reader(registry, Keyword.get(args, :reader))
      ]
      |> List.flatten()

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp start_producer(_registry, nil), do: []

  defp start_producer(registry, args) do
    case Keyword.keyword?(args) do
      true -> producer_child_spec(registry, args)
      false -> Enum.map(args, fn producer -> producer_child_spec(registry, producer) end)
    end
  end

  defp start_consumer(_registry, nil), do: []

  defp start_reader(_registry, nil), do: []

  defp producer_child_spec(registry, args) do
    producer_args = Keyword.put(args, :registry, registry)

    {Stargate.Producer.Supervisor, producer_args}
  end
end
