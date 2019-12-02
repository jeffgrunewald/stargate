defmodule Stargate.Supervisor do
  @moduledoc """
  TODO
  """
  use Supervisor

  @doc """
  TODO
  """
  @spec via(atom(), atom()) :: {:via, atom(), tuple()}
  def via(registry, name) do
    {:via, Registry, {registry, name}}
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
    host = Keyword.fetch!(args, :host)
    protocol = Keyword.get(args, :protocol, "ws")

    children =
      [
        {Registry, name: registry, keys: :unique},
        start_producer(registry, host, protocol, Keyword.get(args, :producer)),
        start_consumer(registry, host, protocol, Keyword.get(args, :consumer)),
        start_reader(registry, host, protocol, Keyword.get(args, :reader))
      ]
      |> List.flatten()

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp start_producer(_registry, _host, _protocol, nil), do: []

  defp start_producer(registry, host, protocol, args) do
    case Keyword.keyword?(args) do
      true ->
        producer_child_spec(registry, host, protocol, args)

      false ->
        Enum.map(args, fn producer -> producer_child_spec(registry, host, protocol, producer) end)
    end
  end

  defp start_consumer(_registry, _host, _protocol, nil), do: []

  defp start_consumer(registry, host, protocol, args) do
    consumer_args =
      merge_args(args, type: :consumer, host: host, protocol: protocol, registry: registry)

    {Stargate.Receiver.Supervisor, consumer_args}
  end

  defp start_reader(_registry, _host, _protocol, nil), do: []

  defp start_reader(registry, host, protocol, args) do
    reader_args =
      merge_args(args, type: :reader, host: host, protocol: protocol, registry: registry)

    {Stargate.Receiver.Supervisor, reader_args}
  end

  defp producer_child_spec(registry, host, protocol, args) do
    producer_args = merge_args(args, host: host, protocol: protocol, registry: registry)

    {Stargate.Producer.Supervisor, producer_args}
  end

  defp merge_args(args1, args2) do
    Keyword.merge(args1, args2, fn _k, _v1, v2 -> v2 end)
  end
end
