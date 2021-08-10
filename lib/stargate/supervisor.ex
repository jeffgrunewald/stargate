defmodule Stargate.Supervisor do
  @moduledoc """
  This module defines a top-level supervisor for your Stargate client.
  It takes a keyword list of configuration values for connecting to the
  cluster and any producers, readers, or consumers desired, and orchestrates
  the starting of the process registry and the websocket client(s).
  """
  use Supervisor

  @type process_key :: {atom(), String.t(), String.t(), String.t(), String.t()}

  @doc """
  Convenience function for working with the Stargate process registry.
  """
  @spec via(atom(), process_key()) :: {:via, atom(), {atom(), process_key()}}
  def via(registry, name) do
    {:via, Registry, {registry, name}}
  end

  @doc """
  Creates a Stargate Supervisor and links it to the current process.
  Pass a keyword list of connection and client information to define
  the types of websocket connection to establish with the Pulsar cluster.

  # Example

      opts = [
          host: [{"broker-url.com", 8080}]
          producer: [
              ... producer configs ...
          ],
          consumer: [
              ... consumer configs ...
          ]
      ]

  See the `Stargate.Producer` and `Stargate.Receiver` modules for the full
  list of configuration options to each type of client connection.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(init_args) do
    name = Keyword.get(init_args, :name, :default)

    Supervisor.start_link(__MODULE__, init_args, name: :"sg_sup_#{name}")
  end

  @impl Supervisor
  def init(init_args) do
    name = Keyword.get(init_args, :name, :default)
    registry = :"sg_reg_#{name}"
    host = Keyword.fetch!(init_args, :host)
    protocol = Keyword.get(init_args, :protocol, "ws")

    children =
      [
        {Registry, name: registry, keys: :unique},
        start_producer(registry, host, protocol, Keyword.get(init_args, :producer)),
        start_consumer(registry, host, protocol, Keyword.get(init_args, :consumer)),
        start_reader(registry, host, protocol, Keyword.get(init_args, :reader))
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
    receiver_child_spec(:consumer, registry, host, protocol, args)
  end

  defp start_reader(_registry, _host, _protocol, nil), do: []

  defp start_reader(registry, host, protocol, args) do
    receiver_child_spec(:reader, registry, host, protocol, args)
  end

  defp producer_child_spec(registry, host, protocol, args) do
    producer_args = merge_args(args, host: host, protocol: protocol, registry: registry)

    {Stargate.Producer.Supervisor, producer_args}
  end

  defp receiver_child_spec(type, registry, host, protocol, args) do
    receiver_args =
      merge_args(args, type: type, registry: registry, host: host, protocol: protocol)

    {Stargate.Receiver.Supervisor, receiver_args}
  end

  defp merge_args(args1, args2) do
    Keyword.merge(args1, args2, fn _k, _v1, v2 -> v2 end)
  end
end
