defmodule Stargate.Receiver.Supervisor do
  @moduledoc """
  Defines a supervisor for the `Stargate.Receiver` reader
  and consumer connections and the associated GenStage pipeline
  for processing and acknowledging messages received on the connection.

  The top-level `Stargate.Supervisor` passes the shared connection and
  `:consumer` or `:reader` configurations to the receiver supervisor
  to delegate management of all receiving processes.
  """
  use Supervisor
  import Stargate.Supervisor, only: [via: 2]

  @doc """
  Starts a `Stargate.Receiver.Supevisor` and links it to the calling
  process.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args) do
    type = Keyword.fetch!(args, :type)
    registry = Keyword.fetch!(args, :registry)
    persistence = Keyword.get(args, :persistence, "persistent")
    tenant = Keyword.fetch!(args, :tenant)
    namespace = Keyword.fetch!(args, :namespace)
    topic = Keyword.fetch!(args, :topic)

    Supervisor.start_link(__MODULE__, args,
      name:
        via(registry, {:"#{type}_sup", "#{persistence}", "#{tenant}", "#{namespace}", "#{topic}"})
    )
  end

  @doc """
  Generates a list of child processes to initialize and
  start them under the supervisor with a `:one_for_all` strategy
  to ensure messages are not dropped if any single stage in
  the pipeline fails.

  The processors stage is configurable to a desired number of processes
  for parallelizing complex or long-running message handling operations.
  """
  @impl Supervisor
  def init(init_args) do
    children =
      [
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
    persistence = Keyword.get(init_args, :persistence, "persistent")
    tenant = Keyword.fetch!(init_args, :tenant)
    ns = Keyword.fetch!(init_args, :namespace)
    topic = Keyword.fetch!(init_args, :topic)
    name = {:processor, "#{persistence}", "#{tenant}", "#{ns}", "#{topic}_#{number}"}
    named_args = Keyword.put(init_args, :processor_name, name)

    Supervisor.child_spec({Stargate.Receiver.Processor, named_args}, id: name)
  end
end
