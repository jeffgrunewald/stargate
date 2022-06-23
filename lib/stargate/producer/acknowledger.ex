defmodule Stargate.Producer.Acknowledger do
  @moduledoc """
  By default, `Stargate.produce/2` will block the calling
  process until acknowledgement is received from Pulsar that the
  message was successfully produced. This can optionally switch
  to an asynchronous acknowledgement by passing an MFA tuple to
  `Stargate.produce/3`.

  This modules defines a GenServer process that
  works in tandem with a producer websocket connection
  to wait for and send receipt acknowledgements received
  from produce operations to the calling process or otherwise
  perform asynchronous acknowledgement operations.
  """
  require Logger
  use GenServer
  import Stargate.Supervisor, only: [via: 2]

  @doc """
  Sends a message to the acknowledger process to perform the ack
  operation saved for that particular message (as identified by the
  context sent with the message).
  """
  @spec ack(GenServer.server(), {:ack, term()} | {:error, term(), term()}) :: :ok
  def ack(acknowledger, response), do: GenServer.cast(acknowledger, response)

  @doc """
  Called by the producer when a message is produced to the Pulsar cluster.
  This function sends a message's context and the desired operation to perform
  for acknowledgement to the Acknowledger process to save in its state and act
  on when directed to acknowledge that message.

  Unless instructed otherwise by calling `Stargate.produce/3`, `Stargate.produce/2`
  assumes the third argument to be the PID of the calling process to send
  receipt confirmation and unblock.
  """
  @spec produce(GenServer.server(), String.t(), pid() | tuple()) :: :ok
  def produce(acknowledger, ctx, ack), do: GenServer.cast(acknowledger, {:produce, ctx, ack})

  @doc """
  Starts a `Stargate.Producer.Acknowledger` process and link it to the calling process.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(init_args) do
    registry = Keyword.fetch!(init_args, :registry)
    persistence = Keyword.get(init_args, :persistence, "persistent")
    tenant = Keyword.fetch!(init_args, :tenant)
    ns = Keyword.fetch!(init_args, :namespace)
    topic = Keyword.fetch!(init_args, :topic)

    GenServer.start_link(__MODULE__, init_args,
      name: via(registry, {:producer_ack, "#{persistence}", "#{tenant}", "#{ns}", "#{topic}"})
    )
  end

  @impl GenServer
  def init(_init_args) do
    {:ok, %{}}
  end

  @impl GenServer
  def handle_cast({:produce, ctx, ack}, state) do
    {:noreply, Map.put(state, ctx, ack)}
  end

  @impl GenServer
  def handle_cast({:ack, ctx}, state) do
    {value, new_state} = Map.pop(state, ctx)

    case value do
      {pid, ref} when is_pid(pid) ->
        send(pid, {ref, :ack})

      {module, function, args} ->
        apply(module, function, args)
    end

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_cast({:error, reason, ctx}, state) do
    {value, new_state} = Map.pop(state, ctx)

    case value do
      {pid, ref} when is_pid(pid) ->
        send(pid, {ref, :error, reason})

      _mfa ->
        Logger.error("Failed to execute produce for reason : #{inspect(reason)}")
    end

    {:noreply, new_state}
  end
end
