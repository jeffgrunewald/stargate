defmodule Stargate.Producer.Acknowledger do
  @moduledoc """
  TODO
  """
  require Logger
  use GenServer
  import Stargate.Supervisor, only: [via: 2]

  @doc """
  TODO
  """
  @spec ack(GenServer.server(), {:ack, term()} | {:error, term(), term()}) :: :ok
  def ack(acknowledger, response), do: GenServer.cast(acknowledger, response)

  @doc """
  TODO
  """
  @spec produce(GenServer.server(), String.t(), pid() | tuple()) :: :ok
  def produce(acknowledger, ctx, ack), do: GenServer.cast(acknowledger, {:produce, ctx, ack})

  @doc """
  TODO
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(init_args) do
    registry = Keyword.fetch!(init_args, :registry)
    tenant = Keyword.fetch!(init_args, :tenant)
    ns = Keyword.fetch!(init_args, :namespace)
    topic = Keyword.fetch!(init_args, :topic)

    GenServer.start_link(__MODULE__, init_args,
      name: via(registry, :"sg_prod_ack_#{tenant}_#{ns}_#{topic}")
    )
  end

  @doc """
  TODO
  """
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
      pid when is_pid(pid) ->
        send(pid, :ack)

      {module, function, args} ->
        apply(module, function, args)
    end

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_cast({:error, reason, ctx}, state) do
    {value, new_state} = Map.pop(state, ctx)

    case value do
      pid when is_pid(pid) ->
        send(pid, {:error, reason})

      _mfa ->
        Logger.error("Failed to execute produce for reason : #{inspect(reason)}")
    end

    {:noreply, new_state}
  end
end
