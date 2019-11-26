defmodule Stargate.Producer.Acknowledger do
  @moduledoc """
  TODO
  """
  require Logger
  use GenServer
  import Stargate.Supervisor

  @doc """
  TODO
  """
  @spec ack(pid(), {:ack, term()} | {:error, term(), term()}) :: :ok
  def ack(acknowledger, response), do: GenServer.cast(acknowledger, response)

  @doc """
  TODO
  """
  def start_link(args) do
    registry = Keyword.fetch!(args, :registry)
    tenant = Keyword.fetch!(args, :tenant)
    namespace = Keyword.fetch!(args, :namespace)
    topic = Keyword.fetch!(args, :topic)

    GenServer.start_link(__MODULE__, args,
      name: via(registry, :"sg_ack_#{tenant}_#{namespace}_#{topic}")
    )
  end

  @doc """
  TODO
  """
  @impl GenServer
  def init(_args) do
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
