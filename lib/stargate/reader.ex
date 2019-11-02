defmodule Stargate.Reader do
  @moduledoc """
  TODO
  """
  use Stargate.Connection

  @doc """
  TODO
  """

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    state = Stargate.Connection.connection_settings(opts, "reader")

    WebSockex.start_link(state.url, __MODULE__, state)
  end

  @impl WebSockex
  def handle_frame({:text, msg}, state) do
    msg
    |> Jason.decode!()
    |> Stargate.Message.new(state.persistence, state.tenant, state.namespace, state.topic)
    |> IO.inspect()

    {:ok, state}
  end
end
