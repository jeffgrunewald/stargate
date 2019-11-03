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
    handler = Keyword.fetch!(opts, :handler)

    state =
      opts
      |> Stargate.Connection.connection_settings("reader", "")
      |> Map.put(:handler, handler)

    WebSockex.start_link(state.url, __MODULE__, state)
  end

  @impl WebSockex
  def handle_frame({:text, msg}, state) do
    msg
    |> Jason.decode!()
    |> Stargate.Message.new(state.persistence, state.tenant, state.namespace, state.topic)
    |> state.handler.handle_messages()
    |> case do
      {:ack, id} ->
        ack = construct_response(id)
        WebSockex.send_frame(self(), {:text, ack})

      :continue ->
        :continue
    end

    {:ok, state}
  end

  defp construct_response(id) do
    %{"messageId" => id}
    |> Jason.encode!()
  end
end
