defmodule Stargate.Producer do
  @moduledoc """
  TODO
  """
  require Logger
  use WebSockex

  def produce(url, message) when is_binary(url) do
    payload = construct_payload(message)

    {:ok, temp_producer} = WebSockex.start(url, __MODULE__, %{})

    Stargate.Producer.produce(temp_producer, payload)

    Process.exit(temp_producer, :shutdown)
  end

  def produce(conn, message) when is_pid(conn) do
    payload = construct_payload(message)

    WebSockex.send_frame(conn, {:text, payload})
  end

  def start_link(opts) do
    url = Keyword.fetch!(opts, :url)

    state = %{url: url}

    WebSockex.start_link(url, __MODULE__, state)
  end

  def handle_connect(_conn, state) do
    :timer.send_interval(30_000, :send_ping)
    {:ok, state}
  end

  def handle_frame({:text, msg}, state) do
    Logger.debug("Received a message : #{inspect(msg)}")
    {:ok, state}
  end

  def handle_info(:send_ping, state) do
    {:reply, :ping, state}
  end

  def handle_ping(_ping_frame, state) do
    {:reply, :pong, state}
  end

  def handle_pong(_pong_frame, state) do
    {:ok, state}
  end

  defp construct_payload(message) do
    %{
      "payload" => Base.encode64(message)
    }
    |> Jason.encode!()
  end
end
