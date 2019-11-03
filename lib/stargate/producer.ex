defmodule Stargate.Producer do
  @moduledoc """
  TODO
  """
  require Logger
  use Stargate.Connection

  @doc """
  TODO
  """
  @spec produce(String.t(), String.t()) :: :ok | {:error, term()}
  def produce(url, message) when is_binary(url) and is_binary(message) do
    payload = construct_payload(message)

    with {:ok, temp_producer} <- WebSockex.start(url, __MODULE__, %{}),
         :ok <- produce(temp_producer, payload) do
      Process.exit(temp_producer, :shutdown)
      :ok
    else
      {:error, reason} -> {:error, reason}
      error -> {:error, error}
    end
  end

  @doc """
  TODO
  """
  @spec produce(pid(), String.t()) :: :ok | {:error, term()}
  def produce(conn, message) when is_pid(conn) and is_binary(message) do
    payload = construct_payload(message)

    WebSockex.send_frame(conn, {:text, payload})
  end

  @doc """
  TODO
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    acknowledger = Keyword.get(opts, :acknowledger)

    state =
      opts
      |> Stargate.Connection.connection_settings("producer")
      |> Map.put(:acknowledger, acknowledger)

    WebSockex.start_link(state.url, __MODULE__, state)
  end

  @impl WebSockex
  def handle_frame({:text, msg}, %{acknowledger: acknowledger} = state)
      when is_atom(acknowledger) do
    Logger.debug("Received response : #{inspect(msg)}")

    msg
    |> Jason.decode!()
    |> format_response()
    |> acknowledger.produce_ack()

    {:ok, state}
  end

  @impl WebSockex
  def handle_frame({:text, msg}, state) do
    Logger.debug("Received response : #{inspect(msg)}")

    {:ok, state}
  end

  defp construct_payload(message) do
    %{
      "payload" => Base.encode64(message)
    }
    |> Jason.encode!()
  end

  defp format_response(%{"result" => "ok", "messageId" => id} = response) do
    case Map.get(response, "context") do
      nil -> {:ok, id}
      context -> {:ok, id, context}
    end
  end

  defp format_response(%{"result" => error, "errorMsg" => explanation} = response) do
    reason = "Error of type : #{error} ocurred; #{explanation}"

    case Map.get(response, "context") do
      nil -> {:error, reason}
      context -> {:error, reason, context}
    end
  end
end
