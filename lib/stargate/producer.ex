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

  defmodule State do
    @moduledoc """
    TODO
    """
    defstruct [
      :url,
      :host,
      :protocol,
      :persistence,
      :tenant,
      :namespace,
      :topic,
      :acknowledger,
      :query_params
    ]
  end

  @doc """
  config = [
    host: [localhost: 8080],
    protocol: "ws",                               optional \\ ws
    persistence: "persistent",                    optional \\ persistent
    tenant: "public",
    namespace: "default",
    topic: "foo",
    acknowledger: MyApp.Producer.Acknowledger,    optional
    query_params: %{                              optional
      send_timeout: 30_000,                         \\ 30_000
      batch_enabled: true,                          \\ false
      batch_max_msg: 1_000,                         \\ 1_000
      max_pending_msg: 1_000,                       \\ 1_000
      batch_max_delay: 25,                          \\ 10
      routing_mode: :round_robin,                   \\ :round_robin | :single
      compression_type: :lz4,                       \\ :lz4 | :zlib
      producer_name: "myapp-producer",
      initial_seq_id: 100,
      hashing_scheme: :murmur3                      \\ :java_string | :murmur3
    }
  ]
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    acknowledger = Keyword.get(opts, :acknowledger)
    query_params_config = Keyword.get(opts, :query_params)
    query_params = Stargate.Producer.QueryParams.build_params(query_params_config)

    state =
      opts
      |> Stargate.Connection.connection_settings("producer", query_params)
      |> Map.put(:acknowledger, acknowledger)
      |> Map.put(:query_params, query_params_config)
      |> (fn fields -> struct(State, fields) end).()

    WebSockex.start_link(state.url, __MODULE__, state)
  end

  @impl WebSockex
  def handle_frame({:text, msg}, %{acknowledger: acknowledger} = state)
      when is_atom(acknowledger) and acknowledger != nil do
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
