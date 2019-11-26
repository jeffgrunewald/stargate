defmodule Stargate.Producer do
  @moduledoc """
  TODO
  """
  require Logger
  use Stargate.Connection
  use Puid
  import Stargate.Supervisor
  alias Stargate.Producer.{Acknowledger, QueryParams}

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
  @spec produce(atom() | pid(), String.t()) :: :ok | {:error, term()}
  def produce(conn, message) do
    ctx = generate()
    payload = construct_payload(message, ctx)
    Acknowledger.ack(Stargate.Producer.Acknowledger, {:produce, ctx, self()})

    WebSockex.send_frame(__MODULE__, {:text, payload})

    receive do
      :ack -> :ok
      err -> err
    end
  end

  @doc """
  TODO
  """
  @spec produce(atom() | pid(), String.t(), {atom(), atom(), [term()]}) :: :ok | {:error, term()}
  def produce(conn, message, mfa) do
    ctx = generate()
    payload = construct_payload(message, ctx)
    Acknowledger.ack(Stargate.Producer.Acknowledger, {:produce, ctx, mfa})

    WebSockex.send_frame(__MODULE__, {:text, payload})
  end

  defmodule State do
    @moduledoc """
    TODO
    """
    defstruct [
      :registry,
      :url,
      :host,
      :protocol,
      :persistence,
      :tenant,
      :namespace,
      :topic,
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
  def start_link(args) do
    query_params_config = Keyword.get(args, :query_params)
    query_params = QueryParams.build_params(query_params_config)

    state =
      args
      |> Stargate.Connection.connection_settings("producer", query_params)
      |> Map.put(:query_params, query_params_config)
      |> (fn fields -> struct(State, fields) end).()

    WebSockex.start_link(state.url, __MODULE__, state,
      name: via(state.registry, :"sg_prod_#{state.tenant}_#{state.namespace}_#{state.topic}")
    )
  end

  @impl WebSockex
  def handle_frame({:text, msg}, state) do
    Logger.debug("Received response : #{inspect(msg)}")

    msg
    |> Jason.decode!()
    |> format_response()
    |> forward()

    {:ok, state}
  end

  defp construct_payload(message, context) do
    %{
      "payload" => Base.encode64(message),
      "context" => context
    }
    |> Jason.encode!()
  end

  defp format_response(%{"result" => "ok", "context" => ctx}) do
    {:ack, ctx}
  end

  defp format_response(%{"result" => error, "errorMsg" => explanation, "context" => ctx}) do
    reason = "Error of type : #{error} ocurred; #{explanation}"

    {:error, reason, ctx}
  end

  defp forward(response), do: Acknowledger.ack(Stargate.Producer.Acknowledger, response)
end
