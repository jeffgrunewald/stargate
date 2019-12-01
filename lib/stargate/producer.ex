defmodule Stargate.Producer do
  @moduledoc """
  TODO
  """
  require Logger
  use Stargate.Connection
  use Puid
  import Stargate.Supervisor, only: [via: 2]
  alias Stargate.Producer.{Acknowledger, QueryParams}

  @type producer :: GenServer.server()
  @type message ::
          String.t()
          | {String.t(), String.t()}
          | %{
              required(:payload) => String.t(),
              optional(:key) => String.t(),
              optional(:context) => String.t(),
              optional(:properties) => map(),
              optional(:replicationClusters) => [String.t()]
            }

  @doc """
  TODO
  """
  @spec produce(String.t(), message()) :: :ok | {:error, term()}
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
  @spec produce(producer(), [message()]) :: :ok | {:error, term()}
  def produce(producer, messages) when is_list(messages) do
    Enum.each(messages, &produce(producer, &1))
  end

  @doc """
  TODO
  """
  @spec produce(producer(), message()) :: :ok | {:error, term()}
  def produce(producer, message) do
    {payload, ctx} = construct_payload(message)

    WebSockex.cast(producer, {:send, payload, ctx, self()})

    receive do
      :ack -> :ok
      err -> err
    end
  end

  @doc """
  TODO
  """
  @spec produce(producer(), [message()], {module(), atom(), [term()]}) :: :ok | {:error, term()}
  def produce(producer, messages, mfa) when is_list(messages) do
    Enum.each(messages, &produce(producer, &1, mfa))
  end

  @doc """
  TODO
  """
  @spec produce(producer(), message(), {module(), atom(), [term()]}) :: :ok | {:error, term()}
  def produce(producer, message, mfa) do
    {payload, ctx} = construct_payload(message)

    WebSockex.cast(producer, {:send, payload, ctx, mfa})
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
    registry = Keyword.fetch!(args, :registry)

    state =
      args
      |> Stargate.Connection.connection_settings(:producer, query_params)
      |> Map.put(:query_params, query_params_config)
      |> Map.put(:registry, registry)
      |> (fn fields -> struct(State, fields) end).()

    WebSockex.start_link(state.url, __MODULE__, state,
      name: via(state.registry, :"sg_prod_#{state.tenant}_#{state.namespace}_#{state.topic}")
    )
  end

  @impl WebSockex
  def handle_cast({:send, payload, ctx, ack}, state) do
    Acknowledger.produce(
      via(state.registry, :"sg_ack_#{state.tenant}_#{state.namespace}_#{state.topic}"),
      ctx,
      ack
    )

    {:reply, {:text, payload}, state}
  end

  @impl WebSockex
  def handle_frame({:text, msg}, state) do
    Logger.debug("Received response : #{inspect(msg)}")

    response =
      msg
      |> Jason.decode!()
      |> format_response()

    :ok =
      state.registry
      |> via(:"sg_ack_#{state.tenant}_#{state.namespace}_#{state.topic}")
      |> Acknowledger.ack(response)

    {:ok, state}
  end

  defp construct_payload(%{payload: _payload, context: context} = message) do
    encoded_message =
      message
      |> Map.update!("payload", &Base.encode64(&1))
      |> Jason.encode!()

    {encoded_message, context}
  end

  defp construct_payload(%{payload: _payload} = message) do
    context = generate()

    encoded_message =
      message
      |> Map.put("context", context)
      |> Map.update!("payload", &Base.encode64(&1))
      |> Jason.encode!()

    {encoded_message, context}
  end

  defp construct_payload({key, payload}) do
    context = generate()

    encoded_message =
      %{
        "key" => key,
        "payload" => Base.encode64(payload),
        "context" => context
      }
      |> Jason.encode!()

    {encoded_message, context}
  end

  defp construct_payload(message) when is_binary(message) do
    context = generate()

    encoded_message =
      %{
        "payload" => Base.encode64(message),
        "context" => context
      }
      |> Jason.encode!()

    {encoded_message, context}
  end

  defp format_response(%{"result" => "ok", "context" => ctx}) do
    {:ack, ctx}
  end

  defp format_response(%{"result" => error, "errorMsg" => explanation, "context" => ctx}) do
    reason = "Error of type : #{error} ocurred; #{explanation}"

    {:error, reason, ctx}
  end
end
