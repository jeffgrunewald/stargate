defmodule Stargate.Reader do
  @moduledoc """
  TODO
  """
  use Stargate.Connection

  @type message_id :: String.t()

  @doc """
  TODO
  """
  @spec ack(GenServer.server(), message_id()) :: :ok | {:error, term()}
  def ack(receiver, message_id) do
    ack = construct_response(message_id)

    WebSockex.send_frame(receiver, {:text, ack})
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
      :handler,
      :handler_init_args,
      :query_params
    ]
  end

  @doc """
  config = [
    host: [localhost: 8080],
    protocol: "ws",                 optional \\ ws
    persistence: "persistent",      optional \\ persistent
    tenant: "public",
    namespace: "default",
    topic: "foo",
    workers: 3,                     optional \\ 1
    handler: MyApp.Reader.Handler,
    handler_init_args: []          optional \\ []
    query_params: %{                optional
      reader_name: "myapp-reader,
      queue_size: 1_000,              \\ 1_000
      starting_message: :latest       \\ :latest
    }
  ]
  """

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args) do
    registry = Keyword.fetch!(args, :registry)
    query_params_config = Keyword.get(args, :query_params)
    query_params = Stargate.Reader.QueryParams.build_params(query_params_config)

    state =
      args
      |> Stargate.Connection.connection_settings("reader", query_params)
      |> Map.put(:query_params, query_params_config)
      |> Map.put(:registry, registry)
      |> (fn fields -> struct(State, fields) end).()

    WebSockex.start_link(state.url, __MODULE__, state,
      name: via(state.registry, :"sg_read_#{state.tenant}_#{state.namespace}_#{state.topic}")
    )
  end

  @impl WebSockex
  def handle_frame({:text, msg}, state) do
    decoded_message =
      msg
      |> Jason.decode!()
      |> Stargate.Message.new(state.persistence, state.tenant, state.namespace, state.topic)

    case state.handler.handle_messages(decoded_message) do
      :ack ->
        ack = construct_response(decoded_message.message_id)
        WebSockex.send_frame(self(), {:text, ack})

      :continue ->
        :continue
    end

    {:ok, state}
  end

  defp construct_response(id), do: "{\"messageId\":\"#{id}\"}"
end
