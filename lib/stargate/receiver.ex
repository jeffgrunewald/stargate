defmodule Stargate.Receiver do
  @moduledoc """
  TODO
  """
  use Stargate.Connection
  import Stargate.Supervisor, only: [via: 2]
  alias Stargate.{Consumer, Reader}
  alias Stargate.Receiver.Dispatcher

  @type message_id :: String.t()

  @doc """
  TODO
  """
  @spec ack(GenServer.server(), message_id()) :: :ok | {:error, term()}
  def ack(receiver, message_id) do
    ack = construct_response(message_id)

    WebSockex.send_frame(receiver, {:text, ack})
  end

  @doc """
  TODO
  """
  @spec pull_permit(GenServer.server(), non_neg_integer()) :: :ok | {:error, term()}
  def pull_permit(receiver, count) do
    permit = construct_permit(count)

    WebSockex.send_frame(receiver, {:text, permit})
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
    protocol: "ws",                optional \\ ws
    persistence: "persistent",     optional \\ persistent
    tenant: "public",
    namespace: "default",
    topic: "foo",
    processors: 3,                 optional \\ 1
    handler: MyApp.Reader.Handler,
    handler_init_args: []          optional \\ []
    query_params: %{               optional
      reader_name: "myapp-reader,
      queue_size: 1_000,             \\ 1_000
      starting_message: :latest      \\ :latest
    }
  ]
  """

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args) do
    type = Keyword.fetch!(args, :type)
    registry = Keyword.fetch!(args, :registry)
    query_params_config = Keyword.get(args, :query_params)

    query_params =
      case type do
        :consumer -> Consumer.QueryParams.build_params(query_params_config)
        :reader -> Reader.QueryParams.build_params(query_params_config)
      end

    setup_state = %{
      registry: registry,
      query_params: query_params_config
    }

    state =
      args
      |> Stargate.Connection.connection_settings(type, query_params)
      |> Map.merge(setup_state)
      |> (fn fields -> struct(State, fields) end).()

    WebSockex.start_link(state.url, __MODULE__, state,
      name: via(state.registry, :"sg_#{type}_#{state.tenant}_#{state.namespace}_#{state.topic}")
    )
  end

  @impl WebSockex
  def handle_frame({:text, msg}, %{tenant: tenant, namespace: ns, topic: topic} = state) do
    :ok = Dispatcher.push(:"sg_dispatcher_#{tenant}_#{ns}_#{topic}", msg)

    {:ok, state}
  end

  defp construct_response(id), do: "{\"messageId\":\"#{id}\"}"

  defp construct_permit(count), do: "{\"type\":\"permit\",\"permitMessages\":#{count}}"
end
