defmodule Stargate.Reader do
  @moduledoc """
  TODO
  """
  use Stargate.Connection

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
    handler: MyApp.Reader.Handler,
    query_params: %{                optional
      reader_name: "myapp-reader,
      queue_size: 1_000,              \\ 1_000
      starting_message: :latest       \\ :latest
    }
  ]
  """

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    handler = Keyword.fetch!(opts, :handler)
    query_params_config = Keyword.get(opts, :query_params)
    query_params = Stargate.Reader.QueryParams.build_params(query_params_config)

    state =
      opts
      |> Stargate.Connection.connection_settings("reader", query_params)
      |> Map.put(:handler, handler)
      |> Map.put(:query_params, query_params_config)
      |> (fn fields -> struct(State, fields) end).()

    WebSockex.start_link(state.url, __MODULE__, state)
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
