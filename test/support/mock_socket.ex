defmodule MockSocket.Supervisor do
  use Supervisor

  def start_link(init_args) do
    Supervisor.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(init_args) do
    port = Keyword.get(init_args, :port)
    path = Keyword.get(init_args, :path)
    source = Keyword.get(init_args, :source)

    [
      {Plug.Cowboy,
       [
         scheme: :http,
         plug: MockSocket.Router,
         options: [
           port: port,
           dispatch: [
             {:_,
              [
                {"/#{path}", MockSocket, source}
              ]}
           ],
           protocol_options: [{:idle_timeout, 60_000}]
         ]
       ]}
    ]
    |> Supervisor.init(strategy: :one_for_one)
  end
end

defmodule MockSocket.Router do
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  match _ do
    send_resp(conn, 404, "not found")
  end
end

defmodule MockSocket do
  @behaviour :cowboy_websocket

  require Logger

  def init(req, opts) do
    {:cowboy_websocket, req, opts}
  end

  def websocket_init(source) do
    state = %{source: source, count: 0}

    {:ok, state}
  end

  def websocket_handle({:text, "push_message"}, %{count: count} = state) do
    message = "consumer message #{count}"
    {:reply, {:text, message}, %{state | count: count + 1}}
  end

  def websocket_handle(
        {:text, "{\"type\":\"permit" <> _permits = message},
        %{source: pid} = state
      ) do
    request = Jason.decode!(message)
    send(pid, {:permit_request, "permitting #{request["permitMessages"]} messages"})
    {:ok, state}
  end

  def websocket_handle({:text, message}, %{source: pid} = state) do
    received =
      try do
        Jason.decode!(message)
      rescue
        _ -> %{"messageId" => "message_id", "context" => "123"}
      end

    case Map.get(received, "context") do
      nil ->
        response = Map.get(received, "messageId")
        send(pid, {:received_frame, "#{response} loud and clear"})
        {:ok, state}

      ctx when is_binary(ctx) ->
        id = Map.get(received, "messageId", "message_id")
        response = "#{ctx}, #{Jason.encode!(received)}"
        send(pid, {:received_frame, "#{response} loud and clear"})

        {:reply,
         {:text,
          Jason.encode!(%{"result" => "ok", "messageId" => "#{id}", "context" => "#{ctx}"})},
         state}
    end
  end

  def websocket_handle(:ping, %{source: pid} = state) do
    send(pid, :pong_from_socket)

    {:reply, :pong, state}
  end

  def websocket_info(_, state) do
    {:ok, state}
  end
end

defmodule SampleClient do
  use Stargate.Connection

  def cast(message), do: WebSockex.cast(__MODULE__, {:send, message})
  def ping_socket(), do: send(__MODULE__, :send_ping)

  def start_link(init_args) do
    port = Keyword.get(init_args, :port)
    path = Keyword.get(init_args, :path)
    WebSockex.start_link("http://localhost:#{port}/#{path}", __MODULE__, %{}, name: __MODULE__)
  end

  def handle_cast({:send, payload}, state) do
    {:reply, {:text, payload}, state}
  end

  def handle_frame({:text, _message}, state) do
    {:ok, state}
  end
end
