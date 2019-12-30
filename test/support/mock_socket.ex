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
      {Plug.Cowboy, [
          scheme: :http,
          plug: MockSocket.Router,
          options: [
            port: port,
            dispatch: [{:_,
                        [
                          {"/#{path}", MockSocket, source}
                        ]
                       }],
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
    state = %{source: source}

    {:ok, state}
  end

  def websocket_handle({:text, message}, %{source: pid} = state) do
    received = "#{message} loud and clear"
    send(pid, {:received_frame, received})

    %{"context" => ctx} =
      try do
        Jason.decode!(message)
      rescue
        _ -> %{"messageId" => "message_id", "context" => "123"}
      end

    {:reply, {:text, "{\"result\":\"ok\",\"messageId\":\"test-id\",\"context\":\"#{ctx}\"}"}, state}
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
