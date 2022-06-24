defmodule MockSocket.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(init_args) do
    Supervisor.start_link(__MODULE__, init_args)
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

  def websocket_handle({:text, "stop"}, state) do
    {:stop, state}
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

    handle_message(pid, received, state)
  end

  def websocket_handle(:ping, %{source: pid} = state) do
    send(pid, :pong_from_socket)

    {:reply, :pong, state}
  end

  def websocket_info(_, state) do
    {:ok, state}
  end

  defp handle_message(
         pid,
         %{"context" => ctx, "properties" => %{"error" => reason}} = received,
         state
       ) do
    response = "#{ctx}, #{Jason.encode!(received)}"
    message_id = Map.get(received, "messageId")
    send(pid, {:received_frame, "#{response} loud and clear"})

    {:reply,
     {:text,
      Jason.encode!(%{
        "result" => "error",
        "errorMsg" => reason,
        "messageId" => message_id,
        "context" => "#{ctx}"
      })}, state}
  end

  defp handle_message(pid, %{"context" => ctx} = received, state) do
    response = "#{ctx}, #{Jason.encode!(received)}"
    message_id = Map.get(received, "messageId")
    send(pid, {:received_frame, "#{response} loud and clear"})

    {:reply,
     {:text,
      Jason.encode!(%{"result" => "ok", "messageId" => message_id, "context" => "#{ctx}"})},
     state}
  end

  defp handle_message(pid, %{"messageId" => message_id}, state) do
    send(pid, {:received_frame, "#{message_id} loud and clear"})
    {:ok, state}
  end
end

defmodule SampleClient do
  use Stargate.Connection

  def cast(message), do: WebSockex.cast(__MODULE__, {:send, message})
  def cast(message, ref), do: WebSockex.cast(__MODULE__, {:send, message, ref})
  def ping_socket, do: send(__MODULE__, :send_ping)

  def start_link(init_args) do
    port = Keyword.get(init_args, :port)
    path = Keyword.get(init_args, :path)
    test_pid = self()

    backoff_calculator =
      Keyword.get(
        init_args,
        :backoff_calculator,
        Stargate.Connection.default_backoff_calculator()
      )

    url = "http://localhost:#{port}/#{path}"

    state = %{
      backoff_calculator: backoff_calculator,
      test_pid: test_pid,
      url: url
    }

    WebSockex.start_link(url, __MODULE__, state, name: __MODULE__)
  end

  def handle_cast({:send, payload}, state) do
    {:reply, {:text, payload}, state}
  end

  def handle_cast({:send, payload, ref}, state) do
    {:reply, {:text, payload}, ref, state}
  end

  def handle_frame({:text, _message}, state) do
    {:ok, state}
  end

  @impl Stargate.Connection
  def handle_connected(%{test_pid: pid}) do
    send(pid, :client_connected)
    :ok
  end

  @impl Stargate.Connection
  def handle_send_error(reason, ref, %{test_pid: pid}) do
    send(pid, {:received_send_error, reason, ref})
    :ok
  end
end
