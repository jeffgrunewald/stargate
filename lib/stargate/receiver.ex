defmodule Stargate.Receiver do
  @moduledoc """
  TODO
  """
  use Stargate.Connection
  import Stargate.Supervisor, only: [via: 2]
  alias Stargate.{Consumer, Reader}

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

    WebSockex.send_frame(receiver, {:text}, permit)
  end

  @doc """
  TODO
  """
  @spec register_workers(GenServer.server(), [pid()]) :: :ok
  def register_workers(receiver, workers) do
    send(receiver, {:register_workers, workers, self()})

    receive do
      :ok -> :ok
      _ -> raise RuntimeError, message: "Unable to register receiver workers"
    end
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
      :query_params,
      :process_queue_len,
      :process_queue_delay,
      :process_queue,
      :process_queue_ts,
      :workers,
      :worker_index
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
    worker_count: 3,               optional \\ 1
    process_queue_len: 10          optional \\ 10
    process_queue_delay: 5_000     optional \\ 5_000
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
      query_params: query_params_config,
      worker_index: 0,
      process_queue_len: Keyword.get(args, :process_queue_len, 10),
      process_queue_delay: Keyword.get(args, :process_queue_delay, 5_000),
      process_queue: [],
      process_queue_ts: :erlang.system_time()
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
  def handle_frame(
        {:text, msg},
        %{
          process_queue: queue,
          process_queue_len: length,
          process_queue_ts: timestamp,
          process_queue_delay: delay
        } = state
      ) do
    case process_messages?(queue, length, timestamp, delay) do
      {false, false} ->
        {:ok, %{state | process_queue: [msg | queue]}}

      {_, _} ->
        {worker, new_index} = current_and_next_worker(state.workers, state.worker_index)
        Stargate.Receiver.Worker.process(worker, Enum.reverse([msg | queue]))

        {:ok,
         %{
           state
           | process_queue: [],
             worker_index: new_index,
             process_queue_ts: :erlang.system_time()
         }}
    end
  end

  @impl WebSockex
  def handle_info({:register_workers, workers, mgr}, state) do
    send(mgr, :ok)

    {:ok, %{state | workers: workers}}
  end

  defp construct_response(id), do: "{\"messageId\":\"#{id}\"}"

  defp construct_permit(count), do: "{\"type\":\"permit\",\"permitMessages\":#{count}}"

  defp current_and_next_worker(workers, index) do
    {Enum.at(workers, index), rem(index + 1, Enum.count(workers))}
  end

  defp process_messages?(queue, length, timestamp, delay) do
    {Enum.count(queue) + 1 == length, :erlang.system_time() - timestamp >= delay}
  end
end
