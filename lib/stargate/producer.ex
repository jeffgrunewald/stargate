defmodule Stargate.Producer do
  @moduledoc """
  TODO
  """
  require Logger
  use Stargate.Connection

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
  def start_link(opts) do
    query_params_config = Keyword.get(opts, :query_params)
    query_params = Stargate.Producer.QueryParams.build_params(query_params_config)

    state =
      opts
      |> Stargate.Connection.connection_settings("producer", query_params)
      |> Map.put(:query_params, query_params_config)
      |> (fn fields -> struct(State, fields) end).()

    WebSockex.start_link(state.url, __MODULE__, state, name: __MODULE__)
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

  defp format_response(%{"result" => "ok", "context" => ctx}) do
    {:ack, ctx}
  end

  defp format_response(%{"result" => error, "errorMsg" => explanation, "context" => ctx}) do
    reason = "Error of type : #{error} ocurred; #{explanation}"

    {:error, reason, ctx}
  end

  defp forward(response), do: GenServer.cast(Stargate.Producer.Acknowledger, response)
end
