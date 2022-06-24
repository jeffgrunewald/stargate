defmodule Stargate.Connection do
  @moduledoc """
  Connection provides the core abstraction for the websocket connection
  between the client application and the Pulsar cluster shared by all
  Stargate producer, reader, and consumer processes.
  """

  @type connection_settings :: %{
          url: String.t(),
          host: String.t(),
          protocol: String.t(),
          persistence: String.t(),
          tenant: String.t(),
          namespace: String.t(),
          topic: String.t()
        }

  @typedoc """
  Calculates custom connection backoff when unable to connect to Pulsar.
  Can be used to implement exponential backoff.
  Note that for an MFA tuple, the arity of the function should be `length(args) + 1` since the first
  arg will be the attempt.
  """
  @type backoff_calculator ::
          (attempt :: non_neg_integer() -> timeout()) | {module(), fun :: atom(), args :: list()}

  @callback handle_send_error(reason :: term, ref :: term, state :: term) :: :ok
  @callback handle_connected(state :: term) :: :ok

  @doc """
  The Connection using macro provides the common websocket connection
  and keepalive functionality into a single line for replicating connection
  and ping/pong handling in a single place.
  """
  defmacro __using__(_opts) do
    quote do
      use WebSockex

      @behaviour Stargate.Connection
      @ping_interval 30_000

      require Logger

      @impl WebSockex
      def handle_connect(_conn, state) do
        tref = Process.send_after(self(), :send_ping, @ping_interval)
        :ok = apply(__MODULE__, :handle_connected, [state])
        {:ok, Map.put(state, :ping_timer_ref, tref)}
      end

      @impl WebSockex
      def handle_info(:send_ping, state) do
        tref = Process.send_after(self(), :send_ping, @ping_interval)
        {:reply, :ping, Map.put(state, :ping_timer_ref, tref)}
      end

      @impl WebSockex
      def handle_ping(_ping_frame, state) do
        {:reply, :pong, state}
      end

      @impl WebSockex
      def handle_pong(_pong_frame, state) do
        {:ok, state}
      end

      @impl WebSockex
      def handle_disconnect(
            %{reason: reason, attempt_number: attempt},
            %{backoff_calculator: backoff_calculator} = state
          ) do
        case Map.get(state, :ping_timer_ref) do
          nil ->
            :ok

          ref ->
            Process.cancel_timer(ref)
            :ok
        end

        backoff = Stargate.Connection.calculate_backoff(backoff_calculator, attempt)

        Logger.warning(
          "[Stargate] disconnected",
          reason: inspect(reason),
          url: state.url,
          attempt: attempt,
          backoff_ms: backoff
        )

        {:backoff, backoff, state}
      end

      @impl WebSockex
      def handle_send_result(:ok, _frame, _ref, state) do
        {:ok, state}
      end

      @impl WebSockex
      def handle_send_result({:error, reason}, :ping, _ref, state) do
        Logger.warning("[Stargate] error sending ping", reason: inspect(reason))
        {:close, state}
      end

      @impl WebSockex
      def handle_send_result({:error, %WebSockex.ConnError{} = reason}, frame, ref, state) do
        Logger.warning("[Stargate] error sending message", reason: inspect(reason), url: state.url)

        :ok = apply(__MODULE__, :handle_send_error, [:not_connected, ref, state])
        {:close, state}
      end

      @impl WebSockex
      def handle_send_result({:error, %WebSockex.NotConnectedError{} = reason}, frame, ref, state) do
        Logger.warning("[Stargate] error sending message", reason: inspect(reason), url: state.url)

        :ok = apply(__MODULE__, :handle_send_error, [:not_connected, ref, state])
        {:ok, state}
      end

      @impl WebSockex
      def handle_send_result({:error, reason}, frame, ref, state) do
        Logger.warning("[Stargate] error sending message", reason: inspect(reason), url: state.url)

        :ok = apply(__MODULE__, :handle_send_error, [:unknown, ref, state])
        {:ok, state}
      end

      @impl WebSockex
      def terminate({reason, stacktrace}, state) when is_exception(reason) do
        Logger.error(Exception.format(:error, reason, stacktrace))
      end

      @impl WebSockex
      def terminate(_reason, _state) do
        :ok
      end

      @impl Stargate.Connection
      def handle_send_error(reason, ref, state) do
        :ok
      end

      @impl Stargate.Connection
      def handle_connected(state) do
        :ok
      end

      defoverridable handle_send_error: 3,
                     handle_connected: 1
    end
  end

  @doc """
  Parses the keyword list configuration passed to a websocket and constructs
  the url needed to establish a connection to the Pulsar cluster. If query
  params are provided, appends the connection-specific params string to the url
  and returns the full result as a map to pass to the connection `start_link/1` function.
  """
  @spec connection_settings(keyword(), atom(), String.t()) :: connection_settings()
  def connection_settings(opts, type, params) do
    host = Keyword.fetch!(opts, :host) |> format_host()
    protocol = Keyword.get(opts, :protocol, "ws")
    persistence = Keyword.get(opts, :persistence, "persistent")
    tenant = Keyword.fetch!(opts, :tenant)
    namespace = Keyword.fetch!(opts, :namespace)
    topic = Keyword.fetch!(opts, :topic)

    subscription =
      case type do
        :consumer -> "/#{Keyword.fetch!(opts, :subscription)}"
        _ -> ""
      end

    base_url =
      "#{protocol}://#{host}/ws/v2/#{type}/#{persistence}/#{tenant}/#{namespace}/#{topic}#{
        subscription
      }"

    url =
      case params do
        "" -> base_url
        _ -> base_url <> "?" <> params
      end

    %{
      url: url,
      host: host,
      protocol: protocol,
      persistence: persistence,
      tenant: tenant,
      namespace: namespace,
      topic: topic
    }
  end

  @doc """
  Parses the keyword list configuration passed to a websocket and constructs the
  authentication options, either SSL or token-based, to be passed to the websocket
  process.
  """
  @spec auth_settings(keyword()) :: keyword()
  def auth_settings(opts) do
    opts
    |> Keyword.take([:ssl_options, :auth_token])
    |> Enum.map(&transform_auth/1)
  end

  @doc false
  @spec calculate_backoff(attempt :: non_neg_integer()) :: timeout()
  def calculate_backoff(_attempt) do
    2_000
  end

  @doc false
  @spec calculate_backoff(backoff_calculator(), attempt :: non_neg_integer()) :: timeout()
  def calculate_backoff(calc, attempt) do
    case calc do
      {module, function, args} ->
        apply(module, function, [attempt | args])

      fun when is_function(fun, 1) ->
        fun.(attempt)

      _ ->
        raise ArgumentError,
              "Backoff calculator does not conform to spec of {module, function, args} or fun/1"
    end
  end

  @doc false
  def default_backoff_calculator() do
    {Stargate.Connection, :calculate_backoff, []}
  end

  defp transform_auth({:ssl_options, _opts} = ssl_opts), do: ssl_opts

  defp transform_auth({:auth_token, token}) do
    {:extra_headers, [{"Authorization", "Bearer " <> token}]}
  end

  defp format_host([{host, port}]), do: "#{host}:#{port}"
  defp format_host({host, port}), do: "#{host}:#{port}"
  defp format_host(connection) when is_binary(connection), do: connection
end
