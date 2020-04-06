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

  @doc """
  The Connection using macro provides the common websocket connection
  and keepalive functionality into a single line for replicating connection
  and ping/pong handling in a single place.
  """
  defmacro __using__(_opts) do
    quote do
      use WebSockex

      @impl WebSockex
      def handle_connect(_conn, state) do
        :timer.send_interval(30_000, :send_ping)
        {:ok, state}
      end

      @impl WebSockex
      def handle_info(:send_ping, state) do
        {:reply, :ping, state}
      end

      @impl WebSockex
      def handle_ping(_ping_frame, state) do
        {:reply, :pong, state}
      end

      @impl WebSockex
      def handle_pong(_pong_frame, state) do
        {:ok, state}
      end
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
    |> Keyword.take([:cacerts, :auth_token])
    |> Enum.flat_map(&transform_auth/1)
  end

  defp transform_auth({:cacerts, _} = cacerts), do: [cacerts, insecure: false]

  defp transform_auth({:auth_token, token}) do
    [{:extra_headers, [{"Authorization", "Bearer " <> token}]}]
  end

  defp format_host([{host, port}]), do: "#{host}:#{port}"
  defp format_host({host, port}), do: "#{host}:#{port}"
  defp format_host(connection) when is_binary(connection), do: connection
end
