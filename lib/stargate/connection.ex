defmodule Stargate.Connection do
  @moduledoc """
  TODO
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
  TODO
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
  TODO
  """
  @spec connection_settings(keyword(), atom(), String.t()) :: connection_settings()
  def connection_settings(opts, type, params) do
    host = Keyword.fetch!(opts, :host) |> format_host()
    protocol = Keyword.get(opts, :protocol, "ws")
    persistence = Keyword.get(opts, :persistence, "persistent")
    tenant = Keyword.fetch!(opts, :tenant)
    namespace = Keyword.fetch!(opts, :namespace)
    topic = Keyword.fetch!(opts, :topic)

    base_url =
      "#{protocol}://#{host}/ws/v2/#{type}/#{persistence}/#{tenant}/#{namespace}/#{topic}"

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

  defp format_host([{host, port}]), do: "#{host}:#{port}"
  defp format_host({host, port}), do: "#{host}:#{port}"
  defp format_host(connection) when is_binary(connection), do: connection
end
