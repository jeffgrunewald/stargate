defmodule Stargate.Connection do
  @moduledoc """
  TODO
  """

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
end
