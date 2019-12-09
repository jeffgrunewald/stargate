defmodule Stargate.Receiver.MessageHandler do
  @moduledoc """
  TODO
  """
  @callback init(term()) :: {:ok, term()}

  @callback handle_message(term(), term()) ::
              {:ack, term()}
              | {:continue, term()}

  @callback handle_message(term()) :: :ack | :continue

  @doc """
  TODO
  """
  defmacro __using__(_opts) do
    quote do
      @behaviour Stargate.Receiver.MessageHandler

      def init(init_args) do
        {:ok, init_args}
      end

      def handle_message(message, state) do
        case handle_message(message) do
          :ack -> {:ack, state}
          :continue -> {:continue, state}
        end
      end

      def handle_message(message), do: :ack

      def topic(), do: Process.get(:sg_topic)

      def namespace(), do: Process.get(:sg_namespace)

      def tenant(), do: Process.get(:sg_tenant)

      def persistence(), do: Process.get(:sg_persistence)

      defoverridable Stargate.Receiver.MessageHandler
    end
  end
end
