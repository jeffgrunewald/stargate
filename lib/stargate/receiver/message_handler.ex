defmodule Stargate.Receiver.MessageHandler do
  @moduledoc """
  TODO
  """
  @callback init(term()) :: {:ok, term()}

  @callback handle_messages(term(), term()) ::
              {:ack, term()}
              | {:continue, term()}

  @callback handle_messages(term()) :: :ack | :continue

  @doc """
  TODO
  """
  defmacro __using__(_opts) do
    quote do
      @behaviour Stargate.Receiver.MessageHandler

      def init(args) do
        {:ok, args}
      end

      def handle_messages(messages, state) do
        case handle_messages(messages) do
          :ack -> {:ack, state}
          :continue -> {:continue, state}
        end
      end

      def handle_messages(messages), do: :ack

      def topic(), do: Process.get(:sg_topic)

      def namespace(), do: Process.get(:sg_namespace)

      def tenant(), do: Process.get(:sg_tenant)

      def persistence(), do: Process.get(:sg_persistence)

      defoverridable Stargate.Receiver.MessageHandler
    end
  end
end
