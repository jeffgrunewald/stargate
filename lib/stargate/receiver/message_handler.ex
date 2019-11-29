defmodule Stargate.Receiver.MessageHandler do
  @moduledoc """
  TODO
  """
  @callback init(term()) :: {:ok, term()}

  @callback handle_messages(term(), term()) ::
  {:ack, term()}
  | {:continue, term()}
  | {:noack, term()}

  @callback handle_messages(term()) :: :ack | :continue | :noack

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
          {:ack, id} -> {:ack, id, state}
          :continue -> {:continue, state}
          :noack -> {noack, state}
        end
      end

      def handle_messages(messages), do: :ack
    end
  end
end
