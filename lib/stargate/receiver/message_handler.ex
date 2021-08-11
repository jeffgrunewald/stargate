defmodule Stargate.Receiver.MessageHandler do
  @moduledoc """
  Defines the `MessageHandler` behaviour required by a module
  passed to a Stargate reader or consumer.

  A message handler must implement a `handle_message/1` for
  stateless message processing, and for stateful processing, a
  `handle_message/2` and an `init/1` function.

  This module also  defines a `__using__` macro to pull default
  implementations of these functions into your module as well as
  getter functions for the topic-aware data stored in the process
  dictionary of the Processor stage calling the message handler.
  """
  @callback init(term()) :: {:ok, term()}

  @callback handle_message(term(), term()) ::
              {:ack, term()}
              | {:continue, term()}

  @callback handle_message(term()) :: :ack | :continue

  @doc """
  Provides a macro for implementing the behaviour in
  the client application's message handler module and automatically
  pull in default implementations of the required functions
  and getters for the data stored in the processor stage's
  process dictionary.
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

      def topic, do: Process.get(:sg_topic)

      def namespace, do: Process.get(:sg_namespace)

      def tenant, do: Process.get(:sg_tenant)

      def persistence, do: Process.get(:sg_persistence)

      defoverridable Stargate.Receiver.MessageHandler
    end
  end
end
