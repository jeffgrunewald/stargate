defmodule TestHandler do
  use Stargate.Receiver.MessageHandler

  def init({source, expected}) do
    {:ok, %{source: source, processed: 0, expected: expected}}
  end

  def handle_message(%{payload: payload}, state) do
    send(state.source, "message #{payload} received")

    processed_count = state.processed + 1

    if processed_count == state.expected,
      do: send(state.source, "all #{processed_count} messages received")

    {:ack, %{state | processed: processed_count}}
  end
end
