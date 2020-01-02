defmodule IntegrationTestHandler do
  use Stargate.Receiver.MessageHandler

  def handle_message(%{payload: payload}) do
    handled_payload = String.to_integer(payload)

    Agent.update(
      :stargate_integration_store,
      fn store -> store ++ [(handled_payload + 1)] end
    )

    case rem(handled_payload, 10) == 0 do
      true -> :ack
      false -> :continue
    end
  end
end
