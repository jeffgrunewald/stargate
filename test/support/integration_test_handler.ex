defmodule IntegrationTestHandler do
  use Stargate.Receiver.MessageHandler

  def handle_message(%{payload: payload}) do
    handled_payload = String.to_integer(payload) + 1

    Agent.update(
      :integration_store,
      fn store -> store ++ [handled_payload] end
    )

    # case rem(handled_payload, 10) == 0 do
    #   true -> :ack
    #   false -> :continue
    # end
    :ack
  end
end
