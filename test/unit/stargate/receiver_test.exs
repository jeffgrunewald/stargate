defmodule Stargate.ReceiverTest do
  use ExUnit.Case

  setup do
    reg_name = :sg_reg_receiver_test
    tenant = "default"
    ns = "public"
    topic = "consumer-test"
    port = Enum.random(49152..65535)
    path = "ws/v2/consumer/persistent/#{tenant}/#{ns}/#{topic}"
    opts = [
      host: [localhost: port],
      registry: reg_name,
      type: :consumer,
      tenant: tenant,
      namespace: ns,
      topic: topic
    ]

    {:ok, registry} = Registry.start_link(keys: :unique, name: reg_name)
    {:ok, server} = MockSocket.Supervisor.start_link(port: port, path: path, source: self())
    {:ok, receiver} = Stargate.Receiver.start_link(opts)
    {:ok, dispatcher} = Stargate.Receiver.Dispatcher.start_link(opts)
    {:ok, consumer} = MockConsumer.start_link(producer: dispatcher, source: self())

    on_exit(fn ->
      Enum.map([registry, server, receiver, dispatcher, consumer], &kill/1)
    end)

    [server: server, receiver: receiver]
  end

  describe "handle_frame" do
    test "receives messages from the socket", %{receiver: receiver} do
      Enum.each(0..2, fn _ -> WebSockex.send_frame(receiver, {:text, "push_message"}) end)

      assert_receive {:event_received, ["consumer message 0"]}
      assert_receive {:event_received, ["consumer message 1"]}
      assert_receive {:event_received, ["consumer message 2"]}
    end
  end

  describe "ack/2" do
    test "sends receive acks to the socket", %{receiver: receiver} do
      Enum.map(["ack1", "ack2", "ack3"], &Stargate.Receiver.ack(receiver, &1))

      assert_receive {:received_frame, "ack1 loud and clear"}
      assert_receive {:received_frame, "ack2 loud and clear"}
      assert_receive {:received_frame, "ack3 loud and clear"}
    end
  end

  describe "pull_permit/2" do
    test "sends permit requests through the socket", %{receiver: receiver} do
      Stargate.Receiver.pull_permit(receiver, 10)
      assert_receive {:permit_request, "permitting 10 messages"}

      Stargate.Receiver.pull_permit(receiver, 50)
      assert_receive {:permit_request, "permitting 50 messages"}

      Stargate.Receiver.pull_permit(receiver, 100)
      assert_receive {:permit_request, "permitting 100 messages"}
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
