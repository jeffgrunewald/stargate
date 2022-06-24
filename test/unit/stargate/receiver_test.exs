defmodule Stargate.ReceiverTest do
  use ExUnit.Case

  alias Stargate.Receiver.Dispatcher

  setup do
    reg_name = :receiver_test
    type = :consumer
    tenant = "default"
    ns = "public"
    topic = "consumer-test"
    subscription = "receiver1"
    port = Enum.random(49_152..65_535)
    path = "ws/v2/#{type}/persistent/#{tenant}/#{ns}/#{topic}/#{subscription}"

    opts = [
      host: [localhost: port],
      registry: :"sg_reg_#{reg_name}",
      type: type,
      tenant: tenant,
      namespace: ns,
      topic: topic,
      subscription: subscription
    ]

    {:ok, registry} = Registry.start_link(keys: :unique, name: :"sg_reg_#{reg_name}")
    {:ok, server} = MockSocket.Supervisor.start_link(port: port, path: path, source: self())

    receiver = Stargate.registry_key(tenant, ns, topic, component: type, name: reg_name)

    on_exit(fn ->
      Enum.map([registry, server], &kill/1)
    end)

    [opts: opts, receiver: receiver]
  end

  describe "handle_frame" do
    setup %{opts: opts} do
      {:ok, dispatcher} = Dispatcher.start_link(opts)
      {:ok, consumer} = MockConsumer.start_link(producer: dispatcher, source: self())

      on_exit(fn ->
        Enum.map([dispatcher, consumer], &kill/1)
      end)

      []
    end

    test "receives messages from the socket", %{receiver: receiver} do
      Enum.each(0..2, fn _ -> WebSockex.send_frame(receiver, {:text, "push_message"}) end)

      assert_receive {:event_received, ["consumer message 0"]}
      assert_receive {:event_received, ["consumer message 1"]}
      assert_receive {:event_received, ["consumer message 2"]}
    end
  end

  describe "ack/2" do
    setup %{opts: opts} do
      {:ok, dispatcher} = Dispatcher.start_link(opts)
      {:ok, consumer} = MockConsumer.start_link(producer: dispatcher, source: self())

      on_exit(fn ->
        Enum.map([dispatcher, consumer], &kill/1)
      end)

      []
    end

    test "sends receive acks to the socket", %{receiver: receiver} do
      Enum.map(["ack1", "ack2", "ack3"], &Stargate.Receiver.ack(receiver, &1))

      assert_receive {:received_frame, "ack1 loud and clear"}
      assert_receive {:received_frame, "ack2 loud and clear"}
      assert_receive {:received_frame, "ack3 loud and clear"}
    end
  end

  describe "pull mode" do
    setup %{opts: opts} do
      opts =
        Keyword.merge(opts,
          backoff_calculator: fn _ -> 0 end,
          query_params: %{
            pull_mode: true
          }
        )

      {:ok, dispatcher} = Dispatcher.start_link(opts)

      {:ok, consumer} =
        MockConsumer.start_link(producer: dispatcher, source: self(), max_demand: 1)

      on_exit(fn ->
        Enum.map([dispatcher, consumer], &kill/1)
      end)

      []
    end

    test "pull_permit sends permit requests through the socket", %{receiver: receiver} do
      Stargate.Receiver.pull_permit(receiver, 10)
      assert_receive {:permit_request, "permitting 10 messages"}

      Stargate.Receiver.pull_permit(receiver, 50)
      assert_receive {:permit_request, "permitting 50 messages"}

      Stargate.Receiver.pull_permit(receiver, 100)
      assert_receive {:permit_request, "permitting 100 messages"}
    end

    test "continue on reconnect", %{receiver: receiver} do
      assert_receive {:permit_request, "permitting 1 messages"}
      WebSockex.send_frame(receiver, {:text, "push_message"})

      assert_receive {:event_received, ["consumer message 0"]}

      :ok = WebSockex.send_frame(receiver, {:text, "stop"})

      Process.sleep(100)

      assert_receive {:permit_request, "permitting 1 messages"}

      WebSockex.send_frame(receiver, {:text, "push_message"})
      assert_receive {:event_received, ["consumer message 0"]}
    end
  end

  describe "push mode" do
    setup %{opts: opts} do
      opts = Keyword.merge(opts, backoff_calculator: fn _ -> 0 end)
      {:ok, dispatcher} = Dispatcher.start_link(opts)
      {:ok, consumer} = MockConsumer.start_link(producer: dispatcher, source: self())

      on_exit(fn ->
        Enum.map([dispatcher, consumer], &kill/1)
      end)

      []
    end

    test "continue on reconnect", %{receiver: receiver} do
      WebSockex.send_frame(receiver, {:text, "push_message"})

      assert_receive {:event_received, ["consumer message 0"]}

      :ok = WebSockex.send_frame(receiver, {:text, "stop"})

      Process.sleep(100)

      WebSockex.send_frame(receiver, {:text, "push_message"})
      assert_receive {:event_received, ["consumer message 0"]}
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
