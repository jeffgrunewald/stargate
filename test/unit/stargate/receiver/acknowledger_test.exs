defmodule Stargate.Receiver.AcknowledgerTest do
  use ExUnit.Case

  setup do
    tenant = "public"
    ns = "default"
    topic = "blorth"
    reg_name = :sg_reg_ack_test
    type = :reader
    port = Enum.random(49_152..65_535)
    path = "ws/v2/reader/persistent/#{tenant}/#{ns}/#{topic}"

    opts = [
      host: [localhost: port],
      registry: reg_name,
      type: type,
      tenant: tenant,
      namespace: ns,
      topic: topic
    ]

    {:ok, server} = MockSocket.Supervisor.start_link(port: port, path: path, source: self())

    {:ok, registry} = Registry.start_link(keys: :unique, name: reg_name)

    {:ok, producer} =
      MockProducer.start_link(
        count: 3,
        registry: reg_name,
        tenant: tenant,
        namespace: ns,
        topic: topic,
        type: :processor
      )

    {:ok, receiver} = Stargate.Receiver.start_link(opts)
    {:ok, acknowledger} = Stargate.Receiver.Acknowledger.start_link(opts)

    on_exit(fn ->
      Enum.each([registry, producer, receiver, acknowledger, server], &kill/1)
    end)

    [producer: producer]
  end

  describe "acknowledger stage" do
    test "acks messages back to the receiver socket", %{producer: producer} do
      GenStage.cast(producer, :push_message)

      assert_receive {:received_frame, "0 loud and clear"}
      assert_receive {:received_frame, "1 loud and clear"}
      assert_receive {:received_frame, "2 loud and clear"}
      refute_receive {:received_frame, "skipped loud and clear"}
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
