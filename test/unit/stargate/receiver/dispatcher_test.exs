defmodule Stargate.Receiver.DispatcherTest do
  use ExUnit.Case

  alias Stargate.Receiver.Dispatcher

  setup do
    reg_name = :sg_reg_dispatcher_test
    type = :reader
    tenant = "default"
    ns = "public"
    topic = "barbaz"
    port = Enum.random(49_152..65_535)
    path = "ws/v2/#{type}/persistent/#{tenant}/#{ns}/#{topic}"

    opts = [
      host: [localhost: port],
      registry: reg_name,
      type: type,
      tenant: tenant,
      namespace: ns,
      topic: topic
    ]

    {:ok, registry} = Registry.start_link(keys: :unique, name: reg_name)
    {:ok, server} = MockSocket.Supervisor.start_link(port: port, path: path, source: self())
    {:ok, dispatcher} = Dispatcher.start_link(opts)
    {:ok, consumer} = MockConsumer.start_link(producer: dispatcher, source: self())

    on_exit(fn ->
      Enum.each([registry, dispatcher, consumer, server], &kill/1)
    end)

    [dispatcher: dispatcher]
  end

  describe "push/2" do
    test "generates events for each message pushed", %{dispatcher: dispatcher} do
      :ok = Dispatcher.push(dispatcher, ["message1", "message2"])

      assert_receive {:event_received, ["message1", "message2"]}
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
