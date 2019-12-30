defmodule Stargate.Receiver.DispatcherTest do
  use ExUnit.Case

  setup do
    opts = [
      registry: :sg_reg_dispatcher_test,
      tenant: "default",
      namespace: "public",
      topic: "barbaz"
    ]

    {:ok, registry} = Registry.start_link(keys: :unique, name: :sg_reg_dispatcher_test)
    {:ok, dispatcher} = Stargate.Receiver.Dispatcher.start_link(opts)
    {:ok, consumer} = MockConsumer.start_link(producer: dispatcher, source: self())

    on_exit(fn ->
      Enum.each([registry, dispatcher, consumer], &kill/1)
    end)

    [dispatcher: dispatcher]
  end

  describe "push/2" do
    test "generates events for each message pushed", %{dispatcher: dispatcher} do
      :ok = Stargate.Receiver.Dispatcher.push(dispatcher, ["message1", "message2"])

      assert_receive {:event_received, ["message1", "message2"]}
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
