defmodule Stargate.Receiver.ProcessorTest do
  use ExUnit.Case

  alias Stargate.Receiver.Processor

  setup do
    tenant = "default"
    ns = "public"
    topic = "bazqux"
    reg_name = :sg_reg_processor_test

    opts = [
      registry: reg_name,
      tenant: tenant,
      namespace: ns,
      topic: topic,
      handler: UnitTestHandler,
      handler_init_args: {self(), 3},
      processor_name: :"sg_processor_#{tenant}_#{ns}_#{topic}_0"
    ]

    {:ok, registry} = Registry.start_link(keys: :unique, name: reg_name)

    {:ok, producer} =
      MockProducer.start_link(
        count: 3,
        registry: reg_name,
        tenant: tenant,
        namespace: ns,
        topic: topic,
        type: :dispatcher
      )

    {:ok, processor} = Processor.start_link(opts)
    {:ok, consumer} = MockConsumer.start_link(producer: processor)

    on_exit(fn ->
      Enum.each([registry, producer, processor, consumer], &kill/1)
    end)

    [producer: producer]
  end

  describe "processor stage" do
    test "handles produced messages", %{producer: producer} do
      GenStage.cast(producer, :push_message)

      assert_receive "message 0 received"
      assert_receive "message 1 received"
      assert_receive "message 2 received"
      assert_receive "all 3 messages received"
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
