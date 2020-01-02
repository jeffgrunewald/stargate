defmodule Stargate.ProducerReceiverTest do
  use ExUnit.Case
  use Divo

  describe "producer and receiver" do
    test "produces to and consumes from pulsar" do
      Agent.start_link(fn -> [] end, name: :stargate_integration_store)

      tenant = "public"
      namespace = "default"
      topic = "integration"
      subscription = "sub"
      input = 0..100 |> Enum.filter(fn num -> rem(num, 2) != 0 end) |> Enum.map(&to_string/1)

      {:ok, pid} =
        Stargate.Supervisor.start_link(
          name: :integration,
          host: [localhost: 8080],
          producer: [
            tenant: tenant,
            namespace: namespace,
            topic: topic
          ],
          consumer: [
            tenant: tenant,
            namespace: namespace,
            topic: topic,
            subscription: subscription,
            processors: 2,
            handler: IntegrationTestHandler
          ]
        )

      Stargate.produce(
        {:via, Registry, {:sg_reg_integration, :"sg_prod_#{tenant}_#{namespace}_#{topic}"}},
        input
      )

      Process.sleep(100)

      expected = 2..100 |> Enum.filter(fn num -> rem(num, 2) == 0 end)
      result = Agent.get(:stargate_integration_store, fn store -> store end) |> Enum.sort()

      assert result == expected

      Supervisor.stop(pid)
    end
  end
end
