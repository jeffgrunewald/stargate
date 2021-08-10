defmodule Stargate.ProducerReceiverTest do
  use ExUnit.Case
  use Divo
  import AsyncAssertion

  setup do
    {:ok, agent} = Agent.start_link(fn -> [] end, name: :integration_store)

    input = 0..100 |> Enum.filter(fn num -> rem(num, 2) != 0 end) |> Enum.map(&to_string/1)
    expected = 2..100 |> Enum.filter(fn num -> rem(num, 2) == 0 end)

    on_exit(fn ->
      Process.exit(agent, :normal)
    end)

    [input: input, expected: expected]
  end

  describe "producer and receiver" do
    test "produces to and reads from pulsar", %{input: input, expected: expected} do
      host = [localhost: 8080]
      tenant = "public"
      namespace = "default"
      topic = "integration1"

      {:ok, producer} =
        Stargate.Supervisor.start_link(
          host: host,
          producer: [
            tenant: tenant,
            namespace: namespace,
            topic: topic
          ]
        )

      Stargate.produce(
        {:via, Registry,
         {:sg_reg_default, {:producer, "persistent", "#{tenant}", "#{namespace}", "#{topic}"}}},
        input
      )

      Supervisor.stop(producer)

      {:ok, reader} =
        Stargate.Supervisor.start_link(
          host: host,
          reader: [
            tenant: tenant,
            namespace: namespace,
            topic: topic,
            processors: 2,
            handler: IntegrationTestHandler,
            query_params: %{
              starting_message: :earliest
            }
          ]
        )

      assert_async(10, 150, fn ->
        result = Agent.get(:integration_store, fn store -> store end) |> Enum.sort()
        assert result == expected
      end)

      Supervisor.stop(reader)
    end

    test "consumer consumes messages off the topic", %{input: input, expected: expected} do
      host = [localhost: 8080]
      tenant = "public"
      namespace = "default"
      topic = "integration2"
      subscription = "sub"

      consumer_opts = [
        tenant: tenant,
        namespace: namespace,
        topic: topic,
        subscription: subscription,
        handler: IntegrationTestHandler,
        query_params: %{
          subscription_type: :failover
        }
      ]

      {:ok, producer} =
        Stargate.Supervisor.start_link(
          host: host,
          producer: [
            tenant: tenant,
            namespace: namespace,
            topic: topic
          ]
        )

      {:ok, consumer1} =
        Stargate.Supervisor.start_link(
          name: :consumer1,
          host: host,
          consumer: consumer_opts
        )

      {:ok, consumer2} =
        Stargate.Supervisor.start_link(
          name: :consumer2,
          host: host,
          consumer: consumer_opts
        )

      Stargate.produce(
        {:via, Registry,
         {:sg_reg_default, {:producer, "persistent", "#{tenant}", "#{namespace}", "#{topic}"}}},
        input
      )

      Enum.random([consumer1, consumer2]) |> Supervisor.stop()

      assert_async(10, 150, fn ->
        result = Agent.get(:integration_store, fn store -> store end) |> Enum.sort()
        assert result == expected
      end)

      Enum.map([producer, consumer1, consumer2], fn pid ->
        if Process.alive?(pid), do: Supervisor.stop(pid)
      end)
    end

    test "consumes in pull mode" do
      input = 0..3000 |> Enum.filter(fn num -> rem(num, 2) != 0 end) |> Enum.map(&to_string/1)
      expected = 2..3000 |> Enum.filter(fn num -> rem(num, 2) == 0 end)
      host = [localhost: 8080]
      tenant = "public"
      namespace = "default"
      topic = "integration3"
      subscription = "sub"

      {:ok, producer} =
        Stargate.Supervisor.start_link(
          host: host,
          producer: [
            tenant: tenant,
            namespace: namespace,
            topic: topic
          ]
        )

      {:ok, consumer} =
        Stargate.Supervisor.start_link(
          name: :consumer,
          host: host,
          consumer: [
            tenant: tenant,
            namespace: namespace,
            topic: topic,
            subscription: subscription,
            handler: IntegrationTestHandler,
            query_params: %{
              pull_mode: true
            }
          ]
        )

      Stargate.produce(
        {:via, Registry,
         {:sg_reg_default, {:producer, "persistent", "#{tenant}", "#{namespace}", "#{topic}"}}},
        input
      )

      assert_async(10, 150, fn ->
        result = Agent.get(:integration_store, fn store -> store end) |> Enum.sort()
        assert result == expected
      end)

      Enum.map([producer, consumer], fn pid ->
        if Process.alive?(pid), do: Supervisor.stop(pid)
      end)
    end
  end
end
