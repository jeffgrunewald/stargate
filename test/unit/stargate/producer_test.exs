defmodule Stargate.ProducerTest do
  use ExUnit.Case

  setup do
    port = Enum.random(49_152..65_535)
    path = "ws/v2/producer/persistent/default/public/foobar"

    {:ok, server} = MockSocket.Supervisor.start_link(port: port, path: path, source: self())

    on_exit(fn ->
      kill(server)
    end)

    [port: port]
  end

  describe "produce/2" do
    test "sends a payload and context to the socket", %{port: port} do
      opts = [
        registry: :sg_reg_producer,
        host: [localhost: port],
        tenant: "default",
        namespace: "public",
        topic: "foobar"
      ]

      {:ok, _registry} = Registry.start_link(keys: :unique, name: :sg_reg_producer)
      {:ok, _} = Stargate.Producer.Supervisor.start_link(opts)

      producer =
        Stargate.registry_key(opts[:tenant], opts[:namespace], opts[:topic],
          registry: opts[:registry],
          component: :producer
        )

      :ok =
        Stargate.produce(producer, %{
          "payload" => "helloooo",
          "context" => "123",
          "properties" => %{"key" => "value"}
        })

      assert_receive {:received_frame,
                      "123, {\"context\":\"123\",\"payload\":\"aGVsbG9vb28=\",\"properties\":{\"key\":\"value\"}} loud and clear"}
    end

    test "sends a payload and context to the socket results in error", %{port: port} do
      opts = [
        registry: :sg_reg_producer,
        host: [localhost: port],
        tenant: "default",
        namespace: "public",
        topic: "foobar"
      ]

      {:ok, _registry} = Registry.start_link(keys: :unique, name: :sg_reg_producer)
      {:ok, _} = Stargate.Producer.Supervisor.start_link(opts)

      producer =
        Stargate.registry_key(opts[:tenant], opts[:namespace], opts[:topic],
          registry: opts[:registry],
          component: :producer
        )

      {:error, "error"} =
        Stargate.produce(producer, %{
          "payload" => "helloooo",
          "context" => "123",
          "properties" => %{"error" => "something went wrong"}
        })

      assert_receive {:received_frame,
                      "123, {\"context\":\"123\",\"payload\":\"aGVsbG9vb28=\",\"properties\":{\"error\":\"something went wrong\"}} loud and clear"}
    end

    test "produces via one-off producer", %{port: port} do
      url = "ws://localhost:#{port}/ws/v2/producer/persistent/default/public/foobar"

      spawn(fn ->
        Stargate.produce(url, %{"context" => "123", "key" => "wooo", "payload" => "hiiii"})
      end)

      assert_receive {:received_frame,
                      "123, {\"context\":\"123\",\"key\":\"wooo\",\"payload\":\"aGlpaWk=\"} loud and clear"}
    end

    test "produces a list of values", %{port: port} do
      opts = [
        registry: :sg_reg_producer,
        host: [localhost: port],
        tenant: "default",
        namespace: "public",
        topic: "foobar"
      ]

      {:ok, _registry} = Registry.start_link(keys: :unique, name: :sg_reg_producer)
      {:ok, _} = Stargate.Producer.Supervisor.start_link(opts)

      producer =
        Stargate.registry_key(opts[:tenant], opts[:namespace], opts[:topic],
          registry: opts[:registry],
          component: :producer
        )

      :ok =
        Stargate.produce(producer, [
          %{"payload" => "hello", "context" => "123"},
          %{"payload" => "world", "context" => "456"}
        ])

      assert_receive {:received_frame,
                      "123, {\"context\":\"123\",\"payload\":\"aGVsbG8=\"} loud and clear"}

      assert_receive {:received_frame,
                      "456, {\"context\":\"456\",\"payload\":\"d29ybGQ=\"} loud and clear"}
    end

    test "returns an error for invalid produce" do
      url = "ws://localhost:8080/ws/v2/producer/wrong/topic/missing"

      assert {:error,
              ["ws:", "", "localhost:8080", "ws", "v2", "producer", "wrong", "topic", "missing"]} ==
               Stargate.produce(url, "not gonna happen")
    end
  end

  describe "produce/3" do
    test "handles produce acking out of band", %{port: port} do
      opts = [
        name: "produce_3_test",
        host: [localhost: port],
        protocol: "ws",
        producer: [
          persistence: "persistent",
          tenant: "default",
          namespace: "public",
          topic: "foobar"
        ]
      ]

      {:ok, _supervisor} = Stargate.Supervisor.start_link(opts)

      [{producer, _}] =
        Registry.lookup(
          :sg_reg_produce_3_test,
          {:producer, "persistent", "default", "public", "foobar"}
        )

      test = self()

      spawn(fn ->
        Stargate.produce(
          producer,
          %{"context" => "123", "payload" => "message"},
          {Kernel, :send, [test, "message received"]}
        )
      end)

      assert_receive "message received", 1_000
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
