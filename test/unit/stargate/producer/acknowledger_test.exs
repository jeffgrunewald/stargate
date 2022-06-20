defmodule Stargate.Producer.AcknowledgerTest do
  use ExUnit.Case

  alias Stargate.Producer.Acknowledger, as: ProdAcknowledger

  setup do
    opts = [
      registry: :sg_reg_test_acknowledger,
      tenant: "default",
      namespace: "public",
      topic: "foobar"
    ]

    {:ok, registry} = Registry.start_link(keys: :unique, name: :sg_reg_test_acknowledger)
    {:ok, acknowledger} = ProdAcknowledger.start_link(opts)

    on_exit(fn ->
      kill(acknowledger)
      kill(registry)
    end)

    [acknowledger: acknowledger]
  end

  describe "synchronous ack" do
    test "tracks a produce and acknowledges to sender", %{acknowledger: acknowledger} do
      ref = make_ref()
      :ok = ProdAcknowledger.produce(acknowledger, "123", {self(), ref})

      :ok = ProdAcknowledger.ack(acknowledger, {:ack, "123"})
      assert_receive {^ref, :ack}
    end

    test "returns errors to the sender", %{acknowledger: acknowledger} do
      ref = make_ref()
      :ok = ProdAcknowledger.produce(acknowledger, "234", {self(), ref})

      :ok = ProdAcknowledger.ack(acknowledger, {:error, "whoops", "234"})

      assert_receive {^ref, :error, "whoops"}
    end
  end

  describe "asynchronous ack" do
    defmodule Ack do
      def ack(res, pid, msg) do
        send(pid, {res, msg})
      end
    end

    test "tracks a produce and executes the saved function", %{acknowledger: acknowledger} do
      :ok =
        ProdAcknowledger.produce(
          acknowledger,
          "123",
          {Stargate.Producer.AcknowledgerTest.Ack, :ack, [self(), "async_ack"]}
        )

      :ok = ProdAcknowledger.ack(acknowledger, {:ack, "123"})

      assert_receive {:ok, "async_ack"}
    end

    test "logs errors when they occur during async ack", %{acknowledger: acknowledger} do
      :ok =
        ProdAcknowledger.produce(
          acknowledger,
          "234",
          {Stargate.Producer.AcknowledgerTest.Ack, :ack, [self(), "async_ack_error"]}
        )

      :ok = ProdAcknowledger.ack(acknowledger, {:error, "oh nooo", "234"})

      assert_receive {{:error, "oh nooo"}, "async_ack_error"}
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
