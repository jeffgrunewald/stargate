defmodule Stargate.ConnectionTest do
  use ExUnit.Case

  setup do
    port = Enum.random(49152..65535)

    {:ok, server} = MockSocket.Supervisor.start_link(port: port, source: self())

    on_exit(fn ->
      kill(server)
    end)

    [port: port]
  end

  describe "connection macro" do
    test "establishes a socket connection", %{port: port} do
      {:ok, _client} = SampleClient.start_link(port: port, source: self())

      :ok = SampleClient.cast("connection test")

      assert_receive {:received_frame, "connection test received loud and clear"}
    end

    test "handles ping requests", %{port: port} do
      {:ok, _client} = SampleClient.start_link(port: port, source: self())

      SampleClient.ping_socket()

      assert_receive :pong_from_socket, 500
    end
  end

  describe "connection settings" do
    test "generates valid connection options" do
      assert %{
        url: "ws://app.example.com/ws/v2/reader/persistent/prod/domain/data-stream?thing=stuff",
        host: "app.example.com",
        protocol: "ws",
        persistence: "persistent",
        tenant: "prod",
        namespace: "domain",
        topic: "data-stream"
      } == Stargate.Connection.connection_settings([host: "app.example.com", tenant: "prod", namespace: "domain", topic: "data-stream"], "reader", "thing=stuff")
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
