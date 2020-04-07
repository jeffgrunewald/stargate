defmodule Stargate.ConnectionTest do
  use ExUnit.Case

  setup do
    port = Enum.random(49152..65535)

    {:ok, server} = MockSocket.Supervisor.start_link(port: port, path: "ws_test", source: self())
    {:ok, client} = SampleClient.start_link(port: port, path: "ws_test")

    on_exit(fn ->
      Enum.map([server, client], &kill/1)
    end)

    [port: port]
  end

  describe "connection macro" do
    test "establishes a socket connection" do
      :ok =
        SampleClient.cast(Jason.encode!(%{"context" => "connection test", "messageId" => "1"}))

      assert_receive {:received_frame,
                      "connection test, {\"context\":\"connection test\",\"messageId\":\"1\"} loud and clear"}
    end

    test "handles ping requests" do
      SampleClient.ping_socket()

      assert_receive :pong_from_socket, 500
    end
  end

  describe "connection settings" do
    test "generates valid connection options" do
      assert %{
               url:
                 "ws://app.example.com/ws/v2/reader/persistent/prod/domain/data-stream?thing=stuff",
               host: "app.example.com",
               protocol: "ws",
               persistence: "persistent",
               tenant: "prod",
               namespace: "domain",
               topic: "data-stream"
             } ==
               Stargate.Connection.connection_settings(
                 [
                   host: "app.example.com",
                   tenant: "prod",
                   namespace: "domain",
                   topic: "data-stream"
                 ],
                 "reader",
                 "thing=stuff"
               )
    end
  end

  describe "auth settings" do
    test "generates valid authentication options" do
      token = "jwt-token"
      certname = "/certa/ca.pem"

      init_args = [
        cacerts: certname,
        insecure: false,
        auth_token: token,
        extra_headers: [{"X-Some-Header", "Some-Value"}],
        something: "else"
      ]

      assert [
               insecure: false,
               cacerts: certname,
               extra_headers: [
                 {"Authorization", "Bearer " <> token},
                 {"X-Some-Header", "Some-Value"}
               ]
             ] == Stargate.Connection.web_socketex_conn_settings(init_args)
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
