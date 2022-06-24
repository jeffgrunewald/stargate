defmodule Stargate.ConnectionTest do
  use ExUnit.Case

  setup do
    port = Enum.random(49_152..65_535)

    {:ok, server} = MockSocket.Supervisor.start_link(port: port, path: "ws_test", source: self())

    on_exit(fn ->
      Enum.map([server], &kill/1)
    end)

    [port: port]
  end

  describe "connection macro" do
    setup %{port: port} do
      {:ok, client} = SampleClient.start_link(port: port, path: "ws_test")

      on_exit(fn ->
        Enum.map([client], &kill/1)
      end)
    end

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

      ssl_opts = [
        cacertfile: "/certs/ca.pem",
        certfile: "/certs/cert.pem",
        keyfile: "/certs/key.pem"
      ]

      init_args = [
        ssl_options: ssl_opts,
        auth_token: token,
        something: "else"
      ]

      assert [
               ssl_options: ssl_opts,
               extra_headers: [{"Authorization", "Bearer " <> token}]
             ] == Stargate.Connection.auth_settings(init_args)
    end
  end

  describe "backoff" do
    defmodule Backoff do
      def calc(attempt, :test) do
        1_000 * attempt
      end
    end

    test "calculate_backoff default calculator" do
      assert 2_000 =
               Stargate.Connection.default_backoff_calculator()
               |> Stargate.Connection.calculate_backoff(1)
    end

    test "calculate_backoff MFA" do
      mfa = {Stargate.ConnectionTest.Backoff, :calc, [:test]}
      assert 1_000 = Stargate.Connection.calculate_backoff(mfa, 1)
      assert 2_000 = Stargate.Connection.calculate_backoff(mfa, 2)
    end

    test "calculate_backoff function" do
      fun = fn attempt -> 1_000 * attempt end
      assert 1_000 = Stargate.Connection.calculate_backoff(fun, 1)
      assert 2_000 = Stargate.Connection.calculate_backoff(fun, 2)
    end

    test "calculate_backoff invalid function" do
      fun = fn -> 1_000 end
      assert_raise ArgumentError, fn -> Stargate.Connection.calculate_backoff(fun, 1) end
    end

    test "sample client", %{port: port} do
      test_pid = self()

      backoff_calc = fn _ ->
        send(test_pid, :calculate_backoff)
        10
      end

      {:ok, _client} =
        SampleClient.start_link(port: port, path: "ws_test", backoff_calculator: backoff_calc)

      assert_receive :client_connected

      SampleClient.cast("stop")

      assert_receive :calculate_backoff
      assert_receive :client_connected
    end
  end

  describe "handle_send_error" do
    test "sending message during backoff results in an error", %{port: port} do
      test_pid = self()

      backoff_calc = fn _ ->
        send(test_pid, :calculate_backoff)
        :infinity
      end

      {:ok, _client} =
        SampleClient.start_link(port: port, path: "ws_test", backoff_calculator: backoff_calc)

      assert_receive :client_connected

      SampleClient.cast("stop")
      assert_receive :calculate_backoff

      ref = make_ref()
      SampleClient.cast("test", ref)
      assert_receive {:received_send_error, :not_connected, ^ref}
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
