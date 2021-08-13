defmodule Stargate.Producer.QueryParamsTest do
  use ExUnit.Case

  alias Stargate.Producer.QueryParams

  describe "Producer query params" do
    test "returns default (empty) query params" do
      input = %{}

      assert "" == QueryParams.build_params(input)
    end

    test "returns custom query params" do
      input = %{
        routing_mode: :round_robin,
        compression_type: :zlib,
        send_timeout: 3_000,
        name: "foobar",
        initial_seq_id: 2_500_000_000
      }

      result =
        "compressionType=ZLIB&initialSequenceId=2500000000&messageRoutingMode=RoundRobinPartition&producerName=foobar&sendTimeoutMillis=3000"

      assert result == QueryParams.build_params(input)
    end
  end
end
