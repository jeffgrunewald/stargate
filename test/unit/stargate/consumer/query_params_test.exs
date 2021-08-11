defmodule Stargate.Consumer.QueryParamsTest do
  use ExUnit.Case

  alias Stargate.Consumer.QueryParams

  describe "Consumer query params" do
    test "returns default (empty) query params" do
      input = %{}

      assert "" == QueryParams.build_params(input)
    end

    test "returns custom query params" do
      input = %{
        subscription_type: :exclusive,
        ack_timeout: 2_000,
        queue_size: 500,
        pull_mode: true
      }

      result =
        "ackTimeoutMillis=2000&pullMode=true&receiverQueueSize=500&subscriptionType=Exclusive"

      assert result == QueryParams.build_params(input)
    end
  end
end
