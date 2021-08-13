defmodule Stargate.Reader.QueryParamsTest do
  use ExUnit.Case

  alias Stargate.Reader.QueryParams

  describe "Reader query params" do
    test "returns default (empty) query params" do
      input = %{}

      assert "" == QueryParams.build_params(input)
    end

    test "returns custom query params" do
      input = %{
        name: "foobar",
        queue_size: 500,
        starting_message: "Dkx4SCF=="
      }

      result = "messageId=Dkx4SCF==&readerName=foobar&receiverQueueSize=500"

      assert result == QueryParams.build_params(input)
    end
  end
end
