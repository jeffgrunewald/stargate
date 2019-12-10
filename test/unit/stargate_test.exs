defmodule StargateTest do
  use ExUnit.Case

  test "converts messages to the correct struct" do
    timestamp = "2016-08-30 16:45:57.785Z"
    input = "{\"messageId\": \"CAAQAw==\",\"payload\": \"SGVsbG8gV29ybGQ=\",\"properties\": {\"key1\": \"value1\", \"key2\": \"value2\"},\"publishTime\": \"#{timestamp}\"}"
    {:ok, date_time, _} = DateTime.from_iso8601(timestamp)

    result = %Stargate.Message{
      topic: "foo",
      namespace: "default",
      tenant: "public",
      persistence: "non-persistent",
      message_id: "CAAQAw==",
      payload: "Hello World",
      key: "",
      properties: %{
        "key1" => "value1",
        "key2" => "value2"
      },
      publish_time: date_time
    }

    assert result == input |> Jason.decode!() |> Stargate.Message.new("non-persistent", "public", "default", "foo")
  end
end
