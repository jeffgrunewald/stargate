defmodule MockProducer do
  use GenStage

  def start_link(init_args) do
    registry = Keyword.get(init_args, :registry)
    tenant = Keyword.get(init_args, :tenant)
    ns = Keyword.get(init_args, :namespace)
    topic = Keyword.get(init_args, :topic)

    GenStage.start_link(__MODULE__, Keyword.get(init_args, :count), name: {:via, Registry, {registry, :"sg_dispatcher_#{tenant}_#{ns}_#{topic}"}})
  end

  def init(num) do
    events = Enum.map(0..(num - 1), fn item -> %{payload: Base.encode64("#{item}"), publishTime: DateTime.utc_now() |> DateTime.to_iso8601()} |> Jason.encode!() end)

    {:producer, %{events: events}}
  end

  def handle_cast(:push_message, %{events: events} = state) when length(events) > 0 do
    [message | remaining_messages] = events

    GenStage.cast(self(), :push_message)

    {:noreply, [message], %{state | events: remaining_messages}}
  end

  def handle_cast(:push_message, %{events: events} = state) when events == [] do
    {:noreply, [:push_count], state}
  end

  def handle_cast(_, state) do
    {:noreply, [], state}
  end

  def handle_demand(_, state) do
    {:noreply, [], state}
  end
end
