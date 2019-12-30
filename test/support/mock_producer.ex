defmodule MockProducer do
  use GenStage

  def start_link(init_args) do
    registry = Keyword.get(init_args, :registry)
    tenant = Keyword.get(init_args, :tenant)
    ns = Keyword.get(init_args, :namespace)
    topic = Keyword.get(init_args, :topic)
    type = Keyword.get(init_args, :type)

    name =
      case type do
        :dispatcher -> :"sg_dispatcher_#{tenant}_#{ns}_#{topic}"
        :processor -> :"sg_processor_#{tenant}_#{ns}_#{topic}_0"
      end

    GenStage.start_link(__MODULE__, Keyword.get(init_args, :count),
      name: {:via, Registry, {registry, name}}
    )
  end

  def init(num) do
    dispatched_events =
      Enum.map(0..(num - 1), fn item ->
        %{
          payload: Base.encode64("#{item}"),
          publishTime: DateTime.utc_now() |> DateTime.to_iso8601()
        }
        |> Jason.encode!()
      end)

    {:producer, %{dispatched_events: dispatched_events}}
  end

  def handle_cast(:push_dispatched_message, %{events: events} = state) when length(events) > 0 do
    [message | remaining_messages] = events

    GenStage.cast(self(), :push_dispatched_message)

    {:noreply, [message], %{state | events: remaining_messages}}
  end

  def handle_cast(:push_dispatched_message, %{events: events} = state) when events == [] do
    {:noreply, [:push_count], state}
  end

  def handle_cast(:push_processed_message, state) do
    {:noreply, [], state}
  end

  def handle_cast(_, state) do
    {:noreply, [], state}
  end

  def handle_demand(_, state) do
    {:noreply, [], state}
  end
end
