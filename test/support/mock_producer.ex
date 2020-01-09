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

    GenStage.start_link(__MODULE__, init_args, name: {:via, Registry, {registry, name}})
  end

  def init(init_args) do
    num = Keyword.get(init_args, :count)
    type = Keyword.get(init_args, :type)

    events =
      case type do
        :dispatcher ->
          Enum.map(0..(num - 1), fn item ->
            %{
              payload: Base.encode64("#{item}"),
              publishTime: DateTime.utc_now() |> DateTime.to_iso8601()
            }
            |> Jason.encode!()
          end)

        :processor ->
          [
            {:continue, "skipped"}
            | Enum.map(0..(num - 1), fn item ->
                {:ack, "#{item}"}
              end)
          ]
      end

    {:producer, %{events: events}}
  end

  def handle_cast(:push_message, %{events: events} = state) when length(events) > 0 do
    [message | remaining_messages] = events

    GenStage.cast(self(), :push_message)

    {:noreply, [message], %{state | events: remaining_messages}}
  end

  def handle_cast(_, state) do
    {:noreply, [], state}
  end

  def handle_demand(_, state) do
    {:noreply, [], state}
  end
end
