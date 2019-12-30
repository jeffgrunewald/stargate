defmodule MockConsumer do
  use GenStage

  def start_link(init_args) do
    GenStage.start_link(__MODULE__, init_args, name: :mock_consumer)
  end

  def init(init_args) do
    state = %{producer: Keyword.get(init_args, :producer), source: Keyword.get(init_args, :source)}
    {:consumer, state, subscribe_to: [{state.producer, []}]}
  end

  def handle_events(messages, _from, state) do
    send(state.source, {:event_received, messages})

    {:noreply, [], state}
  end
end
