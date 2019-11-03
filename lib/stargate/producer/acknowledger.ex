defmodule Stargate.Producer.Acknowledger do
  @moduledoc """
  TODO
  """

  @callback produce_ack(term()) :: :ack | {:ack, term()}
end
