defmodule Stargate do
  @moduledoc """
  Documentation for Stargate.
  """

  @doc """
  TODO
  """

  defdelegate produce(client_or_url, message), to: Stargate.Producer
end
