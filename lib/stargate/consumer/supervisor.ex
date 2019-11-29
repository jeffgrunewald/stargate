defmodule Stargate.Consumer.Supervisor do
  @moduledoc """
  TODO
  """
  use Supervisor

  @doc """
  TODO
  """

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_args) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  TODO
  """

  @impl Supervisor
  def init(_args) do
    :ignore
  end
end
