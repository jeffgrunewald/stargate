defmodule Stargate.Producer.Acknowledger do
  @moduledoc """
  TODO
  """

  @callback produce_ack(
              {:ok, term()}
              | {:ok, term(), term()}
              | {:error, term()}
              | {:error, term(), term()}
            ) ::
              :ack
              | {:ack, term()}
              | {:error, term()}
end
