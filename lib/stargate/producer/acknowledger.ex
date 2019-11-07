defmodule Stargate.Producer.Acknowledger do
  @moduledoc """
  TODO
  """

  @callback init(term()) :: {:ok, term()}

  @callback ack(
              {:ok, term()}
              | {:ok, term(), term()}
              | {:error, term()}
              | {:error, term(), term()}
            ) ::
              :ack | :error

  @callback ack(
              {:ok, term(), term()}
              | {:ok, term(), term(), term()}
              | {:error, term(), term()}
              | {:error, term(), term(), term()}
            ) ::
              {:ack, term()} | {:error, term()}

  defmacro __using__(_opts) do
    quote do
      @behaviour Stargate.Producer.Acknowledger

      def init(args) do
        {:ok, args}
      end

      def ack(response) do
        case elem(response, 0) do
          :ok -> :ack
          :error -> :error
        end
      end

      def ack(response, state) do
        case ack(response) do
          :ok -> {:ok, state}
          :error -> {:ok, state}
        end
      end
    end
  end
end
