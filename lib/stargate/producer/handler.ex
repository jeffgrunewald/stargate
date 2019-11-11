defmodule Stargate.Producer.Handler do
  @moduledoc """
  TODO
  """

  use Puid

  # @doc """
  # TODO
  # """
  # @spec produce(String.t(), String.t()) :: :ok | {:error, term()}
  # def produce(url, message) when is_binary(url) and is_binary(message) do
  #   payload = construct_payload(message)

  #   with {:ok, temp_producer} <- WebSockex.start(url, __MODULE__, %{}),
  #        :ok <- produce(temp_producer, payload) do
  #     Process.exit(temp_producer, :shutdown)
  #     :ok
  #   else
  #     {:error, reason} -> {:error, reason}
  #   error -> {:error, error}
  #   end
  # end

  @doc """
  TODO
  """
  @spec produce(atom() | pid(), String.t()) :: :ok | {:error, term()}
  def produce(conn, message) do
    ctx = generate()
    payload = construct_payload(message, ctx)
    GenServer.cast(Stargate.Producer.Acknowledger, {:produce, ctx, self()})

    WebSockex.send_frame(Stargate.Producer, {:text, payload})

    receive do
      :ack -> :ok
      err -> err
    end
  end

  defp construct_payload(message, context) do
    %{
      "payload" => Base.encode64(message),
      "context" => context
    }
    |> Jason.encode!()
  end

  @doc """
  TODO
  """
  @spec produce(atom() | pid(), String.t(), {atom(), atom(), [term()]}) :: :ok | {:error, term()}
  def produce(conn, message, mfa) do
    ctx = generate()
    payload = construct_payload(message, ctx)
    GenServer.cast(Stargate.Producer.Acknowledger, {:produce, ctx, mfa})

    WebSockex.send_frame(Stargate.Producer, {:text, payload})
  end
end
