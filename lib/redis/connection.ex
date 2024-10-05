defmodule Redis.Connection do
  @moduledoc """
  This Connection module defines the process that handles a single Redis client by listening for a request and
  replying with a response.
  """
  use GenServer
  require Logger
  alias Redis.RESP
  alias Redis.Connection

  defstruct [:socket, buffer: <<>>, kvstore: %{}]

  @spec start_link(:gen_tcp.socket()) :: GenServer.on_start()
  def start_link(socket) do
    GenServer.start_link(__MODULE__, socket)
  end

  @impl true
  def init(socket) do
    state = %__MODULE__{socket: socket}
    {:ok, state}
  end

  @impl true
  def handle_info(message, state)

  def handle_info({:tcp, socket, data}, %__MODULE__{socket: socket} = state) do
    state = update_in(state.buffer, &(&1 <> data))
    state = handle_new_data(state)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, %__MODULE__{socket: socket} = state) do
    {:stop, :normal, state}
  end

  def handle_info({:tcp_error, socket, reason}, %__MODULE__{socket: socket} = state) do
    Logger.error("TCP connection error: #{inspect(reason)}")
    {:stop, :normal, state}
  end

  # TODO handle incomplete messages.
  defp handle_new_data(%__MODULE__{socket: socket} = state) do
    case state.buffer do
      # Done, nothing more to handle.
      "" ->
        state

      _ ->
        {:ok, decoded, rest} = RESP.decode(state.buffer)

        # We only respond to Array requests, just drop other responses.
        case decoded do
          [_ | _] = request ->
            response = parse_request(state, request)
            :gen_tcp.send(socket, RESP.encode(response))

          _ ->
            Logger.error(
              "Got non-array request from client #{inspect(socket)}: #{state.buffer} -> #{inspect(decoded)}"
            )
        end

        state = put_in(state.buffer, rest)
        handle_new_data(state)
    end
  end

  # TODO handle case insensitivity
  defp parse_request(_, ["PING"]), do: "PONG"

  defp parse_request(_, ["PING", arg]), do: arg

  defp parse_request(_, ["ECHO", arg]), do: arg

  defp parse_request(state = %Connection{}, ["GET", arg]) do
    {:ok, value} = Map.fetch(state.kvstore, arg)
    value
  end
end
