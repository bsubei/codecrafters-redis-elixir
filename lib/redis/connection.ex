defmodule Redis.Connection do
  @moduledoc """
  This Connection module defines the process that handles a single Redis client by listening for a request and
  replying with a response.
  """
  use GenServer
  require Logger

  defstruct [:socket, buffer: <<>>]

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

  defp handle_new_data(%__MODULE__{socket: socket} = state) do
    case String.split(state.buffer, "\n", parts: 2) do
      [line, rest] ->
        # If our buffer has at least one line, echo it back to the client.
        :gen_tcp.send(socket, line <> "\n")
        # Remove that line from our buffer.
        state = put_in(state.buffer, rest)
        # And don't forget to check for more lines by recursing.
        handle_new_data(state)

      _other ->
        # Buffer has no lines, do nothing.
        state
    end
  end
end
