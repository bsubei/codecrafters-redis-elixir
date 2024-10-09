defmodule Redis.Connection do
  @moduledoc """
  This Connection module defines the process that handles a single Redis client by listening for a request and
  replying with a response.
  """
  use GenServer
  require Logger
  alias Redis.RESP
  alias Redis.Connection

  defstruct [:socket, :send_fn, buffer: <<>>]

  @spec start_link(%{
          # The socket must always be specified.
          socket: :gen_tcp.socket()
          # The send_fn is optional and will default to :gen_tcp.send/2 if not specified.
        }) :: GenServer.on_start()
  def start_link(init_arg) do
    GenServer.start_link(__MODULE__, init_arg)
  end

  @impl true
  def init(init_arg) do
    state = %__MODULE__{
      socket: Map.get(init_arg, :socket),
      # Use the :gen_tcp.send by default. This is only specified by tests.
      send_fn: Map.get(init_arg, :send_fn, &:gen_tcp.send/2)
    }

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
  defp handle_new_data(%__MODULE__{socket: socket, send_fn: send_fn} = state) do
    case state.buffer do
      # Done, nothing more to handle.
      "" ->
        state

      _ ->
        {:ok, decoded, rest} = RESP.decode(state.buffer)

        # We only respond to Array requests, just drop other responses.
        state =
          case decoded do
            [_ | _] = request ->
              {new_state, %{data: data, encoding: encoding}} = parse_request(state, request)
              send_fn.(socket, RESP.encode(data, encoding))
              new_state

            _ ->
              Logger.error(
                "Got non-array request from client #{inspect(socket)}: #{state.buffer} -> #{inspect(decoded)}"
              )

              state
          end

        state = put_in(state.buffer, rest)
        handle_new_data(state)
    end
  end

  # TODO handle case insensitivity
  defp parse_request(state, ["PING"]), do: {state, simple_string_request("PONG")}
  defp parse_request(state, ["PING", arg]), do: {state, bulk_string_request(arg)}

  defp parse_request(state, ["ECHO", arg]), do: {state, bulk_string_request(arg)}

  defp parse_request(state = %Connection{}, ["GET", arg]) do
    value = Redis.KeyValueStore.get(arg) || ""
    {state, bulk_string_request(value)}
  end

  defp parse_request(state = %Connection{}, ["SET", key, value]) do
    Redis.KeyValueStore.set(key, value)
    {state, simple_string_request("OK")}
  end

  # Set with expiry specified.
  defp parse_request(state = %Connection{}, [
         "SET",
         key,
         value,
         "px",
         relative_timestamp_milliseconds
       ]) do
    # Get the epoch timestamp using the relative requested expiry. i.e. The expiry epoch/unix time is = now + provided expiry timestamp.
    expiry_timestamp_epoch_ms =
      System.os_time(:millisecond) + String.to_integer(relative_timestamp_milliseconds)

    Redis.KeyValueStore.set(key, value, expiry_timestamp_epoch_ms)
    {state, simple_string_request("OK")}
  end

  defp simple_string_request(input), do: %{data: input, encoding: :simple_string}
  defp bulk_string_request(input), do: %{data: input, encoding: :bulk_string}
  # defp array_request(input), do: %{data: input, encoding: :array}
end
