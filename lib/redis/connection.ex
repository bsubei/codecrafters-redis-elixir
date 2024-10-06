defmodule Redis.Connection do
  @moduledoc """
  This Connection module defines the process that handles a single Redis client by listening for a request and
  replying with a response.
  """
  use GenServer
  require Logger
  alias Redis.RESP
  alias Redis.Connection

  # TODO this kvstore is only local to this connection. This should be made into its own GenServer or task to store the state globally across all connections in the server.
  defstruct [:socket, :send_fn, buffer: <<>>, kvstore: %{}]

  @spec start_link(%{
          # The socket must always be specified.
          socket: :gen_tcp.socket(),
          # The send_fn is optional and will default to :gen_tcp.send/2 if not specified.
          send_fn: (port() | atom(), iodata() -> :ok | {:error, term()}) | nil
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
  defp parse_request(state, ["PING"]), do: {state, make_simple_string("PONG")}
  defp parse_request(state, ["PING", arg]), do: {state, make_array([arg])}

  defp parse_request(state, ["ECHO", arg]), do: {state, make_array([arg])}

  defp parse_request(state = %Connection{}, ["GET", arg]) do
    case Map.fetch(state.kvstore, arg) do
      {:ok, value} -> {state, make_array([value])}
      :error -> {state, make_bulk_string("")}
    end
  end

  defp parse_request(state = %Connection{}, ["SET", key, value]) do
    {put_in(state.kvstore, Map.put(state.kvstore, key, value)), make_simple_string("OK")}
  end

  defp make_simple_string(input), do: %{data: input, encoding: :simple_string}
  defp make_bulk_string(input), do: %{data: input, encoding: :bulk_string}
  defp make_array(input), do: %{data: input, encoding: :array}
end
