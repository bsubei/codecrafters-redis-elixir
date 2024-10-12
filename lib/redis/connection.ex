defmodule Redis.Connection do
  @moduledoc """
  This Connection module encapsulates a TCP socket and represents a connection to another Redis client/server. All the functionality for handling messages over this socket are defined here.

  A Connection could be to any of these:
  - a Redis client, in which case this server listens for requests and replies with a single response.
  - a Redis replica server if we're master, in which case we listen for a sync handshake from this replica and once that's done, we start relaying any updates to our KVStore over this Connection.
  - a Redis master server if we're a replica, in which case we initiate a sync handshake with the master. Afterwards, we listen to relayed updates (to which we do not reply).
  """
  use GenServer
  require Logger
  alias Redis.RESP
  alias Redis.ServerState

  # The handshake status must be one of these atoms:
  #
  # :not_started => this either means we're connected to a client and do not expect a handshake, or we're connected to a replica that hasn't initiated the handshake.
  # :connected => this either means we're a replica that's connected to master (and everything's up-to-date), or it means that we're a master and the connection is to a connected up-to-date replica.
  #
  # Statuses when we're a replica connected to master:
  # :ping_sent => the initial ping has been sent and yet to be processed by the master.
  # :replconf_one_sent => the first replconf has been sent and yet to be processed by the master.
  # :replconf_two_sent => the second replconf has been sent and yet to be processed by the master.
  # :psync_sent => the PSYNC has been sent and yet to be processed by the master.
  # :awaiting_rdb => we are awaiting the RDB transfer from master.
  #
  # Statuses when we're a master connected to a replica:
  # :ping_received => the initial ping has been received and processed by us. We now identify this connection as being a replica instead of a regular client (we still process its client-like requests).
  # :replconf_one_received => the first replconf has been received and processed by us.
  # :replconf_two_received => the second replconf has been received and processed by us.
  # :psync_received => the PSYNC has been received and processed by us. We are now sending the RDB to the replica.

  @type t :: %__MODULE__{
          socket: :gen_tcp.socket(),
          send_fn: (:gen_tcp.socket(), iodata() ->
                      :ok
                      | {:error, :closed | {:timeout, binary() | :erlang.iovec()} | :inet.posix()}),
          handshake_status: atom(),
          role: :master | :slave,
          buffer: binary()
        }
  defstruct [:socket, :send_fn, :handshake_status, :role, buffer: <<>>]

  @spec start_link(%{
          # The socket must always be specified.
          socket: :gen_tcp.socket(),
          handshake_status: atom()
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
      send_fn: Map.get(init_arg, :send_fn, &:gen_tcp.send/2),
      handshake_status: Map.get(init_arg, :handshake_status),
      role: ServerState.get_state().server_info.replication.role
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

        # TODO handle case insensitivity
        state =
          case handle_request(state, decoded) do
            # This request warrants a reply.
            {new_state, %{data: data, encoding: encoding}} ->
              send_fn.(socket, RESP.encode(data, encoding))
              new_state

            # This request does not need a reply.
            new_state ->
              new_state
          end

        state = put_in(state.buffer, rest)
        handle_new_data(state)
    end
  end

  # Always reply to any PING with a PONG. But also handle the case when this could be the start of a handshake from a replica to us (if we're master).
  defp handle_request(state, ["PING"]) do
    new_state =
      if state.handshake_status == :not_started and state.role == :master do
        %__MODULE__{state | handshake_status: :ping_received}
      else
        state
      end

    {new_state, simple_string_request("PONG")}
  end

  # Echo back the args if given a PING with args. Do not treat this as the start of a handshake.
  defp handle_request(state, ["PING", arg]), do: {state, bulk_string_request(arg)}

  # Echo back the args in our reply.
  defp handle_request(state, ["ECHO", arg]), do: {state, bulk_string_request(arg)}

  # Get the requested key's value from our key-value store and make that our reply.
  defp handle_request(state, ["GET", arg]) do
    value = Redis.KeyValueStore.get(arg) || ""
    {state, bulk_string_request(value)}
  end

  # Set the key and value in our key-value store and reply with OK.
  defp handle_request(state, ["SET", key, value]) do
    Redis.KeyValueStore.set(key, value)
    {state, simple_string_request("OK")}
  end

  # Set with expiry specified. Only handling the "px" case for now (relative expiry in milliseconds).
  defp handle_request(state, [
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

  # Reply with the contents of all the ServerInfo sections we have.
  defp handle_request(state, ["INFO"]) do
    {state, bulk_string_request(Redis.ServerInfo.to_string(ServerState.get_state().server_info))}
  end

  # Reply with the contents of the specified ServerInfo section.
  defp handle_request(state, ["INFO" | rest]) do
    {state,
     bulk_string_request(Redis.ServerInfo.to_string(ServerState.get_state().server_info, rest))}
  end

  ## Replies to handshake messages

  # If we get a simple string PONG back and we're a replica and we're expecting this reply, continue the handshake by sending the first replconf message.
  defp handle_request(%__MODULE__{role: :slave, handshake_status: :ping_sent} = _state, "PONG") do
    # TODO send replconf
  end

  # If we get a simple string OK back and we're a replica and we just sent the first replconf, continue the handshake by sending the second replconf message.
  defp handle_request(
         %__MODULE__{role: :slave, handshake_status: :replconf_one_sent} = _state,
         "OK"
       ) do
    # TODO send second replconf
  end

  # If we get a simple string OK back and we're a replica and we just sent the second replconf, continue the handshake by sending the PSYNC message.
  defp handle_request(
         %__MODULE__{role: :slave, handshake_status: :replconf_two_sent} = _state,
         "OK"
       ) do
    # TODO send PSYNC
  end

  # If we get a simple string FULLRESYNC back and we're a replica and we just sent the psync, continue the handshake by updating our state and not sending anything (we're awaiting the RDB transfer).
  defp handle_request(
         %__MODULE__{role: :slave, handshake_status: :psync_sent} = _state,
         "OK"
       ) do
    # TODO send PSYNC
  end

  defp simple_string_request(input), do: %{data: input, encoding: :simple_string}
  defp bulk_string_request(input), do: %{data: input, encoding: :bulk_string}
  # defp array_request(input), do: %{data: input, encoding: :array}
end
