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
  # NOTE: there is no "psync_received" because at this point, the master considers the handshake to be complete and the replica to be :connected, because it does not expect replies to the FULLRESYNC and RDB messages.

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
  defp handle_new_data(%__MODULE__{} = state) do
    case state.buffer do
      # Done, nothing more to handle.
      "" ->
        state

      _ ->
        case RESP.decode(state.buffer) do
          # If we can decode this request, then handle it, then recurse in case the buffer has more data to process.
          {:ok, decoded, rest} ->
            # TODO handle case insensitivity
            new_state = handle_request(state, decoded)
            handle_new_data(put_in(new_state.buffer, rest))

          # If we cannot decode this request, then check if it's an RDB transfer that we're expecting and process it.
          :error ->
            if String.starts_with?(state.buffer, "$") and state.role == :slave and
                 state.handshake_status == :awaiting_rdb do
              # TODO parse RDB file and load it in.
              # For now, we do nothing with the RDB file. We also assume there is no more data as part of state.buffer.
              # Just return our updated state.
              put_in(state.handshake_status, :connected)
            else
              IO.puts("Got a request that we can't decode: #{state.buffer}")
              state
            end
        end
    end
  end

  # Return the new state after handling the request (possibly by replying over this Connection or other Connections).
  @spec handle_request(%__MODULE__{}, list(binary) | binary()) :: %__MODULE__{}

  # Always reply to any PING with a PONG. But also handle the case when this could be the start of a handshake from a replica to us (if we're master).
  defp handle_request(state, ["PING"]) do
    :ok = send_message(state, simple_string_request("PONG"))

    if state.handshake_status == :not_started and state.role == :master do
      %__MODULE__{state | handshake_status: :ping_received}
    else
      state
    end
  end

  # Echo back the args if given a PING with args. Do not treat this as the start of a handshake.
  defp handle_request(state, ["PING", arg]) do
    :ok = send_message(state, bulk_string_request(arg))
    state
  end

  # Echo back the args in our reply.
  defp handle_request(state, ["ECHO", arg]) do
    :ok = send_message(state, bulk_string_request(arg))
    state
  end

  # Get the requested key's value from our key-value store and make that our reply.
  defp handle_request(state, ["GET", arg]) do
    value = Redis.KeyValueStore.get(arg) || ""
    :ok = send_message(state, bulk_string_request(value))
    state
  end

  # Set the key and value in our key-value store and reply with OK.
  defp handle_request(state, ["SET", key, value] = request) do
    # TODO handle retries or errors and somehow revert state maybe?
    Redis.KeyValueStore.set(key, value)
    :ok = send_message(state, simple_string_request("OK"))

    # Relay any updates to all connected replicas except this current Connection.
    Enum.map(ServerState.get_state().connected_replicas, fn
      ^state -> nil
      replica_conn -> send_message(replica_conn, array_request(request))
    end)

    state
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
    :ok = send_message(state, simple_string_request("OK"))

    # Relay any updates to all connected replicas except this current Connection. Note that we don't include the expiry terms in here.
    Enum.map(ServerState.get_state().connected_replicas, fn
      ^state -> nil
      replica_conn -> send_message(replica_conn, array_request(["SET", key, value]))
    end)

    state
  end

  # Reply with the contents of all the ServerInfo sections we have.
  defp handle_request(state, ["INFO"]) do
    server_info_string = Redis.ServerInfo.to_string(ServerState.get_state().server_info)
    :ok = send_message(state, bulk_string_request(server_info_string))
    state
  end

  # Reply with the contents of the specified ServerInfo section.
  defp handle_request(state, ["INFO" | rest]) do
    server_info_string = Redis.ServerInfo.to_string(ServerState.get_state().server_info, rest)
    :ok = send_message(state, bulk_string_request(server_info_string))
    state
  end

  ## Replies to handshake messages if we're a replica.

  # If we get a simple string PONG back and we're a replica and we're expecting this reply, continue the handshake by sending the first replconf message.
  defp handle_request(%__MODULE__{role: :slave, handshake_status: :ping_sent} = state, "PONG") do
    :ok =
      send_message(
        state,
        array_request([
          "REPLCONF",
          "listening-port",
          Integer.to_string(ServerState.get_state().cli_config.port)
        ])
      )

    put_in(state.handshake_status, :replconf_one_sent)
  end

  # If we get a simple string OK back and we're a replica and we just sent the first replconf, continue the handshake by sending the second replconf message.
  defp handle_request(
         %__MODULE__{role: :slave, handshake_status: :replconf_one_sent} = state,
         "OK"
       ) do
    :ok =
      send_message(
        state,
        array_request(["REPLCONF", "capa", "psync2"])
      )

    put_in(state.handshake_status, :replconf_two_sent)
  end

  # If we get a simple string OK back and we're a replica and we just sent the second replconf, continue the handshake by sending the PSYNC message.
  defp handle_request(
         %__MODULE__{role: :slave, handshake_status: :replconf_two_sent} = state,
         "OK"
       ) do
    :ok =
      send_message(
        state,
        array_request(["PSYNC", "?", "-1"])
      )

    put_in(state.handshake_status, :psync_sent)
  end

  # If we get a simple string FULLRESYNC back and we're a replica and we just sent the psync, continue the handshake by updating our state and not sending anything (we're awaiting the RDB transfer).
  defp handle_request(
         %__MODULE__{role: :slave, handshake_status: :psync_sent} = state,
         <<"FULLRESYNC", _rest::binary>>
       ) do
    # TODO eventually do something with the master replid given to us.
    put_in(state.handshake_status, :awaiting_rdb)
  end

  # NOTE: receiving the RDB file is handled above in handle_new_data since that message is not RESP-encoded.

  ## Replies to handshake messages if we're master.

  # If we get a REPLCONF back and we're a master and we're expecting this reply, continue the handshake by replying with OK.
  defp handle_request(
         %__MODULE__{role: :master, handshake_status: :ping_received} = state,
         ["REPLCONF", "listening-port", _port]
       ) do
    :ok = send_message(state, simple_string_request("OK"))
    put_in(state.handshake_status, :replconf_one_received)
  end

  # If we get a second REPLCONF back and we're a master and we're expecting this reply, continue the handshake by replying with OK.
  defp handle_request(
         %__MODULE__{role: :master, handshake_status: :replconf_one_received} = state,
         ["REPLCONF", "capa", "psync2"]
       ) do
    :ok = send_message(state, simple_string_request("OK"))
    put_in(state.handshake_status, :replconf_two_received)
  end

  # If we get a PSYNC back and we're a master and we're expecting this reply, finish the handshake by replying with FULLRESYNC and then the RDB file. We
  # consider the replica to be fully connected at this point, because we don't expect replies to our FULLRESYNC and RDB messages, and we can safely send replication updates after this point since the messages are in a queue.
  defp handle_request(
         %__MODULE__{role: :master, handshake_status: :replconf_two_received} = state,
         ["PSYNC", "?", "-1"]
       ) do
    master_replid = ServerState.get_state().server_info.replication.master_replid
    first_reply = simple_string_request("FULLRESYNC #{master_replid} 0")
    :ok = send_message(state, first_reply)

    # TODO for now, "reading" the RDB file is done synchronously here since it's just a hard-coded string. Eventually, creating the RDB file should be done asynchronously and then the reply created once that's ready.
    rdb_contents = Redis.RDB.get_rdb_file()
    rdb_byte_count = byte_size(rdb_contents)
    second_reply = "$#{rdb_byte_count}#{RESP.crlf()}#{rdb_contents}"
    :ok = send_message(state, second_reply)

    # Mark this replica as connected and ready to receive write updates.
    ServerState.add_connected_replica(state)

    put_in(state.handshake_status, :connected)
  end

  defp send_message(%__MODULE__{socket: socket, send_fn: send_fn} = state, message) do
    case send_fn.(socket, message) do
      :ok -> :ok
      {:error, :timeout} -> send_message(state, message)
    end
  end

  defp simple_string_request(input), do: RESP.encode(input, :simple_string)
  defp bulk_string_request(input), do: RESP.encode(input, :bulk_string)
  defp array_request(input) when is_list(input), do: RESP.encode(input, :array)
end
