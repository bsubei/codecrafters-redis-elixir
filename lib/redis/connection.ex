defmodule Redis.Connection do
  @moduledoc """
  This Connection module encapsulates a TCP socket and represents a connection to another Redis client/server. All the functionality for handling messages over this socket are defined here.

  A Connection could be between us (a Redis server) and any of these:
  - a Redis client, in which case this server listens for requests and replies with a single response.
  - a Redis replica server if we're master, in which case we listen for a sync handshake from this replica and once that's done, we start relaying any updates to our KVStore over this Connection.
  - a Redis master server if we're a replica, in which case we initiate a sync handshake with the master. Afterwards, we listen to relayed updates (to which we do not reply) or ACK messages (to which we do reply).
  """
  use GenServer
  require Logger
  alias Redis.RESP
  alias Redis.ServerState

  # The handshake status must be one of these atoms:
  #
  # :not_started => this either means we're connected to a client and do not expect a handshake, or we're connected to a replica that hasn't initiated the handshake (at this point such a replica can't be distinguished from a regular client).
  #
  # Statuses when we're a replica connected to master:
  # :ping_sent => the initial ping has been sent and yet to be processed by the master.
  # :replconf_one_sent => the first replconf has been sent and yet to be processed by the master.
  # :replconf_two_sent => the second replconf has been sent and yet to be processed by the master.
  # :psync_sent => the PSYNC has been sent and yet to be processed by the master.
  # :awaiting_rdb => we are awaiting the RDB transfer from master.
  # :connected_to_master => the handshake has concluded and this connection is the "replication" connection with master. We expect to receive replication updates from master after this point.
  #
  # Statuses when we're a master connected to a replica:
  # :ping_received => the initial ping has been received and processed by us. We now identify this connection as being a replica instead of a regular client (we still process its client-like requests).
  # :replconf_one_received => the first replconf has been received and processed by us.
  # :replconf_two_received => the second replconf has been received and processed by us.
  # :connected_to_replica => the handshake has concluded and this connection is the replication connection with the replica. We will send replication updates to the replica after this point.

  @type t :: %__MODULE__{
          socket: :gen_tcp.socket(),
          send_fn: (:gen_tcp.socket(), iodata() ->
                      :ok
                      | {:error, :closed | {:timeout, binary() | :erlang.iovec()} | :inet.posix()}),
          handshake_status: atom(),
          # TODO the role of the server could change (replica becomes master), so we have to treat this "role" here as a cached value that might need to be updated.
          role: atom(),
          # If this Connection is between a master (us) and a replica (i.e. the handshake_status is :connected_to_replica), this field
          # stores the latest replication offset that the replica has informed us of when we asked it with "REPLCONF GETACK".
          replica_offset_count: integer() | nil,
          buffer: binary()
        }
  defstruct [:socket, :send_fn, :handshake_status, :role, replica_offset_count: nil, buffer: <<>>]

  @spec start_link(%{
          # The socket must always be specified.
          socket: :gen_tcp.socket(),
          handshake_status: atom(),
          role: atom()
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
      role: Map.get(init_arg, :role)
    }

    {:ok, state}
  end

  @impl true
  def handle_call(message, from, state)

  # If this is a Connection representing master (us) connected to a replica, reply with an internal request asking for this replica's latest offset count.
  def handle_call(
        :get_repl_offset,
        _from,
        %__MODULE__{handshake_status: :connected_to_replica} = state
      ) do
    {:reply, state.replica_offset_count, state}
  end

  @impl true
  def handle_info(message, state)

  def handle_info({:tcp, socket, data}, %__MODULE__{socket: socket} = state) do
    state = update_in(state.buffer, &(&1 <> data))
    state = handle_new_data(state)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, %__MODULE__{socket: socket} = state) do
    # Remove this connection from the Map of connected_replicas if it ever was in there.
    :ok = ServerState.remove_connected_replica(state)

    {:stop, :normal, state}
  end

  def handle_info({:tcp_error, socket, reason}, %__MODULE__{socket: socket} = state) do
    Logger.error("TCP connection error: #{inspect(reason)}")
    {:stop, :normal, state}
  end

  # TODO handle incomplete messages.
  @spec handle_new_data(%__MODULE__{}) :: %__MODULE__{}
  defp handle_new_data(%__MODULE__{} = state) do
    case state.buffer do
      # Done, nothing more to handle.
      "" ->
        state

      _ ->
        handle_new_data_impl(state)
    end
  end

  @spec handle_new_data_impl(%__MODULE__{}) :: %__MODULE__{}
  defp handle_new_data_impl(%__MODULE__{} = state) do
    # Handle the special case of reading the incoming RDB file (which is not RESP encoded).
    {new_state, rest} =
      if String.starts_with?(state.buffer, "$") and
           state.handshake_status == :awaiting_rdb do
        handle_incoming_rdb_dump(state)
      else
        # Handle the general case by decoding the RESP message and calling handle_request.
        case RESP.decode(state.buffer) do
          # If we can decode this request, then handle it, then recurse in case the buffer has more data to process.
          {:ok, decoded, rest} ->
            # TODO handle case insensitivity
            new_state = handle_request(state, decoded)
            {new_state, rest}

          # If we cannot decode this request, then check if it's an RDB transfer that we're expecting and process it.
          :error ->
            IO.puts("Got a request that we can't decode: #{state.buffer}. Just dropping it...")

            # Drop everything in the buffer since we can't process this message.
            {state, ""}
        end
      end

    # Recurse to handle more data.
    handle_new_data(put_in(new_state.buffer, rest))
  end

  @spec handle_incoming_rdb_dump(%__MODULE__{}) :: {%__MODULE__{}, binary()}
  defp handle_incoming_rdb_dump(%__MODULE__{} = state) do
    # Grab the length of the RDB data from the header.
    "$" <> msg_with_header = state.buffer

    case RESP.decode_positive_integer(msg_with_header) do
      {:ok, count, msg} ->
        # TODO parse RDB file and load it in.
        <<_rdb_bytes::binary-size(count), rest::binary>> = msg
        # For now, we do nothing with the RDB file. Just return our updated state.
        # Make sure we recurse with the rest of the message in case this TCP segment has multiple messages.
        {put_in(state.handshake_status, :connected_to_master), rest}

      :error ->
        IO.puts("Unable to parse RDB length in header! Dropping message...")
        {state, ""}
    end
  end

  # Return the new state after handling the request (possibly by replying over this Connection or other Connections).
  @spec handle_request(%__MODULE__{}, list(binary) | binary()) :: %__MODULE__{}

  ## Replica-only message handling (for replication updates). NOTE: we update our offset count based on these replication messages we get from master.
  # If we receive replication updates from master, apply them but do not reply.
  defp handle_request(
         %__MODULE__{handshake_status: :connected_to_master} = state,
         ["SET", key, value] = request
       ) do
    Redis.KeyValueStore.set(key, value)
    ServerState.add_byte_offset_count(get_request_encoded_length(request))
    state
  end

  # If we're a connected replica, reply to REPLCONF GETACK with the number of offset bytes so far (not including this request).
  defp handle_request(
         %__MODULE__{handshake_status: :connected_to_master} = state,
         ["REPLCONF", "GETACK", "*"] = request
       ) do
    num_bytes_offset = ServerState.get_byte_offset_count()

    :ok =
      send_message(state, array_request(["REPLCONF", "ACK", Integer.to_string(num_bytes_offset)]))

    ServerState.add_byte_offset_count(get_request_encoded_length(request))

    state
  end

  # NOTE: the order of this function relative to the other "overloads" is important, as it's a catch-all for the cases for messages coming from master.
  # If we're a connected replica, as a catch-all, do not reply to the messages from master unless it's a REPLCONF GETACK.
  defp handle_request(%__MODULE__{handshake_status: :connected_to_master} = state, request) do
    ServerState.add_byte_offset_count(get_request_encoded_length(request))
    state
  end

  ## Master-only message handling.

  # In the master-replica connections, handle REPLCONF ACK replies and store this offset so we know how up-to-date this replica is. These replica offsets will be used by the WAIT command.
  defp handle_request(%__MODULE__{handshake_status: :connected_to_replica} = state, [
         "REPLCONF",
         "ACK",
         num_bytes_offset
       ]) do
    num_bytes_offset = String.to_integer(num_bytes_offset)
    put_in(state.replica_offset_count, num_bytes_offset)
  end

  defp handle_request(%__MODULE__{role: :master} = state, ["WAIT", "0", _timeout_ms]) do
    :ok = send_message(state, integer_request("0"))
    state
  end

  # Wait (block the connection) until the timeout or until this many replicas are up-to-date, and reply with the number of replicas that are up-to-date.
  # See https://redis.io/docs/latest/commands/wait/
  defp handle_request(%__MODULE__{role: :master} = state, [
         "WAIT",
         num_required_replicas,
         timeout_ms
       ]) do
    num_required_replicas = String.to_integer(num_required_replicas)

    expiry_timestamp_epoch_ms =
      System.os_time(:millisecond) + String.to_integer(timeout_ms)

    server_state = ServerState.get_state()

    connected_replicas = server_state.connected_replicas

    # TODO actually the master's (our) offset is never updated. I only implemented updating it for the replica's connection to master.
    current_master_offset = server_state.server_info.replication.master_repl_offset

    # TODO timeout of 0 means block forever

    # TODO send these in parallel
    # TODO I read somewhere that I shouldn't send these if there was "no previous write operations pending". Does that mean I should check if the replica offsets are already up to date?
    # Send REPLCONF GETACK to each replica directly using send_message_on_socket. Each of them will reply
    # to their respective master-replica Connections, which updates their Connection to contain the latest offset count.
    Enum.map(Map.keys(connected_replicas), fn replica_socket ->
      send_message_on_socket(
        replica_socket,
        state.send_fn,
        array_request(["REPLCONF", "GETACK", "*"])
      )
    end)

    # Until the timeout hits, keep reading the replica offsets until we have the minimum required number of replicas reach the required master repl offset.
    case wait_for_num_replicas_to_reach_offset(
           Map.values(connected_replicas),
           num_required_replicas,
           current_master_offset,
           expiry_timestamp_epoch_ms,
           0
         ) do
      # Actually reply with the result either way, even if we time out.
      {:ok, num_up_to_date_replicas} ->
        IO.puts("WE GOT #{num_up_to_date_replicas}/#{num_required_replicas}!!!")
        :ok = send_message(state, integer_request(num_up_to_date_replicas))

      {:timeout, num_up_to_date_replicas} ->
        IO.puts("TIME OUT! WE ONLY GOT #{num_up_to_date_replicas}/#{num_required_replicas}!!!")
        :ok = send_message(state, integer_request(num_up_to_date_replicas))
    end

    state
  end

  ## Replies to handshake messages if we're a replica.

  # If we get a simple string PONG back and we're a replica and we're expecting this reply, continue the handshake by sending the first replconf message.
  defp handle_request(%__MODULE__{handshake_status: :ping_sent} = state, "PONG") do
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
         %__MODULE__{handshake_status: :replconf_one_sent} = state,
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
         %__MODULE__{handshake_status: :replconf_two_sent} = state,
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
         %__MODULE__{handshake_status: :psync_sent} = state,
         <<"FULLRESYNC", _rest::binary>>
       ) do
    # TODO eventually do something with the master replid given to us.
    put_in(state.handshake_status, :awaiting_rdb)
  end

  # NOTE: receiving the RDB file is handled above in handle_new_data_impl since that message is not RESP-encoded.

  ## Replies to handshake messages if we're master.

  # If we get a REPLCONF back and we're a master and we're expecting this reply, continue the handshake by replying with OK.
  defp handle_request(
         %__MODULE__{handshake_status: :ping_received} = state,
         ["REPLCONF", "listening-port", _port]
       ) do
    :ok = send_message(state, simple_string_request("OK"))
    put_in(state.handshake_status, :replconf_one_received)
  end

  # If we get a second REPLCONF back and we're a master and we're expecting this reply, continue the handshake by replying with OK.
  defp handle_request(
         %__MODULE__{handshake_status: :replconf_one_received} = state,
         ["REPLCONF", "capa", "psync2"]
       ) do
    :ok = send_message(state, simple_string_request("OK"))
    put_in(state.handshake_status, :replconf_two_received)
  end

  # If we get a PSYNC back and we're a master and we're expecting this reply, finish the handshake by replying with FULLRESYNC and then the RDB file. We
  # consider the replica to be fully connected at this point, because we don't expect replies to our FULLRESYNC and RDB messages, and we can safely send replication updates after this point since the messages are in a queue.
  defp handle_request(
         %__MODULE__{handshake_status: :replconf_two_received} = state,
         ["PSYNC", "?", "-1"]
       ) do
    master_replid = ServerState.get_state().server_info.replication.master_replid
    first_reply = simple_string_request("FULLRESYNC #{master_replid} 0")
    :ok = send_message(state, first_reply)

    # TODO for now, creating the RDB file is done synchronously here since it's just a hard-coded string. Eventually, creating the RDB file should be done asynchronously and then the reply created once that's ready.
    rdb_contents = Redis.RDB.get_rdb_file()
    rdb_byte_count = byte_size(rdb_contents)
    second_reply = "$#{rdb_byte_count}#{RESP.crlf()}#{rdb_contents}"
    :ok = send_message(state, second_reply)

    # Mark this replica as connected and ready to receive write updates.
    ServerState.add_connected_replica(state, self())

    put_in(state.handshake_status, :connected_to_replica)
  end

  ## Handle client requests.
  # Always reply to any PING with a PONG. But also handle the case when this could be the start of a handshake from a replica to us (if we're master).
  defp handle_request(%__MODULE__{role: :master} = state, ["PING"]) do
    :ok = send_message(state, simple_string_request("PONG"))
    %__MODULE__{state | handshake_status: :ping_received}
  end

  defp handle_request(state, ["PING"]) do
    :ok = send_message(state, simple_string_request("PONG"))
    state
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
    send_message_to_connected_replicas(state, request)

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
    send_message_to_connected_replicas(state, ["SET", key, value])

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

  ## Helpers and utility functions.
  @spec wait_for_num_replicas_to_reach_offset(
          list(pid()),
          integer(),
          integer(),
          integer(),
          integer()
        ) ::
          {:ok, integer()} | {:timeout, integer()}
  defp wait_for_num_replicas_to_reach_offset(
         replica_conn_pids,
         num_required_replicas,
         desired_master_offset,
         expiry_timestamp_epoch_ms,
         num_up_to_date_replicas_so_far
       ) do
    # Check for timeout, otherwise continue trying.
    if System.os_time(:millisecond) >= expiry_timestamp_epoch_ms do
      {:timeout, num_up_to_date_replicas_so_far}
    else
      # Get the latest offset for each replica, then get how many of them are up-to-date.
      num_up_to_date_replicas_so_far =
        Enum.map(replica_conn_pids, fn replica_conn_pid ->
          GenServer.call(replica_conn_pid, :get_repl_offset)
        end)
        |> Enum.count(&(&1 >= desired_master_offset))

      # Keep trying if we don't have enough up-to-date offsets.
      if num_up_to_date_replicas_so_far < num_required_replicas do
        wait_for_num_replicas_to_reach_offset(
          replica_conn_pids,
          num_required_replicas,
          desired_master_offset,
          expiry_timestamp_epoch_ms,
          num_up_to_date_replicas_so_far
        )
      else
        {:ok, num_up_to_date_replicas_so_far}
      end
    end
  end

  defp send_message(%__MODULE__{socket: socket, send_fn: send_fn}, message) do
    send_message_on_socket(socket, send_fn, message)
  end

  defp send_message_on_socket(socket, send_fn, message) do
    case send_fn.(socket, message) do
      :ok -> :ok
      {:error, :timeout} -> send_message_on_socket(socket, send_fn, message)
      # TODO is this actually ok in all cases?
      {:error, :closed} -> :ok
    end
  end

  defp send_message_to_connected_replicas(state, message) do
    socket = state.socket

    Enum.map(Map.keys(ServerState.get_state().connected_replicas), fn
      ^socket ->
        nil

      replica_socket ->
        send_message_on_socket(replica_socket, state.send_fn, array_request(message))
    end)
  end

  defp simple_string_request(input), do: RESP.encode(input, :simple_string)
  defp bulk_string_request(input), do: RESP.encode(input, :bulk_string)

  defp integer_request(input) when is_integer(input),
    do: integer_request(Integer.to_string(input))

  defp integer_request(input), do: RESP.encode(input, :integer)
  defp array_request(input) when is_list(input), do: RESP.encode(input, :array)

  defp get_request_encoded_length(request) do
    case request do
      [_ | _] ->
        IO.iodata_length(array_request(request))

        # TODO support other kinds of requests later. Right now I don't expect there to be other types used.
    end
  end
end
