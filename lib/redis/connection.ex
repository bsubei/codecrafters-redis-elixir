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
  alias Redis.KeyValueStore

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

  @enforce_keys [:socket, :send_fn, :handshake_status, :role]
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
          replica_offset_count: integer(),
          buffer: binary()
        }
  defstruct [:socket, :send_fn, :handshake_status, :role, replica_offset_count: 0, buffer: <<>>]

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
      handshake_status: Map.get(init_arg, :handshake_status, :not_started),
      role: Map.get(init_arg, :role, :master)
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
            decoded =
              case decoded do
                # Make the first element (the command) uppercased so we're not case sensitive.
                [first | _rest] -> [String.upcase(first)] ++ Enum.drop(decoded, 1)
                _ -> String.upcase(decoded)
              end

            {:ok, new_state} = handle_request(state, decoded)
            {new_state, rest}

          # If we cannot decode this request, then check if it's an RDB transfer that we're expecting and process it.
          :error ->
            Logger.error(
              "Got a request that we can't decode: #{state.buffer}. Just dropping it..."
            )

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
        Logger.error("Unable to parse RDB length in header! Dropping message...")
        {state, ""}
    end
  end

  # Return the new state after handling the request (possibly by replying over this Connection or other Connections).
  @spec handle_request(%__MODULE__{}, list(binary) | binary()) :: {:ok, %__MODULE__{}} | :error

  ## Replica-only message handling (for replication updates). NOTE: we update our offset count based on these replication messages we get from master.
  # If we receive replication updates from master, apply them but do not reply.
  defp handle_request(
         %__MODULE__{handshake_status: :connected_to_master} = state,
         ["SET", key, value] = request
       ) do
    KeyValueStore.set(key, value)
    ServerState.add_byte_offset_count(get_request_encoded_length(request))
    {:ok, state}
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

    {:ok, state}
  end

  # NOTE: the order of this function relative to the other "overloads" is important, as it's a catch-all for the cases for messages coming from master.
  # If we're a connected replica, as a catch-all, do not reply to the messages from master unless it's a REPLCONF GETACK.
  defp handle_request(%__MODULE__{handshake_status: :connected_to_master} = state, request) do
    ServerState.add_byte_offset_count(get_request_encoded_length(request))
    {:ok, state}
  end

  ## Master-only message handling. Every time we send something to the replica, we also add to our repl offset (and so does the replica if it receives these).

  # In the master-replica connections, handle REPLCONF ACK replies and store this offset so we know how up-to-date this replica is. These replica offsets will be used by the WAIT command.
  defp handle_request(%__MODULE__{handshake_status: :connected_to_replica} = state, [
         "REPLCONF",
         "ACK",
         num_bytes_offset
       ]) do
    num_bytes_offset = String.to_integer(num_bytes_offset)
    {:ok, put_in(state.replica_offset_count, num_bytes_offset)}
  end

  defp handle_request(%__MODULE__{role: :master} = state, ["WAIT", "0", _timeout_ms]) do
    :ok = send_message(state, integer_request("0"))
    {:ok, state}
  end

  # Wait (block the connection) until the timeout or until this many replicas are up-to-date, and reply with the number of replicas that are up-to-date.
  # See https://redis.io/docs/latest/commands/wait/
  defp handle_request(%__MODULE__{role: :master} = state, [
         "WAIT",
         num_required_replicas,
         timeout_ms
       ]) do
    expiry_timestamp_epoch_ms =
      System.os_time(:millisecond) + String.to_integer(timeout_ms)

    server_state = ServerState.get_state()

    connected_replicas = server_state.connected_replicas

    num_required_replicas = String.to_integer(num_required_replicas)
    num_connected_replicas = map_size(connected_replicas)

    # This is the repl offset that we expect the replicas to have if they are "up-to-date".
    current_master_offset = server_state.server_info.replication.master_repl_offset

    num_up_to_date_replicas =
      Enum.map(Map.values(connected_replicas), fn replica_conn_pid ->
        GenServer.call(replica_conn_pid, :get_repl_offset)
      end)
      |> Enum.count(&(&1 == current_master_offset))

    # Check if all the replicas are up to date or if we have enough already (nothing has been sent since the last time we checked).
    if num_up_to_date_replicas == num_connected_replicas or
         num_up_to_date_replicas >= num_required_replicas do
      :ok = send_message(state, integer_request(num_up_to_date_replicas))
    else
      # Otherwise, we have to fetch the latest replica offsets and keep checking until the replicas are up-to-date.
      # TODO timeout of 0 means block forever, handle that.
      # TODO consider sending these in parallel using spawn() or Task.async_stream().
      # Send REPLCONF GETACK to each replica directly using send_message_on_socket. Each of them will reply
      # to their respective master-replica Connections, which updates their Connection to contain the latest offset count.
      getack_request = ["REPLCONF", "GETACK", "*"]

      Enum.map(Map.keys(connected_replicas), fn replica_socket ->
        send_message_on_socket(
          replica_socket,
          state.send_fn,
          array_request(getack_request)
        )
      end)

      # Until the timeout hits, keep reading the replica offsets until we have the minimum required number of replicas reach the required master repl offset.
      case wait_for_num_replicas_to_reach_offset(
             Map.values(connected_replicas),
             num_required_replicas,
             current_master_offset,
             expiry_timestamp_epoch_ms,
             num_up_to_date_replicas
           ) do
        # Actually reply with the result either way, even if we time out.
        {:ok, num_up_to_date_replicas} ->
          :ok = send_message(state, integer_request(num_up_to_date_replicas))

        {:timeout, num_up_to_date_replicas} ->
          :ok = send_message(state, integer_request(num_up_to_date_replicas))
      end

      ServerState.add_byte_offset_count(get_request_encoded_length(getack_request))
    end

    {:ok, state}
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

    {:ok, put_in(state.handshake_status, :replconf_one_sent)}
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

    {:ok, put_in(state.handshake_status, :replconf_two_sent)}
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

    {:ok, put_in(state.handshake_status, :psync_sent)}
  end

  # If we get a simple string FULLRESYNC back and we're a replica and we just sent the psync, continue the handshake by updating our state and not sending anything (we're awaiting the RDB transfer).
  defp handle_request(
         %__MODULE__{handshake_status: :psync_sent} = state,
         <<"FULLRESYNC", _rest::binary>>
       ) do
    # TODO eventually do something with the master replid given to us.
    {:ok, put_in(state.handshake_status, :awaiting_rdb)}
  end

  # NOTE: receiving the RDB file is handled above in handle_new_data_impl since that message is not RESP-encoded.

  ## Replies to handshake messages if we're master.

  # If we get a REPLCONF back and we're a master and we're expecting this reply, continue the handshake by replying with OK.
  defp handle_request(
         %__MODULE__{handshake_status: :ping_received} = state,
         ["REPLCONF", "listening-port", _port]
       ) do
    :ok = send_message(state, simple_string_request("OK"))
    {:ok, put_in(state.handshake_status, :replconf_one_received)}
  end

  # If we get a second REPLCONF back and we're a master and we're expecting this reply, continue the handshake by replying with OK.
  defp handle_request(
         %__MODULE__{handshake_status: :replconf_one_received} = state,
         ["REPLCONF", "capa", "psync2"]
       ) do
    :ok = send_message(state, simple_string_request("OK"))
    {:ok, put_in(state.handshake_status, :replconf_two_received)}
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

    {:ok, put_in(state.handshake_status, :connected_to_replica)}
  end

  ## Handle client requests.
  # Always reply to any PING with a PONG. But also handle the case when this could be the start of a handshake from a replica to us (if we're master).
  defp handle_request(%__MODULE__{role: :master} = state, ["PING"]) do
    :ok = send_message(state, simple_string_request("PONG"))
    {:ok, %__MODULE__{state | handshake_status: :ping_received}}
  end

  defp handle_request(state, ["PING"]) do
    :ok = send_message(state, simple_string_request("PONG"))
    {:ok, state}
  end

  # Echo back the args if given a PING with args. Do not treat this as the start of a handshake.
  defp handle_request(state, ["PING", arg]) do
    :ok = send_message(state, bulk_string_request(arg))
    {:ok, state}
  end

  # Echo back the args in our reply.
  defp handle_request(state, ["ECHO", arg]) do
    :ok = send_message(state, bulk_string_request(arg))
    {:ok, state}
  end

  # Get the requested key's value from our key-value store and make that our reply.
  defp handle_request(state, ["GET", key]) do
    # Note that replicas don't bother checking for expiry because the master will tell them to expire entries instead.
    value =
      case state.role do
        :master -> KeyValueStore.get(key)
        :slave -> KeyValueStore.get(key, :no_expiry)
      end

    data = if value, do: value.data, else: ""
    :ok = send_message(state, bulk_string_request(data))
    {:ok, state}
  end

  # Set the key and value in our key-value store and reply with OK.
  defp handle_request(state, ["SET", key, value] = request) do
    # TODO handle retries or errors and somehow revert state maybe?
    KeyValueStore.set(key, value)
    :ok = send_message(state, simple_string_request("OK"))

    # Relay any updates to all connected replicas except this current Connection, and make sure to add it to our repl offset.
    send_message_to_connected_replicas(state, request)
    ServerState.add_byte_offset_count(get_request_encoded_length(request))

    {:ok, state}
  end

  # Set with expiry specified. Only handling the "px" case for now (relative expiry in milliseconds).
  defp handle_request(
         state,
         [
           "SET",
           key,
           value,
           "px",
           relative_timestamp_milliseconds
         ] = request
       ) do
    # Get the epoch timestamp using the relative requested expiry. i.e. The expiry epoch/unix time is = now + provided expiry timestamp.
    expiry_timestamp_epoch_ms =
      System.os_time(:millisecond) + String.to_integer(relative_timestamp_milliseconds)

    KeyValueStore.set(key, value, expiry_timestamp_epoch_ms)
    :ok = send_message(state, simple_string_request("OK"))

    # Relay any updates to all connected replicas except this current Connection. Note that we don't include the expiry terms in here.
    send_message_to_connected_replicas(state, ["SET", key, value])
    ServerState.add_byte_offset_count(get_request_encoded_length(request))

    {:ok, state}
  end

  # Reply with the contents of all the ServerInfo sections we have.
  defp handle_request(state, ["INFO"]) do
    server_info_string = Redis.ServerInfo.to_string(ServerState.get_state().server_info)
    :ok = send_message(state, bulk_string_request(server_info_string))
    {:ok, state}
  end

  # Reply with the contents of the specified ServerInfo section.
  defp handle_request(state, ["INFO" | rest]) do
    server_info_string = Redis.ServerInfo.to_string(ServerState.get_state().server_info, rest)
    :ok = send_message(state, bulk_string_request(server_info_string))
    {:ok, state}
  end

  defp handle_request(state, ["TYPE", key]) do
    value =
      case state.role do
        :master -> KeyValueStore.get(key)
        :slave -> KeyValueStore.get(key, :no_expiry)
      end

    type = if value, do: value.type, else: :none
    :ok = send_message(state, simple_string_request(Atom.to_string(type)))

    {:ok, state}
  end

  # TODO support optional arguments eventually
  # Append a new entry to a stream when given an explicit stream key.
  defp handle_request(state, ["XADD", stream_key, entry_id | rest]) do
    # First resolve this entry id (i.e. handle any "*").
    stream =
      case KeyValueStore.get(stream_key, :no_expiry) do
        nil -> %Redis.Stream{}
        value -> value.data
      end

    {:ok, resolved_entry_id} = Redis.Stream.resolve_entry_id(stream, entry_id)

    handle_request_xadd(state, ["XADD", stream_key, resolved_entry_id | rest])
  end

  defp handle_request(state, ["XRANGE", stream_key, start_entry_id, end_entry_id]) do
    # Resolve the entry ids, but don't allow "*".
    case [start_entry_id, end_entry_id] do
      ["*", _] ->
        :error

      [_, "*"] ->
        :error

      _ ->
        stream =
          case KeyValueStore.get(stream_key, :no_expiry) do
            nil -> %Redis.Stream{}
            value -> value.data
          end

        {:ok, resolved_start_id} = Redis.Stream.resolve_entry_id(stream, start_entry_id)
        {:ok, resolved_end_id} = Redis.Stream.resolve_entry_id(stream, end_entry_id)

        reply_message =
          case KeyValueStore.get(stream_key, :no_expiry) do
            nil ->
              bulk_string_request("")

            %Redis.Value{type: :stream, data: stream} ->
              Redis.Stream.get_entries_range(stream, resolved_start_id, resolved_end_id)
              |> stream_entries_to_resp()

            # TODO correctly raise errors
            _ ->
              :error
          end

        :ok = send_message(state, reply_message)

        {:ok, state}
    end
  end

  # XREAD is like an XRANGE where it only takes a start entry id (which is exclusive) and implicitly uses "-" for the end entry id.
  # TODO "streams" is case sensitive but it shouldn't be
  defp handle_request(state, ["XREAD", "streams" | list_of_args]) do
    stream_key_start_entry_id_pairs = make_pairs_from_separated_elements(list_of_args)

    key_to_start_and_end_pairs =
      stream_key_start_entry_id_pairs
      |> Enum.map(fn {stream_key, start_entry_id} ->
        stream =
          case KeyValueStore.get(stream_key, :no_expiry) do
            nil -> %Redis.Stream{}
            value -> value.data
          end

        # We "resolve" the start entry id by finding the next entry id since it's exclusive.
        case Redis.Stream.get_next_entry(stream, start_entry_id) do
          nil ->
            :error

          resolved_start_entry ->
            # We resolve the end entry id and treat it as if it were a "+".
            {:ok, resolved_end_id} = Redis.Stream.resolve_entry_id(stream, "+")
            {stream_key, {resolved_start_entry.id, resolved_end_id}}
        end
      end)

    reply_message =
      key_to_start_and_end_pairs
      # Get a list of pairs: {stream_key, stream_entries}, one for each stream key.
      |> Enum.map(fn {stream_key, {start_entry_id, end_entry_id}} ->
        stream_entries =
          case KeyValueStore.get(stream_key, :no_expiry) do
            nil ->
              []

            %Redis.Value{type: :stream, data: stream} ->
              Redis.Stream.get_entries_range(stream, start_entry_id, end_entry_id)

            # TODO correctly raise errors
            _ ->
              :error
          end

        {stream_key, stream_entries}
      end)
      |> multiple_stream_entries_to_resp()

    :ok = send_message(state, reply_message)

    {:ok, state}
  end

  defp handle_request_xadd(state, ["XADD", stream_key, entry_id | rest] = request) do
    # Validate and parse the key-value arguments, which must come in pairs.
    stream_data = make_pairs_from_consecutive_elements(rest)

    entry = %Redis.Stream.Entry{id: entry_id, data: Map.new(stream_data)}

    # Add this entry to the stream (creating a new stream if needed).
    stream_or_error =
      case KeyValueStore.get(stream_key, :no_expiry) do
        nil ->
          %Redis.Stream{entries: [entry]}

        value ->
          Redis.Stream.add(value.data, entry)
      end

    case stream_or_error do
      :error_not_latest ->
        error_message =
          "ERR The ID specified in XADD is equal or smaller than the target stream top item"

        :ok = send_message(state, simple_error_request(error_message))

      :error_zero ->
        error_message = "ERR The ID specified in XADD must be greater than 0-0"
        :ok = send_message(state, simple_error_request(error_message))

      stream ->
        KeyValueStore.set(stream_key, stream)
        :ok = send_message(state, bulk_string_request(entry_id))

        # TODO this has the resolved entry_id (not the original request, we resolved the "*" bits). Is this what we send to the replicas?
        send_message_to_connected_replicas(state, request)
        # TODO add offset byte count, check exact request also
        ServerState.add_byte_offset_count(get_request_encoded_length(request))
    end

    {:ok, state}
  end

  @spec stream_entries_to_resp(list(%Redis.Stream.Entry{})) :: iodata()
  defp stream_entries_to_resp(entries) do
    # Given the entries for one stream, return them in the RESP format (this is used for the XRANGE command).
    entries
    |> Enum.map(fn entry ->
      entry_values = for {k, v} <- entry.data, do: [k, v]
      [entry.id, List.flatten(entry_values)]
    end)
    |> array_request()
  end

  @spec multiple_stream_entries_to_resp(list({binary(), list(%Redis.Stream.Entry{})})) :: iodata()
  defp multiple_stream_entries_to_resp(stream_key_entries_pairs) do
    # Given multiple stream keys and their entries, return them in the RESP format (this is used for the XADD command).
    stream_key_entries_pairs
    |> Enum.map(fn {key, entries} ->
      formatted_entries =
        entries
        |> Enum.map(fn entry ->
          entry_values = for {k, v} <- entry.data, do: [k, v]
          [entry.id, List.flatten(entry_values)]
        end)

      [key, formatted_entries]
    end)
    |> array_request()
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
        |> Enum.count(&(&1 == desired_master_offset))

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
      :ok ->
        :ok

      {:error, :timeout} ->
        send_message_on_socket(socket, send_fn, message)

      # TODO is this actually ok in all cases?
      {:error, :closed} ->
        :ok
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
  defp simple_error_request(input), do: RESP.encode(input, :simple_error)

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

  @spec make_pairs_from_consecutive_elements(list(binary())) :: list({binary(), binary()})
  defp make_pairs_from_consecutive_elements(pairs) do
    # Input: [key1, val1, key2, val2]
    # Output: [{key1, val1}, {key2, val2}]
    pairs
    |> Enum.chunk_every(2)
    |> Enum.map(fn [k, v] -> {k, v} end)
  end

  @spec make_pairs_from_separated_elements(list(binary())) :: %{binary() => binary()}
  defp make_pairs_from_separated_elements(input) do
    # Input: [key1, key2, val1, val2]
    # Output: %{key1 => val1, key2 => val2}
    {keys, values} =
      input
      |> Enum.split(div(length(input), 2))

    Enum.zip(keys, values)
  end
end
