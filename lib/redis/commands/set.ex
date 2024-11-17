defmodule Redis.Commands.Set do
  alias Redis.{Connection, KeyValueStore, ServerState, RESP}

  @enforce_keys [:key, :value, :expiry_timestamp_epoch_ms, :repl_offset_count, :connection]
  @type t :: %__MODULE__{
          key: binary(),
          value: binary(),
          expiry_timestamp_epoch_ms: integer() | nil,
          repl_offset_count: integer(),
          connection: Connection.t()
        }
  defstruct [:key, :value, :expiry_timestamp_epoch_ms, :repl_offset_count, :connection]

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["SET" | _list_of_args] = original_request) do
    handle_request(resolve_args(connection, original_request))
  end

  @spec resolve_args(Connection.t(), [binary(), ...]) :: t()
  defp resolve_args(connection, ["SET" | list_of_args] = request) do
    [key, value | rest] = list_of_args

    # Only handling the "px" case for now (relative expiry in milliseconds).
    expiry_timestamp_epoch_ms =
      case {connection.handshake_status, rest} do
        # We don't set the expiry when the message is a replication update because replicas don't expire their own entries, they expect master to expire them.
        {:connected_to_master, _} ->
          nil

        {_, []} ->
          nil

        {_, ["px", relative_timestamp_milliseconds]} ->
          System.os_time(:millisecond) + String.to_integer(relative_timestamp_milliseconds)
      end

    repl_offset_count = Connection.get_request_encoded_length(request)

    %__MODULE__{
      key: key,
      value: value,
      expiry_timestamp_epoch_ms: expiry_timestamp_epoch_ms,
      repl_offset_count: repl_offset_count,
      connection: connection
    }
  end

  # If we receive replication updates from master, apply them but do not reply.
  defp handle_request(
         %__MODULE__{connection: %Connection{handshake_status: :connected_to_master}} = request
       ) do
    KeyValueStore.set(request.key, request.value, request.expiry_timestamp_epoch_ms)
    ServerState.add_byte_offset_count(request.repl_offset_count)
    {:ok, request.connection}
  end

  # Otherwise, this is a regular client call. Set the key and value in our key-value store and reply with OK. Relay this to replicas if we are master.
  defp handle_request(request) do
    KeyValueStore.set(request.key, request.value, request.expiry_timestamp_epoch_ms)

    ok = RESP.encode("OK", :simple_string)
    {:ok, new_connection} = Connection.send_message(request.connection, ok)
    request = put_in(request.connection, new_connection)

    # Relay any updates to all connected replicas except this current Connection. Note that we don't include the expiry terms in here.
    send_message_to_connected_replicas(request.connection, ["SET", request.key, request.value])

    ServerState.add_byte_offset_count(request.repl_offset_count)

    {:ok, request.connection}
  end

  defp send_message_to_connected_replicas(connection, message) do
    socket = connection.socket

    Enum.map(Map.keys(ServerState.get_state().connected_replicas), fn
      ^socket ->
        nil

      replica_socket ->
        Connection.send_message_on_socket(
          replica_socket,
          connection.send_fn,
          RESP.encode(message, :array)
        )
    end)
  end
end
