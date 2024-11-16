defmodule Redis.Commands.Ping do
  alias Redis.{Connection, RESP}

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["PING" | _list_of_args] = original_request) do
    handle_request(connection, original_request)
  end

  # Always reply to any PING with a PONG. But also handle the case when this could be the start of a handshake from a replica to us if we're master.
  @spec handle_request(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  defp handle_request(connection, ["PING"]) do
    :ok = Connection.send_message(connection, RESP.encode("PONG", :simple_string))

    new_handshake_status =
      case connection.role do
        :master -> :ping_received
        _ -> connection.handshake_status
      end

    {:ok, %Connection{connection | handshake_status: new_handshake_status}}
  end

  # Echo back the arg if given a PING with an arg. Do not treat this as the start of a handshake.
  defp handle_request(connection, ["PING", arg]) do
    :ok = Connection.send_message(connection, RESP.encode(arg, :bulk_string))
    {:ok, connection}
  end
end
