defmodule Redis.Commands.Echo do
  alias Redis.{Connection, RESP}

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["ECHO" | _list_of_args] = original_request) do
    handle_request(connection, original_request)
  end

  # Echo back the args in our reply.
  @spec handle_request(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  defp handle_request(connection, ["ECHO", arg]) do
    :ok = Connection.send_message(connection, RESP.encode(arg, :bulk_string))
    {:ok, connection}
  end
end
