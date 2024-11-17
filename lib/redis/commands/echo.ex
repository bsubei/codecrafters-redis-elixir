defmodule Redis.Commands.Echo do
  alias Redis.{Connection, RESP}

  @spec handle(Connection.t(), list(binary())) :: {:ok, Connection.t()}
  def handle(connection, ["ECHO", arg]) do
    # Echo back the args in our reply.
    reply_message = RESP.encode(arg, :bulk_string)
    {:ok, connection} = Connection.send_message(connection, reply_message)
    connection = put_in(connection.latest_command_response, reply_message)
    {:ok, connection}
  end
end
