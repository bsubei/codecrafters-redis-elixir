defmodule Redis.Commands.Exec do
  alias Redis.{Connection, RESP}

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["EXEC"]) do
    # TODO actually do stuff

    # If MULTI hasn't been called yet, reply with an error.
    :ok =
      Connection.send_message(connection, RESP.encode("ERR EXEC without MULTI", :simple_error))

    {:ok, connection}
  end
end
