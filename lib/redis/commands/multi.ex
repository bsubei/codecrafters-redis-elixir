defmodule Redis.Commands.Multi do
  alias Redis.Connection

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["MULTI"]) do
    # TODO actually do stuff

    # Reply with ok.
    Connection.reply_ok(connection)

    {:ok, connection}
  end
end
