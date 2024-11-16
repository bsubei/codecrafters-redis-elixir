defmodule Redis.Commands.Multi do
  alias Redis.Connection

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["MULTI"]) do
    # Set the queued transactions in this connection to be an empty list to denote that a transaction has started but has no commands queued yet.
    connection = update_in(connection.queued_transaction_commands, fn _ -> [] end)

    # Reply with ok.
    :ok = Connection.reply_ok(connection)

    {:ok, connection}
  end
end
