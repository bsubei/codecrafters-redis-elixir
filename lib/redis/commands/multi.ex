defmodule Redis.Commands.Multi do
  alias Redis.{Connection, RESP}

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["MULTI"]) do
    # TODO handle multiple MULTI
    # Set the queued transactions in this connection to be an empty list to denote that a transaction has started but has no commands queued yet.
    connection = put_in(connection.queued_transaction_commands, [])
    # Reply with ok.
    Connection.send_message(connection, RESP.encode("OK", :simple_string))
  end
end
