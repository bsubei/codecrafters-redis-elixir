defmodule Redis.Commands.Discard do
  alias Redis.{Connection, RESP}

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["DISCARD"]) do
    {reply_message, connection} =
      case connection.queued_transaction_commands do
        nil ->
          {RESP.encode("ERR DISCARD without MULTI", :simple_error), connection}

        # Set the queued transactions in this connection to be nil to denote that there is no transaction in-progress.
        _ ->
          {RESP.encode("OK", :simple_string), put_in(connection.queued_transaction_commands, nil)}
      end

    Connection.send_message(connection, reply_message)
  end
end
