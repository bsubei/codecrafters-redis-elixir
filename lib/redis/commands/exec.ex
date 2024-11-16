defmodule Redis.Commands.Exec do
  alias Redis.{Connection, RESP}

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["EXEC"]) do
    case connection.queued_transaction_commands do
      # If MULTI hasn't been called yet, reply with an error.
      nil ->
        error_message = RESP.encode("ERR EXEC without MULTI", :simple_error)

        :ok =
          Connection.send_message(connection, error_message)

      # Nothing to apply, return an empty array response
      [] ->
        :ok = Connection.send_message(connection, RESP.encode([], :array))

      # TODO actually apply the transactions
      [_ | _] = _transactions ->
        nil
    end

    # Now that the transaction is finished, set the field to nil.
    connection = update_in(connection.queued_transaction_commands, fn _ -> nil end)

    {:ok, connection}
  end
end
