defmodule Redis.Commands.Exec do
  alias Redis.{Connection, RESP}

  @doc ~S"""
  Executes the current transaction by applying the queued commands in order and replying with an array of responses.

  WARNING: the connection send_fn is swapped out with a dummy function while the commands are being executed, so that we "intercept" the
  replies we would've sent and instead send them all at once afterwards in one array. This means that complex commands with back-and-forth
  messages(like replication/handshake commands) will fail if they are run as part of this transaction. In other words, replication is
  not supported during transactions.
  """
  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["EXEC"]) do
    {:ok, connection} =
      case connection.queued_transaction_commands do
        # If MULTI hasn't been called yet, reply with an error.
        nil ->
          error_message = RESP.encode("ERR EXEC without MULTI", :simple_error)
          Connection.send_message(connection, error_message)

        # Nothing to apply, return an empty array response
        [] ->
          Connection.send_message(connection, RESP.encode([], :array))

        [_ | _] ->
          {new_connection, responses} = apply_transaction(connection)

          # Each of these responses is already RESP encoded. Just make a RESP array of these responses. A string is fine here since it's a valid iodata.
          smushed = Enum.join(responses, "")
          encoded = "*#{length(responses)}\r\n#{smushed}"
          # Send the result of applying the transaction as one array.
          Connection.send_message(new_connection, encoded)
      end

    # Now that the transaction is finished, set the commands field to nil to indicate that no transaction is in-process.
    connection = put_in(connection.queued_transaction_commands, nil)

    {:ok, connection}
  end

  @spec apply_transaction(Connection.t()) :: {Connection.t(), list(iodata())}
  defp apply_transaction(original_connection) do
    commands = original_connection.queued_transaction_commands
    original_connection = put_in(original_connection.queued_transaction_commands, nil)
    # Turn off the send_fn so it doesn't actually send anything.
    # TODO this will break replication messages/commands.
    original_send_fn = original_connection.send_fn
    connection = put_in(original_connection.send_fn, fn _conn, _msg -> :ok end)

    starting_accumulator = {connection, []}

    {final_conn, final_responses} =
      commands
      |> Enum.reduce(starting_accumulator, fn
        cmd, {current_connection, responses} ->
          {:ok, new_connection} = Connection.handle_request(current_connection, cmd)
          latest_response = new_connection.last_sent_message
          {new_connection, responses ++ [latest_response]}
      end)

    final_responses =
      final_responses
      |> Enum.reject(&(&1 == nil))

    # Reset the send_fn since we're done with the transaction.
    final_conn = put_in(final_conn.send_fn, original_send_fn)

    {final_conn, final_responses}
  end
end
