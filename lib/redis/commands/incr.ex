defmodule Redis.Commands.Incr do
  alias Redis.{Connection, RESP, KeyValueStore, Value}

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["INCR" | _list_of_args] = original_request) do
    handle_request(connection, original_request)
  end

  @spec handle_request(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  defp handle_request(connection, ["INCR", key]) do
    # Fetch the value at the key.
    value =
      case KeyValueStore.get(key) do
        # If it doesn't exist, default to 0.
        nil -> "0"
        %Value{data: value, type: :string} when is_binary(value) -> value
      end

    reply_message =
      case Integer.parse(value) do
        # Set the new value only if it's an integer with no other leftovers in the string.
        {number, ""} ->
          KeyValueStore.set(key, "#{number + 1}")
          RESP.encode("#{number + 1}", :integer)

        # If it can't be exactly represented as a base-10 signed integer, return an error message.
        :error ->
          RESP.encode("ERR value is not an integer or out of range", :simple_error)
      end

    # TODO account for replication (offset count + relay)

    Connection.send_message(connection, reply_message)
  end
end
