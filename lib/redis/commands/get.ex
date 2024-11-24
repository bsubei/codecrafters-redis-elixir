defmodule Redis.Commands.Get do
  alias Redis.{Connection, RESP, KeyValueStore}

  # Get the requested key's value from our key-value store and make that our reply.
  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["GET", key]) do
    # Note that replicas don't bother checking for expiry because the master will tell them to expire entries instead.
    value =
      case connection.role do
        :master -> KeyValueStore.get(key)
        :slave -> KeyValueStore.get(key, :no_expiry)
      end

    data = if value, do: value.data, else: ""
    reply_message = RESP.encode(data, :bulk_string)
    Connection.send_message(connection, reply_message)
  end
end
