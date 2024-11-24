defmodule Redis.Commands.Keys do
  alias Redis.{Connection, RESP, KeyValueStore}

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle(connection, ["KEYS", "*"]) do
    # For now, only handle the literal "*".
    Connection.send_message(connection, RESP.encode(KeyValueStore.get_all_keys(), :array))
  end
end
