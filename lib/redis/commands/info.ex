defmodule Redis.Commands.Info do
  alias Redis.{Connection, RESP, ServerInfo, ServerState}

  # Reply with the contents of all the ServerInfo sections we have.
  @spec handle(Redis.Connection.t(), [binary(), ...]) :: {:ok, Redis.Connection.t()}

  def handle(connection, ["INFO"]) do
    server_info_string =
      ServerInfo.get_human_readable_string(ServerState.get_state().server_info)

    Connection.send_message(connection, RESP.encode(server_info_string, :bulk_string))
  end

  # Reply with the contents of the specified ServerInfo section.
  def handle(connection, ["INFO" | rest]) do
    server_info_string =
      ServerInfo.get_human_readable_string(ServerState.get_state().server_info, rest)

    Connection.send_message(connection, RESP.encode(server_info_string, :bulk_string))
  end
end
