defmodule Redis.Commands.Config do
  alias Redis.{Connection, RESP, ServerInfo, ServerState}

  @spec handle(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}

  def handle(connection, ["CONFIG", "GET", arg]) do
    # Handle a single arg only for now.
    handle_get(connection, arg)
  end

  def handle(connection, ["CONFIG", "SET" | args]) do
    handle_set(connection, args)
  end

  defp handle_get(connection, arg) do
    {key, value} =
      ServerState.get_state().server_info
      |> ServerInfo.get_all_keywords()
      |> Enum.find(fn {k, _v} -> k == arg end)

    Connection.send_message(connection, RESP.encode([key, value], :array))
  end

  defp handle_set(_connection, _args) do
    :unimplemented
  end
end
