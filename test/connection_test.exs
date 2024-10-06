defmodule Redis.ConnectionTest do
  use ExUnit.Case, async: true
  alias Redis.Connection
  alias Redis.RESP
  doctest Redis.Connection

  describe "connection handle_info receiving data from a socket" do
    setup do
      test_pid = self()

      send_fn = fn socket, message ->
        send(test_pid, {:tcp_send, socket, message})
      end

      {:ok, connection} = Connection.init(%{socket: make_ref(), send_fn: send_fn})
      {:ok, connection: connection}
    end

    test "receiving a PING request should result in a PONG reply", %{connection: connection} do
      ping = IO.iodata_to_binary(RESP.encode(["PING"], :array))
      Connection.handle_info({:tcp, connection.socket, ping}, connection)
      pong = RESP.encode("PONG", :simple_string)
      assert_receive {:tcp_send, _, ^pong}, 100
    end

    test "receiving a GET request on a non-existing key should return the null bulk string", %{
      connection: connection
    } do
      ping = IO.iodata_to_binary(RESP.encode(["GET", "bananas"], :array))
      Connection.handle_info({:tcp, connection.socket, ping}, connection)
      null_string = RESP.encode("", :bulk_string)
      assert_receive {:tcp_send, _, ^null_string}, 100
    end
  end
end
