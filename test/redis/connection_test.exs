defmodule Redis.ConnectionTest do
  # NOTE: we can't run these tests in async because some of them set the key value store and check its contents.
  use ExUnit.Case, async: false
  alias Redis.Connection
  alias Redis.RESP
  alias Redis.KeyValueStore
  doctest Redis.Connection

  describe "connection handle_info receiving data from a socket" do
    setup do
      test_pid = self()

      send_fn = fn socket, message ->
        send(test_pid, {:tcp_send, socket, message})
      end

      # NOTE: because there is only one KeyValueStore in our application, we should reset it to the initial state after every test clause.
      on_exit(fn -> KeyValueStore.clear() end)

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
      ping = IO.iodata_to_binary(RESP.encode(["GET", "I wonder what's in here?"], :array))
      Connection.handle_info({:tcp, connection.socket, ping}, connection)
      null_string = RESP.encode("", :bulk_string)
      assert_receive {:tcp_send, _, ^null_string}, 100
    end

    test "receiving a SET and then a GET on the same key should return that same value in a reply",
         %{
           connection: connection
         } do
      # Set a key-value.
      set_request = IO.iodata_to_binary(RESP.encode(["SET", "bananas", "yum"], :array))
      Connection.handle_info({:tcp, connection.socket, set_request}, connection)
      ok_reply = RESP.encode("OK", :simple_string)
      assert_receive {:tcp_send, _, ^ok_reply}, 100
      # Get the key-value.
      get_request = IO.iodata_to_binary(RESP.encode(["GET", "bananas"], :array))
      Connection.handle_info({:tcp, connection.socket, get_request}, connection)
      expected_reply = RESP.encode("yum", :bulk_string)
      assert_receive {:tcp_send, _, ^expected_reply}, 100
    end
  end
end
