defmodule Redis.ConnectionTest do
  # NOTE: we can't run these tests in async because some of them set the key value store and check its contents.
  use ExUnit.Case, async: false
  alias Redis.ClientConnection
  alias Redis.RESP
  alias Redis.KeyValueStore
  doctest Redis.ClientConnection

  defp create_test_connection do
    send_fn = fn socket, message ->
      send(self(), {:tcp_send, socket, message})
    end

    {:ok, connection} = ClientConnection.init(%{socket: make_ref(), send_fn: send_fn})
    connection
  end

  describe "connection handle_info receiving data from a socket" do
    setup do
      # NOTE: because there is only one KeyValueStore in our application, we should reset it to the initial state after every test clause.
      on_exit(fn -> KeyValueStore.clear() end)

      {:ok, connection: create_test_connection()}
    end

    test "receiving a PING request should result in a PONG reply", %{connection: connection} do
      ping = IO.iodata_to_binary(RESP.encode(["PING"], :array))
      ClientConnection.handle_info({:tcp, connection.socket, ping}, connection)
      pong = RESP.encode("PONG", :simple_string)
      assert_receive {:tcp_send, _, ^pong}, 100
    end

    test "receiving a GET request on a non-existing key should return the null bulk string", %{
      connection: connection
    } do
      ping = IO.iodata_to_binary(RESP.encode(["GET", "I wonder what's in here?"], :array))
      ClientConnection.handle_info({:tcp, connection.socket, ping}, connection)
      null_string = RESP.encode("", :bulk_string)
      assert_receive {:tcp_send, _, ^null_string}, 100
    end

    test "receiving a SET and then a GET on the same key should return that same value in a reply",
         %{
           connection: connection
         } do
      # Set a key-value.
      set_request = IO.iodata_to_binary(RESP.encode(["SET", "bananas", "yum"], :array))
      ClientConnection.handle_info({:tcp, connection.socket, set_request}, connection)
      ok_reply = RESP.encode("OK", :simple_string)
      assert_receive {:tcp_send, _, ^ok_reply}, 100
      # Get the key-value.
      get_request = IO.iodata_to_binary(RESP.encode(["GET", "bananas"], :array))
      ClientConnection.handle_info({:tcp, connection.socket, get_request}, connection)
      expected_reply = RESP.encode("yum", :bulk_string)
      assert_receive {:tcp_send, _, ^expected_reply}, 100
    end

    test "multiple connections can SET and GET and affect each other",
         %{
           connection: first_connection
         } do
      second_connection = create_test_connection()
      # Neither connections should see "bananas".
      get_request = IO.iodata_to_binary(RESP.encode(["GET", "bananas"], :array))
      null_string = RESP.encode("", :bulk_string)
      ClientConnection.handle_info({:tcp, first_connection.socket, get_request}, first_connection)
      assert_receive {:tcp_send, _, ^null_string}, 100

      ClientConnection.handle_info(
        {:tcp, second_connection.socket, get_request},
        second_connection
      )

      assert_receive {:tcp_send, _, ^null_string}, 100

      # Now, if one of the connections sets "bananas", both should be able to see it.
      set_request = IO.iodata_to_binary(RESP.encode(["SET", "bananas", "yum"], :array))
      ok_reply = RESP.encode("OK", :simple_string)
      ClientConnection.handle_info({:tcp, first_connection.socket, set_request}, first_connection)
      assert_receive {:tcp_send, _, ^ok_reply}, 100

      expected_reply = RESP.encode("yum", :bulk_string)
      ClientConnection.handle_info({:tcp, first_connection.socket, get_request}, first_connection)
      assert_receive {:tcp_send, _, ^expected_reply}, 100

      ClientConnection.handle_info(
        {:tcp, second_connection.socket, get_request},
        second_connection
      )

      assert_receive {:tcp_send, _, ^expected_reply}, 100
    end

    test "sending SET with an already expired expiry should return null when we try to GET",
         %{
           connection: connection
         } do
      # Set a key-value with 0 ms expiry.
      set_request = IO.iodata_to_binary(RESP.encode(["SET", "bananas", "yum", "px", "0"], :array))
      ClientConnection.handle_info({:tcp, connection.socket, set_request}, connection)
      ok_reply = RESP.encode("OK", :simple_string)
      assert_receive {:tcp_send, _, ^ok_reply}, 100
      # Wait a tiny bit to ensure that the entry has expired.
      Process.sleep(100)
      # Get the key-value, it should be expired.
      get_request = IO.iodata_to_binary(RESP.encode(["GET", "bananas"], :array))
      ClientConnection.handle_info({:tcp, connection.socket, get_request}, connection)
      null_string = RESP.encode("", :bulk_string)
      assert_receive {:tcp_send, _, ^null_string}, 100
    end

    test "sending SET with a far expiry should return the entry when we try to GET",
         %{
           connection: connection
         } do
      # Set a key-value with 10000 ms expiry.
      set_request =
        IO.iodata_to_binary(RESP.encode(["SET", "bananas", "yum", "px", "10000"], :array))

      ClientConnection.handle_info({:tcp, connection.socket, set_request}, connection)
      ok_reply = RESP.encode("OK", :simple_string)
      assert_receive {:tcp_send, _, ^ok_reply}, 100
      # Get the key-value, it should not be expired
      get_request = IO.iodata_to_binary(RESP.encode(["GET", "bananas"], :array))
      ClientConnection.handle_info({:tcp, connection.socket, get_request}, connection)
      expected_reply = RESP.encode("yum", :bulk_string)
      assert_receive {:tcp_send, _, ^expected_reply}, 100
    end
  end
end
