defmodule Redis.ConnectionTest do
  # NOTE: we can't run these tests in async because some of them set the key value store and check its contents.
  use ExUnit.Case, async: false
  alias Redis.Connection
  alias Redis.RESP
  alias Redis.KeyValueStore
  doctest Redis.Connection

  defp create_test_connection do
    send_fn = fn socket, message ->
      send(self(), {:tcp_send, socket, message})
      :ok
    end

    {:ok, connection} = Connection.init(%{socket: make_ref(), send_fn: send_fn, role: :master})
    connection
  end

  defp check_request_response(%Connection{socket: socket} = connection, request, response) do
    Connection.handle_info({:tcp, socket, request}, connection)
    assert_receive {:tcp_send, ^socket, ^response}, 100
  end

  describe "connection handle_info receiving data from a socket" do
    setup do
      # NOTE: because there is only one KeyValueStore in our application, we should reset it to the initial state after every test clause.
      on_exit(fn -> KeyValueStore.clear() end)

      {:ok, connection: create_test_connection()}
    end

    test "receiving a PING request should result in a PONG reply on the same socket", %{
      connection: connection
    } do
      ping = IO.iodata_to_binary(RESP.encode(["PING"], :array))
      pong = RESP.encode("PONG", :simple_string)
      check_request_response(connection, ping, pong)
    end

    test "receiving a GET request on a non-existing key should return the null bulk string", %{
      connection: connection
    } do
      ping = IO.iodata_to_binary(RESP.encode(["GET", "I wonder what's in here?"], :array))
      null_string = RESP.encode("", :bulk_string)
      check_request_response(connection, ping, null_string)
    end

    test "receiving a SET and then a GET on the same key should return that same value in a reply",
         %{
           connection: connection
         } do
      # Set a key-value.
      set_request = IO.iodata_to_binary(RESP.encode(["SET", "bananas", "yum"], :array))
      ok_reply = RESP.encode("OK", :simple_string)
      check_request_response(connection, set_request, ok_reply)
      # Get the key-value.
      get_request = IO.iodata_to_binary(RESP.encode(["GET", "bananas"], :array))
      expected_reply = RESP.encode("yum", :bulk_string)
      check_request_response(connection, get_request, expected_reply)
    end

    test "multiple connections can SET and GET and affect each other",
         %{
           connection: first_connection
         } do
      second_connection = create_test_connection()
      # Neither connections should see "bananas".
      get_request = IO.iodata_to_binary(RESP.encode(["GET", "bananas"], :array))
      null_string = RESP.encode("", :bulk_string)
      check_request_response(first_connection, get_request, null_string)
      check_request_response(second_connection, get_request, null_string)
      # Now, if one of the connections sets "bananas", both should be able to see it.
      set_request = IO.iodata_to_binary(RESP.encode(["SET", "bananas", "yum"], :array))
      ok_reply = RESP.encode("OK", :simple_string)
      check_request_response(first_connection, set_request, ok_reply)

      expected_reply = RESP.encode("yum", :bulk_string)
      check_request_response(first_connection, get_request, expected_reply)
      check_request_response(second_connection, get_request, expected_reply)
    end

    test "sending SET with an already expired expiry should return null when we try to GET",
         %{
           connection: connection
         } do
      # Set a key-value with 0 ms expiry.
      set_request = IO.iodata_to_binary(RESP.encode(["SET", "bananas", "yum", "px", "0"], :array))
      Connection.handle_info({:tcp, connection.socket, set_request}, connection)
      ok_reply = RESP.encode("OK", :simple_string)
      check_request_response(connection, set_request, ok_reply)
      # Wait a tiny bit to ensure that the entry has expired.
      Process.sleep(100)
      # Get the key-value, it should be expired.
      get_request = IO.iodata_to_binary(RESP.encode(["GET", "bananas"], :array))
      null_string = RESP.encode("", :bulk_string)
      check_request_response(connection, get_request, null_string)
    end

    test "sending SET with a far expiry should return the entry when we try to GET",
         %{
           connection: connection
         } do
      # Set a key-value with 10000 ms expiry.
      set_request =
        IO.iodata_to_binary(RESP.encode(["SET", "bananas", "yum", "px", "10000"], :array))

      ok_reply = RESP.encode("OK", :simple_string)
      check_request_response(connection, set_request, ok_reply)
      # Get the key-value, it should not be expired
      get_request = IO.iodata_to_binary(RESP.encode(["GET", "bananas"], :array))
      expected_reply = RESP.encode("yum", :bulk_string)
      check_request_response(connection, get_request, expected_reply)
    end

    test "sending XADD will actually create a new stream and add an entry",
         %{
           connection: connection
         } do
      xadd_request =
        IO.iodata_to_binary(
          RESP.encode(
            ["XADD", "the_stream_key", "1234-0", "foo", "bar", "baz", "quirk"],
            :array
          )
        )

      entry_id_reply = RESP.encode("1234-0", :bulk_string)
      check_request_response(connection, xadd_request, entry_id_reply)
      # This stream should now exist in our KeyValueStore.
      %Redis.Value{data: stream, type: :stream, expiry_timestamp_epoch_ms: nil} =
        KeyValueStore.get("the_stream_key", :no_expiry)

      assert length(stream.entries) == 1

      assert %Redis.Stream.Entry{id: "1234-0", data: %{"foo" => "bar", "baz" => "quirk"}} =
               Redis.Stream.get_entry(stream, "1234-0")

      # Let's send another entry to the same stream!
      xadd_request =
        IO.iodata_to_binary(
          RESP.encode(
            ["XADD", "the_stream_key", "1235-0", "foo", "42"],
            :array
          )
        )

      entry_id_reply = RESP.encode("1235-0", :bulk_string)
      check_request_response(connection, xadd_request, entry_id_reply)
      # This stream should now have two entries.
      %Redis.Value{data: stream, type: :stream, expiry_timestamp_epoch_ms: nil} =
        KeyValueStore.get("the_stream_key", :no_expiry)

      assert length(stream.entries) == 2

      assert %Redis.Stream.Entry{id: "1235-0", data: %{"foo" => "42"}} =
               Redis.Stream.get_entry(stream, "1235-0")
    end

    test "sending XADD with 0-* and 42-* entry ids will resolve the sequence number correctly",
         %{
           connection: connection
         } do
      foo_bar_list = ["foo", "bar", "baz", "quirk"]
      foo_bar_map = %{"foo" => "bar", "baz" => "quirk"}

      xadd_request =
        IO.iodata_to_binary(
          RESP.encode(
            ["XADD", "the_stream_key", "0-*" | foo_bar_list],
            :array
          )
        )

      # If we XADD 0-* when the stream is empty, we should get an entry id of 0-1.
      entry_id_reply = RESP.encode("0-1", :bulk_string)
      check_request_response(connection, xadd_request, entry_id_reply)
      # This stream should now exist in our KeyValueStore.
      %Redis.Value{data: stream, type: :stream, expiry_timestamp_epoch_ms: nil} =
        KeyValueStore.get("the_stream_key", :no_expiry)

      assert length(stream.entries) == 1

      assert %Redis.Stream.Entry{id: "0-1", data: ^foo_bar_map} =
               Redis.Stream.get_entry(stream, "0-1")

      # Now let's try the same entry id "0-*", it should reply with the entry id "0-2".
      entry_id_reply = RESP.encode("0-2", :bulk_string)
      check_request_response(connection, xadd_request, entry_id_reply)
      # This stream should now have two entries.
      %Redis.Value{data: stream, type: :stream, expiry_timestamp_epoch_ms: nil} =
        KeyValueStore.get("the_stream_key", :no_expiry)

      assert length(stream.entries) == 2

      assert %Redis.Stream.Entry{id: "0-2", data: ^foo_bar_map} =
               Redis.Stream.get_entry(stream, "0-2")

      # Finally, if we XADD 42-*, it will resolve to the entry id 42-0.
      xadd_request =
        IO.iodata_to_binary(
          RESP.encode(
            ["XADD", "the_stream_key", "42-*" | foo_bar_list],
            :array
          )
        )

      entry_id_reply = RESP.encode("42-0", :bulk_string)
      check_request_response(connection, xadd_request, entry_id_reply)

      %Redis.Value{data: stream, type: :stream, expiry_timestamp_epoch_ms: nil} =
        KeyValueStore.get("the_stream_key", :no_expiry)

      assert length(stream.entries) == 3

      assert %Redis.Stream.Entry{id: "42-0", data: ^foo_bar_map} =
               Redis.Stream.get_entry(stream, "42-0")
    end
  end
end
