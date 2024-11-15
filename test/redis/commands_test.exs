defmodule Redis.CommandsTest do
  use ExUnit.Case, async: true
  alias Redis.Commands.{XAdd, XRange}
  alias Redis.{Connection, KeyValueStore, RESP}

  # These test helpers are copied from connection_test, find a way to reuse.
  defp create_test_connection do
    send_fn = fn socket, message ->
      send(self(), {:tcp_send, socket, message})
      :ok
    end

    {:ok, connection} = Connection.init(%{socket: make_ref(), send_fn: send_fn, role: :master})
    connection
  end

  defp check_response(%Connection{socket: socket}, response) do
    assert_receive {:tcp_send, ^socket, ^response}, 100
  end

  defp check_stream(stream_key, expected_stream) do
    %Redis.Value{type: :stream, data: ^expected_stream} =
      KeyValueStore.get(stream_key, :no_expiry)
  end

  describe "running XADD" do
    setup do
      # NOTE: because there is only one KeyValueStore in our application, we should reset it to the initial state after every test clause.
      on_exit(fn -> KeyValueStore.clear() end)

      {:ok, connection: create_test_connection()}
    end

    test "without key-value pairs errors out", %{
      connection: connection
    } do
      assert_raise FunctionClauseError, fn ->
        XAdd.handle(connection, ["XADD", "stream1", "1-1"])
      end
    end

    test "on an empty stream creates a new stream with that data", %{connection: connection} do
      stream_key = "stream1"
      entry_id = "1-1"
      key = "key1"
      value = "val1"
      {:ok, connection} = XAdd.handle(connection, ["XADD", stream_key, entry_id, key, value])

      expected_stream = %Redis.Stream{
        entries: [%Redis.Stream.Entry{id: entry_id, data: [{key, value}]}]
      }

      check_stream(stream_key, expected_stream)
      check_response(connection, RESP.encode(entry_id, :bulk_string))
    end

    test "with an invalid entry id will error out and crash the server", %{
      connection: connection
    } do
      # NOTE: some kinds of entry ids are invalid but do not crash the server, instead the server replies with an error.

      entry_id = "not numbers"

      assert_raise ArgumentError, fn ->
        XAdd.handle(connection, ["XADD", "stream1", entry_id, "key1", "val1"])
      end

      entry_id = "1234-not numbers"

      assert_raise ArgumentError, fn ->
        XAdd.handle(connection, ["XADD", "stream1", entry_id, "key1", "val1"])
      end

      entry_id = "*-1234"

      assert_raise ArgumentError, fn ->
        XAdd.handle(connection, ["XADD", "stream1", entry_id, "key1", "val1"])
      end
    end

    test "with valid entry ids will not return error messages", %{connection: connection} do
      # These are all valid: "<timestamp>-<sequence_number>", "<timestamp>-*", "<timestamp>", and "*".
      # NOTE: I'm creating these entries in this order because old entry ids cannot be used with XADD.
      # Should resolve to "1234-0", the missing sequence number always implies 0.
      entry_id = "1234"

      {:ok, connection} =
        XAdd.handle(connection, ["XADD", "stream42", entry_id, "holy", "bananas"])

      check_response(connection, RESP.encode("1234-0", :bulk_string))

      entry_id = "1234-1"
      {:ok, connection} = XAdd.handle(connection, ["XADD", "stream42", entry_id, "holy", "moly"])
      check_response(connection, RESP.encode(entry_id, :bulk_string))

      # Should resolve to "1234-2"
      entry_id = "1234-*"

      {:ok, connection} =
        XAdd.handle(connection, ["XADD", "stream42", entry_id, "holy", "schnikes"])

      check_response(connection, RESP.encode("1234-2", :bulk_string))

      # Don't know what exactly it resolves to (it depends on system time), but we know it's likely to be later than the 1234 epoch timestamp, which is valid.
      entry_id = "*"
      {:ok, connection} = XAdd.handle(connection, ["XADD", "stream42", entry_id, "mamma", "mia"])

      socket = connection.socket

      receive do
        {:tcp_send, ^socket, response} ->
          {:ok, response, ""} = RESP.decode(IO.iodata_to_binary(response))
          [timestamp, "0"] = String.split(response, "-")
          assert String.to_integer(timestamp) > 1234

        _ ->
          assert false
      end
    end

    test "on non-empty stream adds new data", %{connection: connection} do
      stream_key = "stream1"
      entry_id = "1-1"
      # Make sure the stream is created already.
      {:ok, connection} = XAdd.handle(connection, ["XADD", stream_key, entry_id, "key1", "val1"])

      expected_stream = %Redis.Stream{
        entries: [%Redis.Stream.Entry{id: entry_id, data: [{"key1", "val1"}]}]
      }

      check_response(connection, RESP.encode(entry_id, :bulk_string))
      check_stream(stream_key, expected_stream)

      # Appending to the same entry id will reply with an error message.
      {:ok, connection} = XAdd.handle(connection, ["XADD", stream_key, entry_id, "key2", "val2"])

      # The stream doesn't change.
      check_stream(stream_key, expected_stream)

      check_response(
        connection,
        RESP.encode(
          "ERR The ID specified in XADD is equal or smaller than the target stream top item",
          :simple_error
        )
      )

      # Appending to a different entry id works
      new_entry_id = "2-0"

      {:ok, connection} =
        XAdd.handle(connection, ["XADD", stream_key, new_entry_id, "key2", "val2"])

      expected_stream = %Redis.Stream{
        entries: [
          %Redis.Stream.Entry{id: new_entry_id, data: [{"key2", "val2"}]},
          %Redis.Stream.Entry{id: entry_id, data: [{"key1", "val1"}]}
        ]
      }

      check_stream(stream_key, expected_stream)
      check_response(connection, RESP.encode(new_entry_id, :bulk_string))
    end
  end

  describe "running XRANGE" do
    setup do
      # NOTE: because there is only one KeyValueStore in our application, we should reset it to the initial state after every test clause.
      on_exit(fn -> KeyValueStore.clear() end)

      {:ok, connection: create_test_connection()}
    end

    test "with missing arguments errors out", %{
      connection: connection
    } do
      assert_raise FunctionClauseError, fn ->
        XRange.handle(connection, ["XRANGE"])
      end

      assert_raise FunctionClauseError, fn ->
        XRange.handle(connection, ["XRANGE", "stream1"])
      end

      assert_raise FunctionClauseError, fn ->
        XRange.handle(connection, ["XRANGE", "stream1", "start_id1"])
      end

      assert_raise FunctionClauseError, fn ->
        XRange.handle(connection, ["XRANGE", "stream1", "start_id1", "end1", "unaccounted_for"])
      end
    end

    test "on an empty stream returns a nil response", %{connection: connection} do
      stream_key = "nonexistent_stream"
      {:ok, connection} = XRange.handle(connection, ["XRANGE", stream_key, "0-1", "1-0"])

      # We should get a nil response and the stream shouldn't exist in the key value store.
      assert KeyValueStore.get(stream_key, :no_expiry) == nil
      check_response(connection, RESP.encode("", :bulk_string))
    end

    test "on non-empty stream returns data for the requested range in the same order they were originally stored",
         %{connection: connection} do
      stream_key = "stream1"
      # Make sure the stream is populated with data from 0-1 to 1-1. Let's say we have 5 entries.
      {:ok, connection} = XAdd.handle(connection, ["XADD", stream_key, "0-1", "key1", "val1"])
      check_response(connection, RESP.encode("0-1", :bulk_string))

      {:ok, connection} =
        XAdd.handle(connection, [
          "XADD",
          stream_key,
          "0-2",
          "key2",
          "val2",
          "anotherkey2",
          "anotherval2"
        ])

      check_response(connection, RESP.encode("0-2", :bulk_string))
      {:ok, connection} = XAdd.handle(connection, ["XADD", stream_key, "1-0", "key3", "val3"])
      check_response(connection, RESP.encode("1-0", :bulk_string))
      {:ok, connection} = XAdd.handle(connection, ["XADD", stream_key, "1-1", "key4", "val4"])
      check_response(connection, RESP.encode("1-1", :bulk_string))
      {:ok, connection} = XAdd.handle(connection, ["XADD", stream_key, "1-2", "key5", "val5"])
      check_response(connection, RESP.encode("1-2", :bulk_string))

      # If we request from 0-2 to 1-1, we should get the middle 3 entries only.
      {:ok, connection} = XRange.handle(connection, ["XRANGE", stream_key, "0-2", "1-1"])

      expected_response =
        RESP.encode(
          [
            # The response contains 3 entries.
            # Each entry contains a pair: the entry id (a string) and its contents (a list).
            # NOTE: the order of the contents is the same as they were added using XADD.
            ["0-2", ["key2", "val2", "anotherkey2", "anotherval2"]],
            ["1-0", ["key3", "val3"]],
            ["1-1", ["key4", "val4"]]
          ],
          :array
        )

      check_response(connection, expected_response)
    end
  end

  # TODO add basic tests for xread
end
