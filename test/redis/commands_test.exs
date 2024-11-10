defmodule Redis.CommandsTest do
  use ExUnit.Case, async: true
  # alias Redis.Commands.{XRead, XAdd, XRange}
  alias Redis.Commands.XAdd
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
        entries: [%Redis.Stream.Entry{id: entry_id, data: %{key => value}}]
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
        entries: [%Redis.Stream.Entry{id: entry_id, data: %{"key1" => "val1"}}]
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
          %Redis.Stream.Entry{id: new_entry_id, data: %{"key2" => "val2"}},
          %Redis.Stream.Entry{id: entry_id, data: %{"key1" => "val1"}}
        ]
      }

      check_stream(stream_key, expected_stream)
      check_response(connection, RESP.encode(new_entry_id, :bulk_string))
    end
  end

  # TODO add basic tests for xrange and xread
end
