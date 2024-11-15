defmodule Redis.Commands.XRange do
  @moduledoc """
  Defines how the Redis server handles the XRANGE command, see docs here: https://redis.io/docs/latest/commands/xrange/ . The implementation of XRANGE here only covers the basic usecases, i.e. not all the available options are implemented.

  XRANGE will return all the entries from the specified start and end entry IDs (both inclusive) for the specified stream key. Both "+" and "-" are supported as entry id arguments.

  Supported usage:
  XRANGE key start end
  """
  alias Redis.{Connection, KeyValueStore, Stream, RESP}

  @spec handle(%Connection{}, list(binary())) :: {:ok, %Connection{}}
  def handle(connection, ["XRANGE", stream_key, start_entry_id, end_entry_id]) do
    # Resolve the entry ids, but don't allow "*".
    case [start_entry_id, end_entry_id] do
      ["*", _] ->
        :error

      [_, "*"] ->
        :error

      _ ->
        stream = KeyValueStore.get_stream(stream_key)
        {:ok, resolved_start_id} = Stream.resolve_entry_id(stream, start_entry_id)
        {:ok, resolved_end_id} = Stream.resolve_entry_id(stream, end_entry_id)

        # TODO refactor this so we don't get the stream twice.
        reply_message =
          case KeyValueStore.get(stream_key, :no_expiry) do
            nil ->
              RESP.encode("", :bulk_string)

            %Redis.Value{type: :stream, data: stream} ->
              Stream.get_entries_range(stream, resolved_start_id, resolved_end_id)
              |> Stream.stream_entries_to_resp()

            # TODO correctly raise errors
            _ ->
              :error
          end

        :ok = Connection.send_message(connection, reply_message)

        {:ok, connection}
    end
  end
end
