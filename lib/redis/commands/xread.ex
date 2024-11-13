defmodule Redis.Commands.XRead do
  @moduledoc """
  Defines how the Redis server handles the XREAD command, see docs here: https://redis.io/docs/latest/commands/xread/ . The implementation of XREAD here only covers the basic usecases, i.e. not all the available options are implemented.

  The XREAD command is similar to XRANGE, and is used to get the data from multiple streams starting from a given entry id until the end of the stream.

  The implementation of XREAD has two modes: "blocking" and "nonblocking"
  - In nonblocking mode, the server immediately replies with the contents of the requested streams based on what was available at the time.
  - In blocking mode, the server waits until the specified timeout, and in the meantime replies with the stream data as it becomes available.
  """
  alias Redis.{Connection, KeyValueStore, Stream}

  @enforce_keys [:stream_keys, :end_entry_ids, :connection]
  @type t :: %__MODULE__{
          stream_keys: list(binary()),
          start_entry_ids: list(binary()),
          end_entry_ids: list(binary()),
          connection: %Connection{},
          block_timeout_ms: integer() | nil
        }
  defstruct [:stream_keys, :start_entry_ids, :end_entry_ids, :connection, block_timeout_ms: nil]

  def handle_xread(connection, ["XREAD" | list_of_args]) do
    xread_request = resolve_xread_args(connection, list_of_args)
    handle_xread_nonblocking(xread_request)

    # TODO
    # Do the blocking one
    # wait a bit
    # try again (fetch new kv store state) until timeout
    # Block (wait) on this request until timeout, and relay any new entries we find in the meantime.
  end

  @spec handle_xread_nonblocking(%__MODULE__{}) :: {:ok, %Connection{}}
  defp handle_xread_nonblocking(xread_request) do
    # Get a list of pairs: {stream_key, stream_entries}, one for each stream key.
    reply_message =
      Enum.zip([
        xread_request.stream_keys,
        xread_request.start_entry_ids,
        xread_request.end_entry_ids
      ])
      |> Enum.map(fn {stream_key, start_entry_id, end_entry_id} ->
        stream_entries =
          case KeyValueStore.get(stream_key, :no_expiry) do
            nil ->
              []

            %Redis.Value{type: :stream, data: stream} ->
              Redis.Stream.get_entries_range(stream, start_entry_id, end_entry_id)

            # TODO correctly raise errors
            _ ->
              :error
          end

        {stream_key, stream_entries}
      end)
      |> Stream.multiple_stream_entries_to_resp()

    :ok = Connection.send_message(xread_request.connection, reply_message)

    {:ok, xread_request.connection}
  end

  @spec resolve_xread_args(%Connection{}, list(binary())) :: %__MODULE__{}
  def resolve_xread_args(connection, list_of_args) do
    {block_timeout_ms, rest} =
      case list_of_args do
        ["block", timeout_ms | rest] -> {timeout_ms, rest}
        rest -> {nil, rest}
      end

    # TODO "streams" is case sensitive but it shouldn't be
    ["streams" | list_of_keys_then_entry_ids] = rest

    # A list of keys then start ids: [key1, key2, ..., start_id1, start_id2, ...]
    {stream_keys, start_ids} =
      list_of_keys_then_entry_ids
      # {[key1, key2, ...], [start_id1, start_id2, ...]}
      |> Enum.split(div(length(list_of_keys_then_entry_ids), 2))

    streams =
      stream_keys
      |> Enum.map(fn stream_key ->
        case KeyValueStore.get(stream_key, :no_expiry) do
          nil -> %Redis.Stream{}
          value -> value.data
        end
      end)

    {resolved_start_ids, resolved_end_ids} =
      Enum.zip(streams, start_ids)
      |> Enum.map(fn {stream, start_id} ->
        # We resolve the start entry id by finding the next entry id since XREAD treats the start entry id argument as exclusive.
        case Redis.Stream.get_next_entry(stream, start_id) do
          nil ->
            :error

          resolved_start_entry ->
            # We resolve the end entry id and treat it as if it were a "+", i.e. everything until the end of the stream.
            {:ok, resolved_end_id} = Redis.Stream.resolve_entry_id(stream, "+")
            {resolved_start_entry.id, resolved_end_id}
        end
      end)
      |> Enum.unzip()

    %__MODULE__{
      stream_keys: stream_keys,
      start_entry_ids: resolved_start_ids,
      end_entry_ids: resolved_end_ids,
      connection: connection,
      block_timeout_ms: block_timeout_ms
    }
  end
end
