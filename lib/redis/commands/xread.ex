defmodule Redis.Commands.XRead do
  @moduledoc """
  Defines how the Redis server handles the XREAD command, see docs here: https://redis.io/docs/latest/commands/xread/ . The implementation of XREAD here only covers the basic usecases, i.e. not all the available options are implemented.

  The XREAD command is similar to XRANGE, and is used to get the data from multiple streams starting from a given entry id until the end of the stream.

  The implementation of XREAD has two modes: "blocking" and "nonblocking"
  - In nonblocking mode, the server immediately replies with the contents of the requested streams based on what was available at the time.
  - In blocking mode, the server waits until the specified timeout, and in the meantime replies with the stream data as it becomes available.
  """
  alias Redis.{Connection, KeyValueStore, Stream, RESP, Utils}

  @enforce_keys [:stream_keys, :start_entry_ids, :connection, :block_timeout_epoch_ms]
  @type t :: %__MODULE__{
          stream_keys: list(binary()),
          start_entry_ids: list(binary()),
          connection: %Connection{},
          block_timeout_epoch_ms: integer() | :none | :infinity
        }
  defstruct [
    :stream_keys,
    :start_entry_ids,
    :connection,
    block_timeout_epoch_ms: :none
  ]

  @spec handle_xread(Connection.t(), [binary(), ...]) :: {:ok, Connection.t()}
  def handle_xread(connection, ["XREAD" | list_of_args]) do
    handle_xread_request(resolve_xread_args(connection, list_of_args))
  end

  # Single xread request, no blocking/waiting. This is also called "asynchronous".
  @spec handle_xread_request(__MODULE__.t()) :: {:ok, Connection.t()}
  defp handle_xread_request(%__MODULE__{block_timeout_epoch_ms: :none} = xread_request) do
    _next_xread_request = xread_impl(xread_request)

    {:ok, xread_request.connection}
  end

  # Recursively call handle xread in "blocking" mode with an infinite timeout.
  defp handle_xread_request(%__MODULE__{block_timeout_epoch_ms: :infinity} = xread_request) do
    next_xread_request = xread_impl(xread_request)
    handle_xread_request(next_xread_request)
  end

  # Finitely recurse in blocking mode (checking for timeout every time).
  defp handle_xread_request(xread_request) do
    if System.os_time(:millisecond) > xread_request.block_timeout_epoch_ms do
      :ok = Connection.send_message(xread_request.connection, RESP.encode("", :bulk_string))
      {:ok, xread_request.connection}
    else
      next_xread_request = xread_impl(xread_request)
      handle_xread_request(next_xread_request)
    end
  end

  @spec xread_impl(__MODULE__.t()) :: __MODULE__.t()
  defp xread_impl(xread_request) do
    # Get the entries for each stream
    {stream_keys, stream_entries_per_key, resolved_end_ids} =
      Enum.zip(xread_request.stream_keys, xread_request.start_entry_ids)
      |> Enum.map(fn {stream_key, start_entry_id} ->
        stream = KeyValueStore.get_stream(stream_key)

        # Given the desired start id, find the earliest entry that exists in the stream that isn't before the start id.
        # If one exists, then we have data to send, and this is our resolved start id.
        # Otherwise, we bail because we have no data to send.
        case Stream.get_first_entry_greater_than_or_equal(stream, start_entry_id) do
          nil ->
            # bail
            {stream_key, [], nil}

          resolved_start_entry ->
            # Yay we have data to send
            # Now set the end id to be the latest entry in the stream (it could be the start id, if there's only one entry to send).
            {:ok, resolved_end_id} = Stream.resolve_entry_id(stream, "+")

            # Actually get the entries for the start-end range for this stream.
            stream_entries =
              Stream.get_entries_range(stream, resolved_start_entry.id, resolved_end_id)

            {stream_key, stream_entries, resolved_end_id}
        end
      end)
      |> Utils.unzip()

    new_start_ids =
      Enum.zip(xread_request.start_entry_ids, resolved_end_ids)
      |> Enum.map(fn
        # If no end id was provided, then there's no need to update the original start id since nothing will be sent for this stream.
        {original_start_id, _resolved_end_id = nil} -> original_start_id
        # Since we're sending the entries from original start to end, update the start id so we don't resend these entries in the next iteration.
        {_original_start_id, resolved_end_id} -> Stream.increment_entry_id(resolved_end_id)
      end)

    next_xread_request = %__MODULE__{xread_request | start_entry_ids: new_start_ids}

    # Filter out any streams with no entries to send.
    stream_key_entries_pairs =
      Enum.zip(stream_keys, stream_entries_per_key)
      |> Enum.reject(fn {_stream_key, stream_entries} -> Enum.empty?(stream_entries) end)

    # Send the entries.
    if not Enum.empty?(stream_key_entries_pairs) do
      reply_message =
        Stream.multiple_stream_entries_to_resp(stream_key_entries_pairs)

      :ok = Connection.send_message(xread_request.connection, reply_message)
    end

    next_xread_request
  end

  @spec resolve_xread_args(Connection.t(), [binary(), ...]) :: __MODULE__.t()
  defp resolve_xread_args(connection, list_of_args) do
    {block_timeout_epoch_ms, rest} =
      case list_of_args do
        ["block", timeout_ms | rest] ->
          case String.to_integer(timeout_ms) do
            0 -> {:infinity, rest}
            timeout_ms -> {System.os_time(:millisecond) + timeout_ms, rest}
          end

        rest ->
          {:none, rest}
      end

    # The next word should be "streams" (case-insensitive).
    # Extract the list of keys then start ids: [key1, key2, ..., start_id1, start_id2, ...]
    ["STREAMS" | list_of_keys_then_entry_ids] = [String.upcase(hd(rest)) | tl(rest)]

    # Separate the keys and start ids: {[key1, key2, ...], [start_id1, start_id2, ...]}
    {stream_keys, start_ids} =
      list_of_keys_then_entry_ids
      |> Enum.split(div(length(list_of_keys_then_entry_ids), 2))

    # The start ids we get are exclusive, "resolve" them by incrementing them once. Also, treat "$" as the latest entry id.
    resolved_start_ids =
      Enum.zip(stream_keys, start_ids)
      |> Enum.map(fn
        {stream_key, "$"} ->
          first_entry = KeyValueStore.get_stream(stream_key).entries |> List.first()
          Stream.increment_entry_id(first_entry.id)

        {_stream_key, start_id} ->
          Stream.increment_entry_id(start_id)
      end)

    %__MODULE__{
      stream_keys: stream_keys,
      start_entry_ids: resolved_start_ids,
      connection: connection,
      block_timeout_epoch_ms: block_timeout_epoch_ms
    }
  end
end
