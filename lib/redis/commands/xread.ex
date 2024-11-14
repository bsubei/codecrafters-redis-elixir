defmodule Redis.Commands.XRead do
  @moduledoc """
  Defines how the Redis server handles the XREAD command, see docs here: https://redis.io/docs/latest/commands/xread/ . The implementation of XREAD here only covers the basic usecases, i.e. not all the available options are implemented.

  The XREAD command is similar to XRANGE, and is used to get the data from multiple streams starting from a given entry id until the end of the stream.

  The implementation of XREAD has two modes: "blocking" and "nonblocking"
  - In nonblocking mode, the server immediately replies with the contents of the requested streams based on what was available at the time.
  - In blocking mode, the server waits until the specified timeout, and in the meantime replies with the stream data as it becomes available.
  """
  alias Redis.{Connection, KeyValueStore, Stream, RESP}

  @enforce_keys [:stream_keys, :start_entry_ids, :end_entry_ids, :connection]
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

    time_now_ms = System.os_time(:millisecond)

    case xread_request.block_timeout_ms do
      nil ->
        handle_xread_nonblocking(xread_request)

      # NOTE: the timeout can be 0 as well, which makes it block forever.
      block_timeout_ms when block_timeout_ms == 0 ->
        # TODO do the blocking one
        # wait a bit
        # try again (fetch new kv store state) until timeout
        # Block (wait) on this request until timeout, and relay any new entries we find in the meantime.
        # nil timestamp indicates no timeout (infinite blocking)
        timeout_epoch_ms = nil
        handle_xread_blocking(xread_request, timeout_epoch_ms, time_now_ms)

      block_timeout_ms ->
        timeout_epoch_ms = time_now_ms + block_timeout_ms
        handle_xread_blocking(xread_request, timeout_epoch_ms, System.os_time(:millisecond))
    end
  end

  defp handle_xread_blocking(xread_request, timeout_epoch_ms, time_now_ms)
       when timeout_epoch_ms != nil and time_now_ms > timeout_epoch_ms do
    # If timed out, return. Otherwise, proceed.
    IO.inspect("timeout! #{time_now_ms} > #{timeout_epoch_ms}")
    :ok = Connection.send_message(xread_request.connection, RESP.encode("", :bulk_string))
    {:ok, xread_request.connection}
  end

  defp handle_xread_blocking(xread_request, timeout_epoch_ms, _time_now_ms) do
    IO.inspect(
      "xread blocking with start #{inspect(xread_request.start_entry_ids)} and end #{inspect(xread_request.end_entry_ids)}"
    )

    # TODO this part is identical to the nonblocking one, refactor.
    # Fetch new stream state. Relay any new stream data to the connection.
    {:ok, _conn} = handle_xread_nonblocking(xread_request)

    # For each stream key:
    # Update the start entry id to be the end entry id for the next recursion.
    # Update the end entry id, to resolve to "+".
    {new_start_ids, new_end_ids} =
      Enum.zip([
        xread_request.stream_keys,
        xread_request.start_entry_ids,
        xread_request.end_entry_ids
      ])
      |> Enum.map(fn {stream_key, old_start_id, old_end_id} ->
        stream =
          case KeyValueStore.get(stream_key, :no_expiry) do
            nil -> %Stream{}
            value -> value.data
          end

        new_start_id =
          if old_end_id == nil, do: old_start_id, else: Stream.increment_entry_id(old_end_id)

        new_end_id =
          case Stream.resolve_entry_id(stream, "+") do
            {:ok, new_end_id} ->
              if Stream.entry_id_greater_than?(old_start_id, new_end_id),
                # or  new_end_id == old_start_id
                do: nil,
                else: new_end_id

            # TODO still have to take care of not resending same

            # if new_end_id == old_end_id do
            #   Stream.increment_entry_id(old_end_id)
            # else
            #   new_end_id
            # end

            {:error, _reason} ->
              # TODO handle error here correctly
              nil
          end

        {new_start_id, new_end_id}
      end)
      |> Enum.unzip()

    # |> IO.inspect(label: "new start and end ids")

    # TODO might need a sleep here.
    Process.sleep(50)

    # We recurse with the updated XREAD request.
    new_xread_request = %__MODULE__{
      xread_request
      | start_entry_ids: new_start_ids,
        end_entry_ids: new_end_ids
    }

    handle_xread_blocking(new_xread_request, timeout_epoch_ms, System.os_time(:millisecond))
  end

  @spec handle_xread_nonblocking(%__MODULE__{}) :: {:ok, %Connection{}}
  defp handle_xread_nonblocking(xread_request) do
    # Get a list of pairs: {stream_key, stream_entries}, one for each stream key.
    stream_key_entries_pairs =
      Enum.zip([
        xread_request.stream_keys,
        xread_request.start_entry_ids,
        xread_request.end_entry_ids
      ])

      # TODO what if everything's filtered out? Then what do we send?
      # |> IO.inspect(label: "handle_xread_nonblocking zipped")
      # NOTE: the start id can never be nil because we can't lose track of what we've sent so far. The end id can be nil to indicate there is no new data to send.
      |> Enum.reject(fn {_stream_key, _start_entry_id, end_entry_id} -> end_entry_id == nil end)
      # |> IO.inspect(label: "handle_xread_nonblocking zipped after filtering")
      |> Enum.map(fn {stream_key, start_entry_id, end_entry_id} ->
        stream_entries =
          case KeyValueStore.get(stream_key, :no_expiry) do
            nil ->
              []

            %Redis.Value{type: :stream, data: stream} ->
              Stream.get_entries_range(stream, start_entry_id, end_entry_id)

            # TODO correctly raise errors
            _ ->
              :error
          end

        {stream_key, stream_entries}
      end)
      |> Enum.reject(fn {_stream_key, stream_entries} -> Enum.empty?(stream_entries) end)

    case stream_key_entries_pairs do
      [] ->
        nil

      [_ | _] ->
        reply_message =
          stream_key_entries_pairs
          |> Stream.multiple_stream_entries_to_resp()
          |> IO.inspect(label: "SENDING RESP")

        :ok = Connection.send_message(xread_request.connection, reply_message)
    end

    {:ok, xread_request.connection}
  end

  @spec resolve_xread_args(%Connection{}, list(binary())) :: %__MODULE__{}
  def resolve_xread_args(connection, list_of_args) do
    {block_timeout_ms, rest} =
      case list_of_args do
        ["block", timeout_ms | rest] -> {String.to_integer(timeout_ms), rest}
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
          nil -> %Stream{}
          value -> value.data
        end
      end)

    {resolved_start_ids, resolved_end_ids} =
      Enum.zip(streams, start_ids)
      # |> IO.inspect(label: "zipped streams and start ids")
      |> Enum.map(fn {stream, start_id} ->
        # The start entry id for xread is exclusive, so we actually want to start one increment afterwards.
        resolved_start_id = Stream.increment_entry_id(start_id)

        # We resolve the end entry id and treat it as if it were a "+", i.e. everything until the end of the stream.
        resolved_end_id =
          case Stream.resolve_entry_id(stream, "+") do
            {:error, _} ->
              nil

            {:ok, resolved_end_id} ->
              if Stream.entry_id_greater_than?(resolved_start_id, resolved_end_id),
                do: nil,
                else: resolved_end_id
          end

        {resolved_start_id, resolved_end_id}
      end)
      # |> IO.inspect(label: "resolved start and end ids")
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
