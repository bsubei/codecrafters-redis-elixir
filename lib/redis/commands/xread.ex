defmodule Redis.Commands.XRead do
  @moduledoc """
  Defines how the Redis server handles the XREAD command, see docs here: https://redis.io/docs/latest/commands/xread/ . The implementation of XREAD here only covers the basic usecases, i.e. not all the available options are implemented.

  The XREAD command is similar to XRANGE, and is used to get the data from multiple streams starting from a given entry id until the end of the stream.

  The implementation of XREAD has two modes: "blocking" and "nonblocking"
  - In nonblocking mode, the server immediately replies with the contents of the requested streams based on what was available at the time.
  - In blocking mode, the server waits until the specified timeout, and in the meantime replies with the stream data as it becomes available.
  """

  # def handle_xread(connection, list_of_args) do
  #   # TODO much better off putting this parsing in a module and defining these options in a struct
  #   case list_of_args do
  #     ["block", timeout_ms | rest] ->
  #       # Do the blocking one
  #       # wait a bit
  #       # try again (fetch new kv store state) until timeout
  #       # Block (wait) on this request until timeout, and relay any new entries we find in the meantime.
  #       handle_xread_blocking(state, rest)

  #     rest ->
  #       # Read whatever stream values we have and reply with whatever we got, don't wait for new data.
  #       handle_xread_nonblocking(state, rest)
  #   end
  # end

  # @spec resolve_xread_args(list(binary())) :: list(tuple(binary(), tuple(binary(), binary())))
  # def resolve_xread_args(list_of_args) do
  #   # A list of pairs: [{key1, start_id1}, {key2, start_id2}, ...]
  #   make_pairs_from_separated_elements(list_of_args)
  #   |> Enum.map(fn {stream_key, start_entry_id} ->
  #     stream =
  #       case KeyValueStore.get(stream_key, :no_expiry) do
  #         nil -> %Redis.Stream{}
  #         value -> value.data
  #       end

  #     # We resolve the start entry id by finding the next entry id since XREAD treats the start entry id argument as exclusive.
  #     case Redis.Stream.get_next_entry(stream, start_entry_id) do
  #       nil ->
  #         :error

  #       resolved_start_entry ->
  #         # We resolve the end entry id and treat it as if it were a "+", i.e. everything until the end of the stream.
  #         {:ok, resolved_end_id} = Redis.Stream.resolve_entry_id(stream, "+")
  #         {stream_key, {resolved_start_entry.id, resolved_end_id}}
  #     end
  #   end)
  # end
end
