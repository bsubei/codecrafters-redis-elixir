defmodule Redis.Commands.XAdd do
  @moduledoc """
  Defines how the Redis server handles the XADD command, see docs here: https://redis.io/docs/latest/commands/xadd/ . The implementation of XADD here only covers the basic usecases, i.e. not all the available options are implemented.

  XADD: Appends the specified stream entry to the stream at the specified key. If the key does not exist, as a side effect of running this command the key is created with a stream value.

  Supported usage:
  XADD key <* | id> field value [field value ...]


  TODO support clamping the entry ids to the current local timestamp to handle time jump shenanigans. From the docs:
  In order to guarantee this property, if the current top ID in the stream has a time greater than the current local time of the instance, the top entry time will be used instead, and the sequence part of the ID incremented. This may happen when, for instance, the local clock jumps backward, or if after a failover the new master has a different absolute time.
  """

  alias Redis.{Connection, RESP, KeyValueStore, Stream}

  @enforce_keys [:stream_key, :entry_id, :connection, :field_value_pairs]
  @type t :: %__MODULE__{
          stream_key: binary(),
          entry_id: binary(),
          connection: %Connection{},
          # Must have at least one pair.
          field_value_pairs: [{binary(), binary()}, ...]
        }
  defstruct [:stream_key, :entry_id, :connection, :field_value_pairs]

  @spec handle(Connection.t(), list(binary())) :: {:ok, Connection.t()}
  def handle(connection, ["XADD" | _rest] = request) do
    xadd_impl(resolve_args(connection, request))
  end

  @spec resolve_args(Connection.t(), [binary(), ...]) :: t()
  defp resolve_args(connection, ["XADD", stream_key, entry_id | rest]) do
    # First resolve this entry id (i.e. handle any "*").
    stream = KeyValueStore.get_stream(stream_key)

    {:ok, resolved_entry_id} = Stream.resolve_entry_id(stream, entry_id)

    %__MODULE__{
      stream_key: stream_key,
      entry_id: resolved_entry_id,
      connection: connection,
      # Validate and parse the key-value arguments, which must come in pairs.
      field_value_pairs: make_pairs_from_consecutive_elements(rest)
    }
  end

  @spec xadd_impl(%__MODULE__{}) :: {:ok, Connection.t()}
  defp xadd_impl(request) do
    entry = %Stream.Entry{
      id: request.entry_id,
      data: request.field_value_pairs
    }

    # Add this entry to the stream (creating a new stream if needed).
    stream_or_error =
      case KeyValueStore.get(request.stream_key, :no_expiry) do
        nil ->
          %Stream{entries: [entry]}

        value ->
          Stream.add(value.data, entry)
      end

    case stream_or_error do
      :error_not_latest ->
        error_message =
          "ERR The ID specified in XADD is equal or smaller than the target stream top item"

        :ok =
          Connection.send_message(request.connection, RESP.encode(error_message, :simple_error))

      :error_zero ->
        error_message = "ERR The ID specified in XADD must be greater than 0-0"

        :ok =
          Connection.send_message(request.connection, RESP.encode(error_message, :simple_error))

      stream ->
        :ok = KeyValueStore.set(request.stream_key, stream)

        :ok =
          Connection.send_message(
            request.connection,
            RESP.encode(request.entry_id, :bulk_string)
          )

        # TODO ignore replication for now since I don't have a way to test it.
    end

    {:ok, request.connection}
  end

  @spec make_pairs_from_consecutive_elements(list(binary())) :: [{binary(), binary()}, ...]
  defp make_pairs_from_consecutive_elements(pairs = [_key1, _value1 | _rest])
       when rem(length(pairs), 2) == 0 do
    # Input: [key1, val1, key2, val2]
    # Output: [{key1, val1}, {key2, val2}]
    pairs
    |> Enum.chunk_every(2)
    |> Enum.map(fn [k, v] -> {k, v} end)
  end
end
