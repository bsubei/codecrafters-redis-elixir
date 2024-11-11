defmodule Redis.Stream.Entry do
  @enforce_keys [:id]
  @type t :: %__MODULE__{id: binary(), data: list({binary(), binary()})}
  # NOTE: the entry data ordering should be preserved. From the docs:
  # The field-value pairs are stored in the same order they are given by the user
  defstruct [:id, data: []]
end

defmodule Redis.Stream do
  @moduledoc """
  This module defines a "stream" that is stored in a KeyValueStore. A stream contains multiple entries, each entry has an id and any number of key-value fields.
  """
  alias Redis.Stream.Entry
  alias Redis.RESP

  # TODO for now just use a list in reversed order (because prepending is O(1) in Elixir), but eventually use a data structure that is more optimized for O(1) random-access lookup AND O(1)-ish range lookups
  @type t :: %__MODULE__{entries: list(%Entry{})}
  defstruct entries: []

  @type error_reason :: :empty_entries

  @spec get_entry(%__MODULE__{}, binary()) :: %Entry{} | nil
  def get_entry(state, entry_id) do
    state.entries
    |> Enum.find(fn entry -> entry_id == entry.id end)
  end

  @spec get_next_entry(%__MODULE__{}, binary()) :: %Entry{} | nil
  def get_next_entry(state, entry_id) do
    # Grab the entry immediately after the specified entry id (the list is stored in reverse chronological order).
    state.entries
    |> Enum.take_while(fn entry -> entry_id != entry.id end)
    |> List.last()
  end

  @spec get_first_entry_with_timestamp_ms(%__MODULE__{}, integer()) :: %Entry{} | nil
  def get_first_entry_with_timestamp_ms(state, timestamp_ms) do
    state.entries
    |> Enum.find(fn entry -> timestamp_ms == timestamp_ms_from_entry_id(entry.id) end)
  end

  @spec get_entries_range(%__MODULE__{}, binary(), binary()) :: list(%Entry{})
  def get_entries_range(state, start_entry_id, end_entry_id) do
    # NOTE: the start and end range here is inclusive on both ends.
    # NOTE: we store the list of entries in reverse order (i.e. the first entry in the list is the most-recent one), which is why we go from end to start.
    state.entries
    # We don't want any entries that are later (more recent) than the specified end timestamp and sequence number.
    |> Enum.drop_while(fn entry ->
      entry_id_greater_than?(entry.id, end_entry_id)
    end)
    # We don't want any entries that are older (less recent) than the specified start timestamp and sequence number.
    |> Enum.take_while(fn entry ->
      entry_ids_equal?(entry.id, start_entry_id) or
        entry_id_greater_than?(entry.id, start_entry_id)
    end)
    # Finally, reverse the list so we get the entries from start to end.
    |> Enum.reverse()
  end

  @spec entry_id_exists?(%__MODULE__{}, binary()) :: boolean()
  def entry_id_exists?(state, entry_id) do
    case get_entry(state, entry_id) do
      nil -> false
      _ -> true
    end
  end

  @spec add(%__MODULE__{}, %Entry{}) ::
          %__MODULE__{} | :error_not_latest | :error_zero
  def add(state, new_entry) do
    case validate_entry_to_add(state, new_entry) do
      :ok ->
        # NOTE: we prepend the latest entry to the beginning because we're using Elixir linked lists.
        update_in(state.entries, &[new_entry | &1])

      error ->
        error
    end
  end

  @spec resolve_entry_id(%__MODULE__{}, binary()) :: {:ok, binary()} | {:error | error_reason()}
  def resolve_entry_id(state, "*") do
    time_now_ms = System.os_time(:millisecond)
    resolve_entry_id(state, "#{time_now_ms}-*")
  end

  def resolve_entry_id(state, "-") do
    # The entries are stored in reverse order (earliest last).
    case List.last(state.entries) do
      nil -> {:error, :empty_entries}
      entry -> {:ok, entry.id}
    end
  end

  def resolve_entry_id(state, "+") do
    # The entries are stored in reverse order (latest first).
    case List.first(state.entries) do
      nil -> {:error, :empty_entries}
      entry -> {:ok, entry.id}
    end
  end

  # This handles the cases when entry_id is: "<timestamp>" or "<timestamp>-*" or "<timestamp>-<sequence_number>"
  def resolve_entry_id(state, entry_id) do
    timestamp_ms = timestamp_ms_from_entry_id(entry_id)

    sequence_number =
      case String.split(entry_id, "-") |> Enum.at(1) do
        # Having no sequence number implies a 0.
        nil -> 0
        "*" -> next_sequence_number(state, timestamp_ms)
        number -> String.to_integer(number)
      end

    {:ok, "#{timestamp_ms}-#{sequence_number}"}
  end

  @spec next_sequence_number(%__MODULE__{}, integer) :: binary()
  defp next_sequence_number(state, timestamp_ms) do
    # Iterate over the entries from latest to oldest and find the first entry with this timestamp. Add one and that's the next sequence number.
    # Otherwise, use a default of 0 (unless the timestamp is 0, in which case use 1).
    case get_first_entry_with_timestamp_ms(state, timestamp_ms) do
      nil -> if timestamp_ms == 0, do: "1", else: "0"
      latest_entry -> sequence_number_from_entry_id(latest_entry.id) + 1
    end
  end

  @spec timestamp_ms_from_entry_id(binary()) :: integer()
  defp timestamp_ms_from_entry_id(entry_id) do
    String.split(entry_id, "-") |> List.first() |> String.to_integer()
  end

  @spec sequence_number_from_entry_id(binary()) :: integer()
  defp sequence_number_from_entry_id(entry_id) do
    String.split(entry_id, "-") |> List.last() |> String.to_integer()
  end

  @spec entry_ids_equal?(binary(), binary()) :: boolean()
  defp entry_ids_equal?(left_entry_id, right_entry_id) do
    {left_timestamp, left_sequence_number} =
      {timestamp_ms_from_entry_id(left_entry_id), sequence_number_from_entry_id(left_entry_id)}

    {right_timestamp, right_sequence_number} =
      {timestamp_ms_from_entry_id(right_entry_id), sequence_number_from_entry_id(right_entry_id)}

    left_timestamp == right_timestamp and left_sequence_number == right_sequence_number
  end

  @spec entry_id_greater_than?(binary(), binary()) :: boolean()
  defp entry_id_greater_than?(left_entry_id, right_entry_id) do
    {left_timestamp, left_sequence_number} =
      {timestamp_ms_from_entry_id(left_entry_id), sequence_number_from_entry_id(left_entry_id)}

    {right_timestamp, right_sequence_number} =
      {timestamp_ms_from_entry_id(right_entry_id), sequence_number_from_entry_id(right_entry_id)}

    left_timestamp > right_timestamp or
      (left_timestamp == right_timestamp and left_sequence_number > right_sequence_number)
  end

  @spec validate_entry_to_add(%__MODULE__{}, %Entry{}) ::
          :ok | :error_zero | :error_not_latest
  defp validate_entry_to_add(state, new_entry) do
    # Validate the entry ID (must be unique and monotonically increasing).
    latest_entry = List.first(state.entries)

    cond do
      length(state.entries) == 0 ->
        :ok

      entry_id_exists?(state, new_entry.id) ->
        :error_not_latest

      entry_id_greater_than?(new_entry.id, latest_entry.id) ->
        :ok

      new_entry.id == "0-0" ->
        :error_zero

      true ->
        :error_not_latest
    end
  end

  ## Helpers and utility functions.

  @spec stream_entries_to_resp(list(%Entry{})) :: iodata()
  def stream_entries_to_resp(entries) do
    # Given the entries for one stream, return them in the RESP format (this is used for the XRANGE command).
    entries
    |> Enum.map(fn entry ->
      entry_values = for {k, v} <- entry.data, do: [k, v]
      [entry.id, List.flatten(entry_values)]
    end)
    |> RESP.encode(:array)
  end

  @spec multiple_stream_entries_to_resp(list({binary(), list(%Entry{})})) :: iodata()
  def multiple_stream_entries_to_resp(stream_key_entries_pairs) do
    # Given multiple stream keys and their entries, return them in the RESP format (this is used for the XREAD command).
    stream_key_entries_pairs
    |> Enum.map(fn {key, entries} ->
      formatted_entries =
        entries
        |> Enum.map(fn entry ->
          entry_values = for {k, v} <- entry.data, do: [k, v]
          [entry.id, List.flatten(entry_values)]
        end)

      [key, formatted_entries]
    end)
    |> RESP.encode(:array)
  end
end
