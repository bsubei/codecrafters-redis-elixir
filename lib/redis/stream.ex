defmodule Redis.Stream.Entry do
  @enforce_keys [:id]
  @type t :: %__MODULE__{id: binary(), data: %{}}
  defstruct [:id, data: %{}]
end

defmodule Redis.Stream do
  @moduledoc """
  This module defines a "stream" that is stored in a KeyValueStore. A stream contains multiple entries, each entry has an id and any number of key-value fields.
  """

  # TODO for now just use a list in reversed order (because prepending is O(1) in Elixir), but eventually use a data structure that is more optimized for O(1) random-access lookup AND O(1)-ish range lookups
  @type t :: %__MODULE__{entries: list(%Redis.Stream.Entry{})}
  defstruct entries: []

  @spec get_entry(%__MODULE__{}, binary()) :: %Redis.Stream.Entry{} | nil
  def get_entry(state, entry_id) do
    state.entries
    |> Enum.find(fn entry -> entry_id == entry.id end)
  end

  @spec get_entry_with_timestamp_ms(%__MODULE__{}, integer()) :: %Redis.Stream.Entry{} | nil
  def get_entry_with_timestamp_ms(state, timestamp_ms) do
    state.entries
    |> Enum.find(fn entry -> timestamp_ms == timestamp_ms_from_entry_id(entry.id) end)
  end

  @spec entry_id_exists?(%__MODULE__{}, binary()) :: boolean()
  def entry_id_exists?(state, entry_id) do
    case get_entry(state, entry_id) do
      nil -> false
      _ -> true
    end
  end

  @spec add(%__MODULE__{}, %Redis.Stream.Entry{}) ::
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

  @spec resolve_entry_id(%__MODULE__{}, binary()) :: binary()
  def resolve_entry_id(state, "*") do
    time_now_ms = System.os_time(:millisecond)
    resolve_entry_id(state, "#{time_now_ms}-*")
  end

  # This handles the case when entry_id is either of: "<timestamp>-*" or "<timestamp>-<sequence_number>"
  def resolve_entry_id(state, entry_id) do
    timestamp_ms = timestamp_ms_from_entry_id(entry_id)

    sequence_number =
      case String.split(entry_id, "-") |> List.last() do
        "*" -> next_sequence_number(state, timestamp_ms)
        number -> number
      end

    "#{timestamp_ms}-#{sequence_number}"
  end

  @spec next_sequence_number(%__MODULE__{}, integer) :: binary()
  defp next_sequence_number(state, timestamp_ms) do
    # Iterate over the entries from latest to oldest and find the first entry with this timestamp. Add one and that's the next sequence number.
    # Otherwise, use a default of 0 (unless the timestamp is 0, in which case use 1).
    case get_entry_with_timestamp_ms(state, timestamp_ms) do
      nil -> if timestamp_ms == 0, do: "1", else: "0"
      latest_entry -> sequence_number_from_entry(latest_entry) + 1
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

  # defp timestamp_ms_from_entry(state), do: timestamp_ms_from_entry_id(state.id)
  defp sequence_number_from_entry(state), do: sequence_number_from_entry_id(state.id)

  @spec entry_id_greater_than?(binary(), binary()) :: boolean()
  defp entry_id_greater_than?(left_entry_id, right_entry_id) do
    {left_timestamp, left_sequence_number} =
      {timestamp_ms_from_entry_id(left_entry_id), sequence_number_from_entry_id(left_entry_id)}

    {right_timestamp, right_sequence_number} =
      {timestamp_ms_from_entry_id(right_entry_id), sequence_number_from_entry_id(right_entry_id)}

    left_timestamp > right_timestamp or
      (left_timestamp == right_timestamp and left_sequence_number > right_sequence_number)
  end

  @spec validate_entry_to_add(%__MODULE__{}, %Redis.Stream.Entry{}) ::
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
end
