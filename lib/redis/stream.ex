defmodule Redis.Stream do
  @moduledoc """
  This module defines a "stream" that is stored in a KeyValueStore. A stream contains multiple entries, each entry has an id and any number of key-value fields.
  """
  defmodule Entry do
    @enforce_keys [:id]
    @type t :: %__MODULE__{id: binary(), data: %{}}
    defstruct [:id, data: %{}]
  end

  # TODO for now just use a list, but eventually use a data structure that is more optimized for O(1) random-access lookup AND O(1)-ish range lookups
  @enforce_keys [:entries]
  @type t :: %__MODULE__{entries: list(%Redis.Stream.Entry{})}
  defstruct [:entries]

  @spec get_entry_id(%__MODULE__{}, binary()) :: %Redis.Stream.Entry{} | nil
  def get_entry_id(state, entry_id) do
    state.entries
    |> Enum.find(fn entry -> entry_id == entry.id end)
  end

  @spec entry_id_exists?(%__MODULE__{}, binary()) :: boolean()
  def entry_id_exists?(state, entry_id) do
    case get_entry_id(state, entry_id) do
      nil -> false
      _ -> true
    end
  end

  # TODO we don't guarantee unique entry IDs at this point.
  @spec append(%__MODULE__{}, %Redis.Stream.Entry{}) :: %__MODULE__{}
  def append(state, entry) do
    update_in(state.entries, fn
      entries -> [entries | entry]
    end)
  end
end
