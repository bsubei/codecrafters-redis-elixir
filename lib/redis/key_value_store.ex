defmodule Redis.KeyValueStore do
  # We use an Agent because GenServer is overkill.
  use Agent

  @spec start_link(%{}) :: Agent.on_start()
  def start_link(init_data), do: Agent.start_link(fn -> init_data end, name: __MODULE__)

  @spec get(String.Chars.t()) :: String.Chars.t() | nil
  def get(key) when is_binary(key) do
    # NOTE: we do the lookup in the Agent itself because it's probably faster than copying all the data so that the caller can do the lookup itself.
    case Agent.get(__MODULE__, &Map.get(&1, key)) do
      # Key not found.
      nil ->
        nil

      # Key found, now check if a timestamp is specified and if it expired.
      {value, expiry_timestamp_epoch_s} ->
        case expiry_timestamp_epoch_s do
          nil ->
            value

          _ ->
            # TODO handle checking for expired timestamps
            value
        end
    end
  end

  @spec set(String.Chars.t(), String.Chars.t()) :: :ok
  def set(key, value) do
    __MODULE__.set(key, value, nil)
  end

  @spec set(String.Chars.t(), String.Chars.t(), number() | nil) :: :ok
  def set(key, value, expiry_timestamp_epoch_s) do
    Agent.update(__MODULE__, &Map.put(&1, key, {value, expiry_timestamp_epoch_s}))
  end

  def clear(), do: Agent.update(__MODULE__, fn _ -> %{} end)
end
