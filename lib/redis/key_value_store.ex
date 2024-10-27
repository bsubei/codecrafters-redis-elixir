defmodule Redis.KeyValueStore do
  @moduledoc """
  This KeyValueStore module holds the key-value data store of the Redis server.
  """

  # We use an Agent because GenServer is overkill.
  use Agent
  alias Redis.Value

  @spec start_link(%{binary() => Value}) :: Agent.on_start()
  def start_link(init_data), do: Agent.start_link(fn -> init_data end, name: __MODULE__)

  @spec get(String.Chars.t(), :no_expiry) :: Value | nil
  def get(key, :no_expiry) when is_binary(key) do
    Agent.get(__MODULE__, &Map.get(&1, key))
  end

  @spec get(String.Chars.t()) :: Value | nil
  def get(key) do
    # NOTE: we do the lookup in the Agent itself because it's probably faster than copying all the data and the caller doing the lookup itself.
    case Agent.get(__MODULE__, &Map.get(&1, key)) do
      # Key not found, return nil.
      nil ->
        nil

      # Key found, now check if a timestamp is specified and if it expired.
      %Value{expiry_timestamp_epoch_ms: expiry} = value ->
        if expiry == nil or expiry >= System.os_time(:millisecond), do: value, else: nil
    end
  end

  @spec set(String.Chars.t(), String.Chars.t(), number() | nil) :: :ok
  def set(key, data, expiry_timestamp_epoch_ms \\ nil) do
    Agent.update(__MODULE__, &Map.put(&1, key, Value.init(data, expiry_timestamp_epoch_ms)))
  end

  def clear(), do: Agent.update(__MODULE__, fn _ -> %{} end)
end
