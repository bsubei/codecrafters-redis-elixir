defmodule Redis.ServerState do
  @moduledoc """
  This ServerState module holds all the mutable data describing the current state of the Redis server. e.g. the current replication offset, or whether it's a replica or a master.

  It also holds the ServerConfig, which has the options used to originally start up this server.
  """
  # We use an Agent because GenServer is overkill.
  use Agent
  alias Redis.CLIConfig
  alias Redis.ServerInfo

  defstruct cli_config: CLIConfig, server_info: ServerInfo

  @spec start_link(%__MODULE__{}) :: Agent.on_start()
  def start_link(init_data), do: Agent.start_link(fn -> init_data end, name: __MODULE__)

  @spec get_state() :: %__MODULE__{}
  def get_state() do
    Agent.get(__MODULE__, & &1)
  end

  @spec set_state(%__MODULE__{}) :: :ok
  def set_state(new_state) do
    Agent.update(__MODULE__, fn _ -> new_state end)
  end
end
