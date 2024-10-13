defmodule Redis.ServerState do
  @moduledoc """
  This ServerState module holds all the mutable data describing the current state of the Redis server. e.g. the current replication offset, or whether it's a replica or a master.

  It also holds the ServerConfig, which has the options used to originally start up this server.
  """
  # We use an Agent because GenServer is overkill.
  use Agent
  alias Redis.CLIConfig
  alias Redis.ServerInfo
  alias Redis.Connection

  @type t :: %__MODULE__{
          cli_config: %CLIConfig{},
          server_info: %ServerInfo{},
          # NOTE: these Connections are copied at the point when that replica became connected. i.e. some of its state may become stale (it's safe to access the socket and send_fn though).
          connected_replicas: MapSet.t(%Connection{})
        }
  defstruct cli_config: CLIConfig, server_info: ServerInfo, connected_replicas: MapSet.new()

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

  @spec add_connected_replica(%Connection{}) :: :ok
  def add_connected_replica(replica) do
    Agent.update(__MODULE__, fn state ->
      update_in(state.connected_replicas, fn replicas -> MapSet.put(replicas, replica) end)
    end)
  end

  @spec has_connected_replica(:gen_tcp.socket()) :: boolean()
  def has_connected_replica(replica) do
    Agent.get(__MODULE__, fn state -> state.connected_replicas end) |> MapSet.member?(replica)
  end
end
