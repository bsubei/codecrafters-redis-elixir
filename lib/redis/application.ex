defmodule Redis.Application do
  @moduledoc """
  This application is a Redis server that listens to incoming Redis client connections and handles their requests.
  """

  use Application
  alias Redis.ServerInfo

  @impl true
  def start(_type, _args) do
    cli_config = Redis.CLIConfig.init()
    server_info = ServerInfo.init(cli_config)

    children = [
      # Set up our key value store as empty.
      # TODO we hardcode the key-value store to be empty on startup. Load from file later.
      # TODO same deal here with supervision tree. If the key-value store goes down, we probably have to shut down the entire server.
      {Redis.KeyValueStore, %{}},
      # Set up our server state.
      # TODO if the server state restarts, it should probably restart everything. Figure out a proper supervision tree for this later.
      {Redis.ServerState,
       %Redis.ServerState{
         cli_config: cli_config,
         server_info: server_info,
         connected_replicas: %{}
       }},
      # Start listening to incoming connections from clients / replicas, also initiate the sync handshake with master if we're a replica.
      {Redis.ConnectionManager, %{}}
    ]

    opts = [strategy: :one_for_one, name: Redis.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
