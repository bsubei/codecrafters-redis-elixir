defmodule Redis.Application do
  @moduledoc """
  This application is a Redis server that listens to incoming Redis client connections and handles their requests.
  """

  use Application
  alias Redis.ServerInfo

  @impl true
  def start(_type, _args) do
    {opts, _remaining_args, _invalid} =
      OptionParser.parse(System.argv(),
        strict: [port: :integer, replicaof: :string]
      )

    cli_config = Redis.CLIConfig.create(opts)

    # We know we're a slave if we are provided the --replicaof CLI argument.
    role =
      case cli_config.replicaof do
        nil -> :master
        _ -> :slave
      end

    server_info = ServerInfo.init(role)

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
         connected_replicas: MapSet.new()
       }},
      # Start listening to incoming connections from clients / replicas, also initiate the sync handshake with master if we're a replica.
      {Redis.ConnectionManager, %{}}
    ]

    opts = [strategy: :one_for_one, name: Redis.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
