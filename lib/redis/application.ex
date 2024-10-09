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

    role =
      case cli_config.replicaof do
        nil -> :master
        _ -> :slave
      end

    server_info = ServerInfo.init(role)

    children = [
      # TODO we hardcode the key-value store to be empty on startup. Load from file later.
      {Redis.KeyValueStore, %{}},
      # TODO if the server state restarts, it should probably restart everything. Figure out a proper supervision tree for this later.
      {Redis.ServerState, %Redis.ServerState{cli_config: cli_config, server_info: server_info}},
      {Redis.ConnectionAcceptor, {}}
    ]

    opts = [strategy: :one_for_one, name: Redis.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
