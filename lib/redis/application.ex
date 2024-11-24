defmodule Redis.Application do
  @moduledoc """
  This application is a Redis server that listens to incoming Redis client connections and handles their requests.
  """

  use Application
  alias Redis.{CLIConfig, KeyValueStore, ServerInfo}

  @impl true
  def start(_type, _args) do
    cli_config = Redis.CLIConfig.init()
    server_info = ServerInfo.init(cli_config)

    children = [
      # Set up our key value store as empty.
      # TODO same deal here with supervision tree. If the key-value store goes down, we probably have to shut down the entire server.
      {Redis.KeyValueStore, read_rdb_on_startup(cli_config)},
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

  @spec read_rdb_on_startup(CLIConfig.t()) :: KeyValueStore.data_t()
  def read_rdb_on_startup(cli_config) do
    # Read in the RDB file on startup, otherwise use an empty map as the KeyValueStore.
    case {cli_config.dir, cli_config.dbfilename} do
      {nil, _} ->
        IO.puts("Starting up with a fresh KeyValueStore since no RDB filepath was provided")
        %{}

      {_, nil} ->
        IO.puts("Starting up with a fresh KeyValueStore since no RDB filepath was provided")
        %{}

      {dir, dbfilename} ->
        filepath = "#{dir}/#{dbfilename}"

        case Redis.RDB.decode_rdb_file(filepath) do
          {:ok, rdb_data} ->
            rdb_data

          {:error, :file_does_not_exist} ->
            IO.puts(
              "Could not find RDB file: #{filepath}, continuing with an empty KeyValueStore!"
            )

            %{}

          {:error, reason} ->
            raise "Unhandled error #{reason} while reading RDB file..."
        end
    end
  end
end
