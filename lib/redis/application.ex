defmodule Redis.Application do
  @moduledoc """
  This application is a Redis server that listens to incoming Redis client connections and handles their requests.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # TODO we hardcode the key-value store to be empty on startup. Load from file later.
      {Redis.KeyValueStore, %{}},
      {Redis.ConnectionAcceptor, port: 6379}
    ]

    opts = [strategy: :one_for_one, name: Redis.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
