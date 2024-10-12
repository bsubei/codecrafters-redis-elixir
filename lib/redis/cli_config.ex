defmodule Redis.CLIConfig do
  @moduledoc """
  This module holds the configuration used to start up the Redis server, i.e. it contains only immutable options that were set on start-up.
  Generally, it contains things passed in as CLI arguments.
  """

  @type t :: %__MODULE__{port: integer() | nil, replicaof: binary() | nil}
  defstruct [:port, :replicaof]

  @spec create(keyword()) :: %__MODULE__{}
  def create(opts) do
    %__MODULE__{
      port: Keyword.get(opts, :port, 6379),
      replicaof: Keyword.get(opts, :replicaof)
    }
  end
end
