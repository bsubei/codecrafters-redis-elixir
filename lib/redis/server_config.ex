defmodule Redis.ServerConfig do
  @moduledoc """
  This ServerConfig module holds the configuration used to start up the Redis server, i.e. it contains only immutable options that were set on start-up.
  """

  defstruct [:port]

  @spec create(keyword()) :: %__MODULE__{}
  def create(opts) do
    %__MODULE__{
      port: Keyword.get(opts, :port, 6379)
    }
  end
end
