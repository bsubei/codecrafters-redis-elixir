defmodule Redis.CLIConfig do
  @moduledoc """
  This module holds the configuration used to start up the Redis server, i.e. it contains only immutable options that were set on start-up.
  Generally, it contains things passed in as CLI arguments.
  """

  @type t :: %__MODULE__{
          port: integer() | nil,
          replicaof: binary() | nil,
          dir: binary() | nil,
          dbfilename: binary() | nil
        }
  defstruct [:port, :replicaof, :dir, :dbfilename]

  @spec init() :: t()
  def init() do
    {opts, _remaining_args, _invalid} =
      OptionParser.parse(System.argv(),
        strict: [port: :integer, replicaof: :string, dir: :string, dbfilename: :string]
      )

    %__MODULE__{
      port: Keyword.get(opts, :port, 6379),
      replicaof: Keyword.get(opts, :replicaof),
      dir: Keyword.get(opts, :dir),
      dbfilename: Keyword.get(opts, :dbfilename)
    }
  end
end
