defmodule Redis.ServerInfo.Replication do
  @moduledoc """
  This Replication module defines the metadata about replication that will show up in the INFO command.

  role is always either :master or :slave
  """
  @enforce_keys [:role, :master_replid]
  @type t :: %__MODULE__{
          role: atom(),
          master_replid: binary(),
          master_repl_offset: integer(),
          connected_slaves: integer()
        }
  defstruct [:role, :master_replid, master_repl_offset: 0, connected_slaves: 0]

  def init(role) when is_atom(role) do
    # TODO use UUID to get an actual randomized uuid when I figure out how to get codecrafters to load in deps. Use a hardcoded string for now.
    # init(role, UUID.uuid4() |> String.replace("-", ""))
    init(role, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
  end

  def init(role, master_replid) when is_atom(role) and is_binary(master_replid) do
    %__MODULE__{role: role, master_replid: master_replid}
  end
end

defmodule Redis.ServerInfo.Persistence do
  @type t :: %__MODULE__{
          dir: binary() | nil,
          dbfilename: binary() | nil
        }
  defstruct [:dir, :dbfilename]
end

defmodule Redis.ServerInfo do
  @moduledoc """
  This ServerInfo module defines the metadata about the given Redis server, i.e. what is accessible via the INFO command.
  """
  require Logger

  @type t :: %__MODULE__{
          replication: __MODULE__.Replication.t(),
          persistence: __MODULE__.Persistence.t()
        }
  defstruct [:replication, :persistence]

  def init(%Redis.CLIConfig{replicaof: replicaof, dir: dir, dbfilename: dbfilename}) do
    # We know we're a slave if we are provided the --replicaof CLI argument.
    role =
      case replicaof do
        nil -> :master
        _ -> :slave
      end

    %__MODULE__{
      replication: __MODULE__.Replication.init(role),
      persistence: %__MODULE__.Persistence{dir: dir, dbfilename: dbfilename}
    }
  end

  @doc ~S"""
    Convert the contents of all or the specified sections of the ServerInfo to a displayable string.

    ## Examples

        iex> server_info = %Redis.ServerInfo{replication: Redis.ServerInfo.Replication.init(:master, "foo")}
        iex> lines = Redis.ServerInfo.get_human_readable_string(server_info) |> String.split("\n") |> MapSet.new()
        iex> assert "role:master" in lines and assert "master_replid:foo" in lines
        iex> lines = Redis.ServerInfo.get_human_readable_string(server_info, ["replication"]) |> String.split("\n") |> MapSet.new()
        iex> assert "role:master" in lines and assert "master_replid:foo" in lines
  """
  @spec get_human_readable_string(t()) :: binary()
  def get_human_readable_string(server_info) do
    # TODO probably should use a macro for this.
    get_human_readable_string(server_info, ["replication", "persistence"])
  end

  @spec get_human_readable_string(t(), list(binary())) :: binary()
  def get_human_readable_string(server_info, section_names) do
    IO.iodata_to_binary(get_human_readable_string(server_info, section_names, []))
  end

  @spec get_human_readable_string(t(), list(binary()), iodata()) :: iodata()
  defp get_human_readable_string(_server_info, [], accumulator), do: accumulator

  defp get_human_readable_string(server_info, section_names_remaining, accumulator) do
    # Grab the first section name, e.g. :replication.
    [section_name | rest] = section_names_remaining

    # Convert the entire section to a "string", where each field is a string element in an iodata. Make sure to include a newline after each entry.
    section_header = "# #{section_name}\n"

    new_acc =
      case server_info |> Map.get(String.to_atom(section_name)) do
        nil ->
          accumulator

        section ->
          section_contents =
            Map.from_struct(section)
            |> Enum.map_join("\n", fn
              {k, nil} -> "#{k}:"
              {k, v} when is_atom(v) -> "#{k}:#{Atom.to_string(v)}"
              {k, v} -> "#{k}:#{v}"
            end)

          section_as_strings = [section_header, section_contents, "\n"]
          [accumulator, section_as_strings]
      end

    # Add this section to the accumulator and recurse.
    get_human_readable_string(server_info, rest, new_acc)
  end

  @spec get_all_keywords(t()) :: %{binary() => binary()}
  def get_all_keywords(state) do
    # Merge all the structs together as a single map.
    [state.replication, state.persistence]
    |> Enum.map(&Map.from_struct/1)
    |> Enum.reduce(&Map.merge/2)
    |> Enum.into(%{}, fn {k, v} -> {Atom.to_string(k), to_string(v)} end)
  end
end
