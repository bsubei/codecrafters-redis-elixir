defmodule Redis.ServerInfo do
  @moduledoc """
  This ServerInfo module defines the metadata about the given Redis server, i.e. what is accessible via the INFO command.
  """
  require Logger

  defstruct replication: __MODULE__.Replication

  defmodule Replication do
    @moduledoc """
    This Replication module defines the metadata about replication that will show up in the INFO command.

    role is always either :master or :slave
    """
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

  def init(role) when is_atom(role) do
    %__MODULE__{replication: Replication.init(role)}
  end

  @doc ~S"""
    Convert the contents of all or the specified sections of the ServerInfo to a displayable string.

    ## Examples

        iex> server_info = %Redis.ServerInfo{replication: Redis.ServerInfo.Replication.init(:master, "foo")}
        iex> lines = Redis.ServerInfo.to_string(server_info) |> String.split("\n") |> MapSet.new()
        iex> assert "role:master" in lines and assert "master_replid:foo" in lines
        iex> lines = Redis.ServerInfo.to_string(server_info, ["replication"]) |> String.split("\n") |> MapSet.new()
        iex> assert "role:master" in lines and assert "master_replid:foo" in lines
  """
  @spec to_string(__MODULE__) :: binary()
  def to_string(server_info) do
    # TODO probably should use a macro for this.
    to_string(server_info, ["replication"])
  end

  @spec to_string(__MODULE__, list(binary())) :: binary()
  def to_string(server_info, section_names) do
    IO.iodata_to_binary(to_string(server_info, section_names, []))
  end

  @spec to_string(__MODULE__, list(binary()), iodata()) :: iodata()
  defp to_string(_server_info, [], accumulator), do: accumulator

  @spec to_string(__MODULE__, list(binary()), iodata()) :: iodata()
  defp to_string(server_info, section_names_remaining, accumulator)
       when is_list(section_names_remaining) do
    # Grab the first section name, e.g. :replication.
    [section_name | rest] = section_names_remaining

    # Convert the entire section to a "string", where each field is a string element in an iodata. Make sure to include a newline after each entry.
    section_header = "# #{section_name}\n"

    section_contents =
      Map.get(server_info, String.to_atom(section_name))
      |> Map.from_struct()
      |> Enum.map_join("\n", fn
        {k, nil} -> "#{k}:"
        {k, v} when is_atom(v) -> "#{k}:#{Atom.to_string(v)}"
        {k, v} -> "#{k}:#{v}"
      end)

    section_as_strings = [section_header, section_contents, "\n"]

    # Add this recent one to the accumulator and recurse.
    new_acc = [accumulator, section_as_strings]
    to_string(server_info, rest, new_acc)
  end
end
