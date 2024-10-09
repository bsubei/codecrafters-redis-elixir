defmodule Redis.ServerInfo do
  @moduledoc """
  This ServerInfo module defines the metadata about the given Redis server, i.e. what is accessible via the INFO command.
  """
  require Logger

  defstruct replication: __MODULE__.Replication

  defmodule Replication do
    @moduledoc """
    This Replication module defines the metadata about replication that will show up in the INFO command.
    """
    defstruct [:role, :master_replid, master_repl_offset: 0, connected_slaves: 0]

    def init() do
      init(UUID.uuid4() |> String.replace("-", ""))
    end

    def init(master_replid) when is_binary(master_replid) do
      %__MODULE__{role: :master, master_replid: master_replid}
    end
  end

  @doc ~S"""

    ## Examples

        iex> server_info = %Redis.ServerInfo{replication: Redis.ServerInfo.Replication.init("foo")}
        iex> Redis.ServerInfo.to_string(server_info)
        "# replication\nrole:master\nmaster_replid:foo\nmaster_repl_offset:0\nconnected_slaves:0\n"
  """
  @spec to_string(__MODULE__) :: binary()
  def to_string(server_info) do
    # TODO probably should use a macro for this.
    to_string(server_info, [:replication])
  end

  @spec to_string(__MODULE__, list(atom())) :: binary()
  def to_string(server_info, section_names) do
    IO.iodata_to_binary(to_string(server_info, section_names, []))
  end

  @spec to_string(__MODULE__, list(atom()), iodata()) :: iodata()
  defp to_string(_server_info, [], accumulator), do: accumulator

  @spec to_string(__MODULE__, list(atom()), iodata()) :: iodata()
  defp to_string(server_info, section_names_remaining, accumulator)
       when is_list(section_names_remaining) do
    # Grab the first section name, e.g. :replication.
    [section_name | rest] = section_names_remaining

    # Convert the entire section to a "string", where each field is a string element in an iodata. Make sure to include a newline after each entry.
    section_header = "# #{section_name}\n"

    section_contents =
      Map.get(server_info, section_name)
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
