defmodule Redis.RDB do
  alias Redis.KeyValueStore

  @type decoding_error_t() ::
          __MODULE__.Header.error_t()
          | __MODULE__.Metadata.error_t()
          | __MODULE__.Database.error_t()
          | __MODULE__.EndOfFile.error_t()

  @spec get_empty_rdb() :: binary()
  def get_empty_rdb() do
    hardcoded_rdb_in_hex =
      "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

    Base.decode16!(hardcoded_rdb_in_hex, case: :lower)
  end

  @spec decode_rdb_file(binary()) :: {:ok, KeyValueStore.data_t()} | {:error, atom()}
  def decode_rdb_file(filepath) do
    case File.open(filepath, [:read, :binary]) do
      {:ok, file} ->
        case IO.binread(file, :eof) do
          {:error, reason} -> {:error, reason}
          rdb_data -> decode_rdb(IO.iodata_to_binary(rdb_data))
        end

      error_w_reason ->
        error_w_reason
    end
  end

  @spec decode_rdb(binary()) :: {:ok, KeyValueStore.data_t()} | decoding_error_t()
  def decode_rdb(rdb_data) do
    # TODO there must be a cleaner way to express this "pipeline" of data transformations.
    IO.puts("Decoding RDB file...")

    case __MODULE__.Header.decode(rdb_data) do
      {:ok, header, rest} ->
        IO.puts("Redis version #{header.version} detected...")

        case __MODULE__.Metadata.decode(rest) do
          {:ok, metadata, rest} ->
            IO.puts("#{map_size(metadata)} metadata fields parsed...")

            case __MODULE__.Database.decode(rest) do
              {:ok, database, rest} ->
                case __MODULE__.EndOfFile.decode(rest) do
                  {:ok, :eof, rest} ->
                    # TODO use actual decoded values
                    {:ok, %{}}

                  error_tuple ->
                    error_tuple
                end

              error_tuple ->
                error_tuple
            end

          error_tuple ->
            error_tuple
        end

      error_tuple ->
        error_tuple
    end
  end
end

defmodule Redis.RDB.Header do
  @type error_t :: {:error, :version_too_old | :invalid_header_section}
  @type t :: %__MODULE__{version: non_neg_integer()}
  defstruct [:version]

  @magic_string "REDIS"

  @spec init(non_neg_integer()) :: {:ok, Redis.RDB.Header.t()} | error_t()
  defp init(version) do
    if(version < 9, do: {:error, :version_too_old}, else: {:ok, %__MODULE__{version: version}})
  end

  @spec decode(binary()) :: {:ok, t(), binary()} | error_t()

  def decode(<<@magic_string::binary, version::binary-size(4), rest::binary>>) do
    # Look for the magic string, then extract the version (an integer as 4 ASCII bytes). Return the header version and the rest of the data.

    case init(String.to_integer(version)) do
      {:ok, header} -> {:ok, header, rest}
      error_tuple -> error_tuple
    end
  end

  def decode(_data) do
    {:error, :invalid_header_section}
  end
end

# TODO it's enough to hardcode these fields, not use a general map. Ignore unknown fields.
# redis-ver: The Redis Version that wrote the RDB
# redis-bits: Bit architecture of the system that wrote the RDB, either 32 or 64
# ctime: Creation time of the RDB
# used-mem: Used memory of the instance that wrote the RDB

defmodule Redis.RDB.Metadata do
  @type error_t :: {:error, :invalid_metadata_section}
  @type t :: %{binary() => binary()}

  @metadata_start 0xFA

  @spec decode(binary()) :: {:ok, t(), binary()} | error_t()

  # TODO Zero or more metadata sections
  def decode(<<@metadata_start, rest::binary>>) do
    something = %{}
    {:ok, something, rest}
  end

  def decode(_data) do
    {:error, :invalid_metadata_section}
  end
end

defmodule Redis.RDB.Database do
  @type error_t :: {:error, :invalid_database_section}
  def decode(_data) do
    {:error, :invalid_database_section}
  end
end

defmodule Redis.RDB.EndOfFile do
  @type error_t :: {:error, :invalid_eof_section}
  def decode(_data) do
    {:error, :invalid_eof_section}
  end
end
