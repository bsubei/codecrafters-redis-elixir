defmodule Redis.RDB do
  alias Redis.KeyValueStore

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
          rdb_data -> decode_rdb(rdb_data)
        end

      error_w_reason ->
        error_w_reason
    end
  end

  @spec decode_rdb(iodata()) :: {:ok, KeyValueStore.data_t()} | {:error, atom()}
  def decode_rdb(rdb_data) do
    case __MODULE__.Header.decode(rdb_data) do
      {:ok, _header, _rest} ->
        # TODO continue decoding
        {:ok, %{}}

      error_tuple ->
        error_tuple
    end
  end
end

defmodule Redis.RDB.Header do
  @type error_t :: {:error, :version_too_old}
  @type t :: %__MODULE__{version: non_neg_integer()}
  defstruct [:version]

  @magic_string "REDIS"

  @spec init(non_neg_integer()) :: {:ok, Redis.RDB.Header.t()} | error_t()
  defp init(version) do
    if(version < 9, do: {:error, :version_too_old}, else: {:ok, %__MODULE__{version: version}})
  end

  @spec decode(iodata()) :: {:ok, t(), iodata()} | error_t()

  def decode(<<@magic_string::binary, version::binary-size(4), rest::binary>>) do
    # Look for the magic string, then extract the version. Return the header version and the rest of the data.

    case init(String.to_integer(version)) do
      {:ok, header} -> {:ok, header, rest}
      error_tuple -> error_tuple
    end
  end

  def decode(_data) do
    {:error, :invalid_file_format}
  end
end
