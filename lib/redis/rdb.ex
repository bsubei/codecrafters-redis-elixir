defmodule Redis.RDB do
  alias Redis.KeyValueStore

  def get_rdb_file() do
    hardcoded_rdb_in_hex =
      "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

    Base.decode16!(hardcoded_rdb_in_hex, case: :lower)
  end

  @spec decode_rdb_file(binary()) :: {:ok, KeyValueStore.data_t()} | {:error, atom()}
  def decode_rdb_file(dbfilename) do
    # TODO read file, return :file_does_not_exist error reason
    rdb_data = []
    # TODO decode it
    decode_rdb(rdb_data)
  end

  @spec decode_rdb(iodata()) :: {:ok, KeyValueStore.data_t()} | {:error, atom()}
  def decode_rdb(rdb_data) do
  end
end
