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

  @spec decode_rdb_file(binary()) :: {:ok, list(KeyValueStore.data_t())} | {:error, atom()}
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

  @spec decode_rdb(binary()) :: {:ok, list(KeyValueStore.data_t())} | decoding_error_t()
  def decode_rdb(rdb_data) do
    # TODO there must be a cleaner way to express this "pipeline" of data transformations.
    IO.puts("Decoding RDB file...")

    case __MODULE__.Header.decode(rdb_data) do
      {:ok, header, rest} ->
        IO.puts("Redis version #{header.version} detected...")

        case __MODULE__.Metadata.decode(rest) do
          {:ok, %{} = metadata_sections, rest} ->
            IO.puts("Decoded #{map_size(metadata_sections)} metadata section(s):")
            IO.puts("#{metadata_sections}")

            case __MODULE__.Database.decode(rest) do
              {:ok, database_sections, rest} ->
                IO.puts("Decoded #{length(database_sections)} database section(s)...")

                IO.puts(
                  "The lengths of the database sections: #{inspect(database_sections |> Enum.map(&map_size(&1)))}..."
                )

                case __MODULE__.EndOfFile.decode(rest, rdb_data) do
                  :ok ->
                    # TODO return actual useful stuff
                    {:ok, database_sections}

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

  @spec decode_string(binary()) :: {:ok, binary(), binary()} | {:error, atom()}

  # If the two MSBs are "0b11", then this is either an integer or an LZF compressed string.
  def decode_string(<<0b11::2, lsbs::6, rest::binary>>) do
    decode_special_string(lsbs, rest)
  end

  def decode_string(data) do
    # Get the length from the prefix.
    case decode_length_prefix(data) do
      {:ok, length, rest} ->
        # Now read that many bytes. That's our string.
        <<bytes::binary-size(length), rest::binary>> = rest
        {:ok, bytes, rest}

      error_tuple ->
        error_tuple
    end
  end

  @spec decode_length_prefix(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, atom()}
  def decode_length_prefix(<<0b00::2, lsbs::6, rest::binary>>) do
    # The 6 LSBs represent the length
    {:ok, lsbs, rest}
  end

  def decode_length_prefix(<<0b01::2, length::14, rest::binary>>) do
    # Read one more byte, and together with the 6 LSBs, that's the length.
    # TODO little vs big endian
    {:ok, length, rest}
  end

  def decode_length_prefix(<<0b10::2, _lsbs::6, length::32, rest::binary>>) do
    # The next 4 bytes represent the length
    {:ok, length, rest}
  end

  def decode_length_prefix(_) do
    # We don't expect the MSBs to be 0b11, that should never happen.
    {:error, :invalid_length_prefix}
  end

  @spec decode_special_string(byte(), binary()) :: {:ok, binary(), binary()} | {:error, atom()}
  defp decode_special_string(0, <<length::8, data::binary-size(length), rest>>) do
    # The next 8 bits are the length of the "integer as string" that follows.
    {:ok, data, rest}
  end

  defp decode_special_string(1, <<length::16, data::binary-size(length), rest>>) do
    # The next 16 bits are the length of the "integer as string" that follows.
    {:ok, data, rest}
  end

  defp decode_special_string(2, <<length::32, data::binary-size(length), rest>>) do
    # The next 32 bits are the length of the "integer as string" that follows.
    {:ok, data, rest}
  end

  defp decode_special_string(3, _data) do
    # LZF compressed string, not implemented
    {:error, :unimplemented_lzf_string}
  end

  defp decode_special_string(_, _) do
    {:error, :invalid_string_encoding}
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
  @type error_t ::
          {:error,
           :missing_metadata_start_byte
           | :bad_metadata_key
           | :bad_metadata_value}
  @type t :: %{binary() => binary()}
  @metadata_start 0xFA

  @spec decode(binary(), t()) :: {:ok, t(), binary()} | error_t()
  def decode(input, acc \\ %{})

  # Decodes zero or more metadata sections.
  def decode(<<@metadata_start, rest::binary>>, acc) do
    # TODO parse the actual metadata section here
    # TODO there must be one key-value pair in each metadata section
    case decode_metadata_pair(rest) do
      {:ok, key, value, new_rest} ->
        # TODO
        new_acc = %{acc | key => value}
        decode(new_rest, new_acc)

      error_tuple ->
        error_tuple
    end
  end

  # If on the first database section decode we find no metadata start byte, this is an error.
  def decode(_data, %{}) do
    {:error, :missing_metadata_start_byte}
  end

  # If on subsequent recursive calls we find no metadata start byte, then we've decoded enough.
  def decode(data, acc) do
    {:ok, acc, data}
  end

  @spec decode_metadata_pair(binary()) :: {:ok, binary(), binary(), binary()} | error_t()
  defp decode_metadata_pair(data) do
    # TODO I don't think decode_string can meaningfully return an error. It'll always decode whatever bytes it finds and can't know if there's an "error"
    case Redis.RDB.decode_string(data) do
      {:ok, key, rest} ->
        case Redis.RDB.decode_string(rest) do
          {:ok, value, rest} ->
            {:ok, key, value, rest}

          _error_tuple ->
            {:error, :bad_metadata_value}
        end

      _error_tuple ->
        {:error, :bad_metadata_key}
    end
  end
end

defmodule Redis.RDB.Database do
  @type error_t :: {:error, :invalid_database_section}
  # Each database section gets parsed as a KeyValueStore.
  @type t :: Redis.KeyValueStore.data_t()
  @database_start 0xFE

  @spec decode(binary(), list(t())) :: {:ok, list(t()), binary()} | error_t()
  def decode(data, acc \\ [])

  def decode(<<@database_start, rest::binary>>, acc) do
    # TODO implement this
    case rest do
      "0" ->
        {:error, :invalid_database_section}

      _ ->
        database_section = %{}
        new_acc = acc ++ [database_section]
        decode(rest, new_acc)
    end
  end

  # If on the first database section decode we find no database start byte, this is an error.
  def decode(_data, []) do
    {:error, :invalid_database_section}
  end

  # If on subsequent recursive calls we find no database start byte, then we've decoded enough.
  def decode(data, acc) do
    {:ok, acc, data}
  end
end

defmodule Redis.RDB.EndOfFile do
  @type error_t :: {:error, :invalid_eof_section}
  @eof_start 0xFF

  @spec decode(binary(), binary()) :: :ok | error_t()

  def decode(<<@eof_start, rest::binary>>, all_data) do
    crc_check(all_data, rest)
  end

  def decode(_data, _all_data) do
    {:error, :invalid_eof_section}
  end

  @spec crc_check(binary(), binary()) :: :ok | error_t()
  def crc_check(all_data, crc) do
    # TODO actually implement
    if all_data == crc do
      :ok
    else
      {:error, :invalid_crc}
    end
  end
end
