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
          {:ok, %{} = metadata, rest} ->
            IO.puts("Decoded #{map_size(metadata)} metadata entries:")
            IO.puts("#{inspect(metadata)}")

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
  def decode_string(<<0b11::2, 0b00::6, num::integer-8-little, rest::binary>>) do
    # The string is encoded as an 8-bit integer.
    {:ok, Integer.to_string(num), rest}
  end

  def decode_string(<<0b11::2, 0b01::6, num::integer-16-little, rest::binary>>) do
    # The string is encoded as a 16-bit little endian integer.
    {:ok, Integer.to_string(num), rest}
  end

  def decode_string(<<0b11::2, 0b10::6, num::integer-32-little, rest::binary>>) do
    # The string is encoded as a 32-bit little endian integer.
    {:ok, Integer.to_string(num), rest}
  end

  def decode_string(<<0b11::2, 0b11::6, _rest::binary>>) do
    # LZF compressed string, not implemented
    {:error, :unimplemented_lzf_string}
  end

  def decode_string(<<0b11::2, _rest::binary>>) do
    {:error, :invalid_string_encoding}
  end

  def decode_string("") do
    {:error, :nothing_to_decode}
  end

  def decode_string(data) do
    # A length-encoded int comes first and that's the length of our string.
    case decode_int(data) do
      {:ok, length, rest} ->
        # Now read that many bytes. That's our string.
        <<bytes::binary-size(length), rest::binary>> = rest
        {:ok, bytes, rest}

      error_tuple ->
        error_tuple
    end
  end

  @spec decode_int(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, atom()}

  @spec decode_int(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, atom()}
  def decode_int(<<0b00::2, lsbs::6, rest::binary>>) do
    # The 6 LSBs represent the length
    {:ok, lsbs, rest}
  end

  def decode_int(<<0b01::2, length::14, rest::binary>>) do
    # Read one more byte, and together with the 6 LSBs, that's the length.
    {:ok, length, rest}
  end

  def decode_int(<<0b10::2, _lsbs::6, length::integer-32-little, rest::binary>>) do
    # The next 4 bytes (little endian) represent the length
    {:ok, length, rest}
  end

  def decode_int(_) do
    # We don't expect the MSBs to be 0b11, that should never happen.
    {:error, :invalid_length_encoded_int}
  end
end

defmodule Redis.RDB.Header do
  @type error_t :: {:error, :version_too_old | :invalid_header_section}
  @type t :: %__MODULE__{version: non_neg_integer()}
  defstruct [:version]

  @magic_string "REDIS"
  @min_supported_version 9

  @spec init(non_neg_integer()) :: {:ok, Redis.RDB.Header.t()} | error_t()
  defp init(version) do
    if(version < @min_supported_version,
      do: {:error, :version_too_old},
      else: {:ok, %__MODULE__{version: version}}
    )
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
  @type error_t :: {:error, :bad_metadata_key | :bad_metadata_value}
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
        # IO.puts("Decoded #{key}, #{value}, rest: #{inspect(new_rest, base: :hex)}")
        new_acc = Map.merge(acc, %{key => value})
        decode(new_rest, new_acc)

      error_tuple ->
        error_tuple
    end
  end

  # If on subsequent recursive calls we find no metadata start byte, then we've decoded enough.
  def decode(data, acc) do
    {:ok, acc, data}
  end

  @spec decode_metadata_pair(binary()) :: {:ok, binary(), binary(), binary()} | error_t()
  defp decode_metadata_pair(data) do
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
  @type error_t :: {:error, :invalid_resizedb | :invalid_resizedb_expiry_number | :missing_eof}
  # Each database subsection gets parsed as a KeyValueStore.
  @type t :: Redis.KeyValueStore.data_t()
  @database_start 0xFE
  @resizedb_start 0xFB
  @expiry_s_entry_start 0xFD
  @expiry_ms_entry_start 0xFC
  # Yes, this is also defined in another module, but it's never going to change, so it's fine.
  @eof_start 0xFF

  @spec decode_subsection(binary()) :: {:ok, {non_neg_integer(), t()}, binary()} | error_t()
  defp decode_subsection(data) do
    # Grab the index from the database selector, i.e. the index of the database we're about to decode.
    <<db_index::8, rest::binary>> = data

    # Grab the resizedb section, which is only used for validation in our case since we won't bother with pre-allocating the database hashmap.
    case decode_resizedb(rest) do
      {:ok, {num_total_entries, num_expiry_entries}, rest} ->
        case decode_entries(rest) do
          {:ok, entries = %{}, rest} ->
            # validate total entries and expiry entries
            case validate_entries(entries, num_total_entries, num_expiry_entries) do
              :ok ->
                {:ok, {db_index, entries}, rest}

              error_tuple ->
                error_tuple
            end
        end

      error_tuple ->
        error_tuple
    end
  end

  @spec decode_resizedb(binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}, binary()} | error_t()
  defp decode_resizedb(<<@resizedb_start, rest::binary>>) do
    # The resizedb section simply has two length-encoded integers.
    case Redis.RDB.decode_int(rest) do
      {:ok, num_total_entries, rest} ->
        case Redis.RDB.decode_int(rest) do
          {:ok, num_expiry_entries, rest} ->
            {:ok, {num_total_entries, num_expiry_entries}, rest}

          _ ->
            {:error, :invalid_resizedb}
        end

      _ ->
        {:error, :invalid_resizedb}
    end
  end

  defp decode_resizedb(_data), do: {:error, :invalid_resizedb}

  @spec decode_entries(binary(), t()) :: {:ok, t(), binary()} | error_t()
  defp decode_entries(data, acc \\ %{})
  # NOTE: we don't strip off the eof or db first byte if we return without recursing.
  # If we see the end of file, that's the end of this database section.
  defp decode_entries(<<@eof_start, _rest::binary>> = data, acc), do: {:ok, acc, data}
  # If we see the start of the next of the next db, that's the end of this database section.
  defp decode_entries(<<@database_start, _rest::binary>> = data, acc), do: {:ok, acc, data}

  defp decode_entries("", _acc) do
    {:error, :missing_eof}
  end

  defp decode_entries(<<@expiry_s_entry_start, expiry_s::integer-32-little, rest::binary>>, acc) do
    expiry_ms = expiry_s * 1000
    decode_entries_impl(rest, acc, expiry_ms)
  end

  defp decode_entries(<<@expiry_ms_entry_start, expiry_ms::integer-64-little, rest::binary>>, acc) do
    decode_entries_impl(rest, acc, expiry_ms)
  end

  defp decode_entries(data, acc) do
    decode_entries_impl(data, acc)
  end

  @spec decode_entries_impl(binary(), t(), non_neg_integer() | nil) ::
          {:ok, t(), binary()} | error_t()
  defp decode_entries_impl(data, acc, expiry_ms \\ nil)

  defp decode_entries_impl(<<0::8, rest::binary>>, acc, expiry_ms) do
    # Read key
    case Redis.RDB.decode_string(rest) do
      {:ok, key, rest} ->
        # Read value
        case Redis.RDB.decode_string(rest) do
          {:ok, value, rest} ->
            # Put them together with the expiry into the acc and recurse.
            value = Redis.Value.init(value, expiry_ms)
            new_acc = Map.merge(acc, %{key => value})
            decode_entries(rest, new_acc)

          error_tuple ->
            error_tuple
        end

      error_tuple ->
        error_tuple
    end
  end

  # TODO we only support string types for now.
  defp decode_entries_impl(<<_value_type::8, _rest::binary>>, _acc, _expiry_ms),
    do: {:error, :unimplemented_value_type}

  @spec validate_entries(t(), non_neg_integer(), non_neg_integer()) :: :ok | {:error, atom()}
  defp validate_entries(entries, num_total, num_expiry) do
    length = map_size(entries)

    length_w_expiry =
      entries |> Enum.count(fn {_key, value} -> value.expiry_timestamp_epoch_ms != nil end)

    cond do
      num_expiry > num_total -> {:error, :invalid_resizedb_expiry_number}
      length != num_total -> {:error, :missing_entries}
      length_w_expiry != num_expiry -> {:error, :missing_entries}
      true -> :ok
    end
  end

  @spec decode(binary(), list(t())) :: {:ok, list(t()), binary()} | error_t()
  def decode(data, acc \\ [])

  # There are three pieces of information we can extract from a database subsection:
  # 1. this database subsection's index
  # 2. the hash table size (think of it as the database header)
  # 3. and each key-value entry for this database subsection
  # This function decodes zero or more database subsections, and returns them as an ordered list of Maps (each Map is a "database").
  def decode(<<@database_start, rest::binary>>, acc) do
    # %{integer() => t()} ==== %{integer(), %{binary() => %Redis.Value{}}}

    case decode_subsection(rest) do
      {:ok, {db_index, entries = %{}}, rest} ->
        # Update the list of databases based on the index key
        new_acc = replace_at_padded(acc, db_index, entries, %{})
        # Recurse to decode the next database subsection.
        decode(rest, new_acc)

      error_tuple ->
        error_tuple
    end
  end

  # Base case, no more database subsections to decode.
  def decode(data, acc) do
    {:ok, acc, data}
  end

  # For the given list, replace the element at the given index with the given element, padding any elements if the index is greater than the current length.
  @spec replace_at_padded(list(t()), non_neg_integer(), t(), term()) :: list(term())
  defp replace_at_padded(list, index, element, padding) do
    padding_needed = max(0, index - length(list) + 1)
    padded_list = list ++ List.duplicate(padding, padding_needed)
    List.replace_at(padded_list, index, element)
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
