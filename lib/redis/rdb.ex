defmodule Redis.RDB do
  alias Redis.KeyValueStore

  @type error_t() :: {:error, list(atom())}

  @spec get_empty_rdb() :: binary()
  def get_empty_rdb() do
    hardcoded_rdb_in_hex =
      "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

    Base.decode16!(hardcoded_rdb_in_hex, case: :lower)
  end

  @spec decode_rdb_file(binary()) :: {:ok, list(KeyValueStore.data_t()), binary()} | error_t()
  def decode_rdb_file(filepath) do
    case File.open(filepath, [:read, :binary]) do
      {:ok, file} ->
        case IO.binread(file, :eof) do
          {:error, reason} -> {:error, [:decode_rdb_file_binread] ++ [reason]}
          rdb_data -> decode_rdb(IO.iodata_to_binary(rdb_data))
        end

      {:error, reason} ->
        {:error, [:decode_rdb_file_open] ++ [reason]}
    end
  end

  @spec decode_rdb(binary()) :: {:ok, list(KeyValueStore.data_t()), binary()} | error_t()
  def decode_rdb(rdb_data) do
    # TODO there must be a cleaner way to express this "pipeline" of data transformations.
    case __MODULE__.Header.decode(rdb_data) do
      {:ok, _header, rest} ->
        case __MODULE__.Metadata.decode(rest) do
          {:ok, %{} = _metadata, rest} ->
            case __MODULE__.Database.decode(rest) do
              {:ok, database_sections, rest} ->
                case __MODULE__.EndOfFile.decode(rest, rdb_data) do
                  {:ok, rest} -> {:ok, database_sections, rest}
                  {:error, reasons} -> {:error, [:decode_rdb_eof_decode | reasons]}
                end

              {:error, reasons} ->
                {:error, [:decode_rdb_database_decode | reasons]}
            end

          {:error, reasons} ->
            {:error, [:decode_rdb_metadata_decode | reasons]}
        end

      {:error, reasons} ->
        {:error, [:decode_rdb_header_decode | reasons]}
    end
  end

  @spec decode_string(binary()) :: {:ok, binary(), binary()} | error_t()

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
    {:error, [:decode_string_unimplemented_lzf_string]}
  end

  def decode_string(<<0b11::2, _rest::binary>>) do
    {:error, [:decode_string_invalid_string_encoding]}
  end

  def decode_string("") do
    {:error, [:decode_string_nothing_to_decode]}
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

  @spec decode_int(binary()) :: {:ok, non_neg_integer(), binary()} | error_t()
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
    {:error, [:decode_int_invalid_length_encoded_int]}
  end
end

defmodule Redis.RDB.Header do
  alias Redis.RDB
  @type t :: %__MODULE__{version: non_neg_integer()}
  defstruct [:version]

  @magic_string "REDIS"
  @min_supported_version 9

  @spec init(non_neg_integer()) :: {:ok, t()} | RDB.error_t()
  defp init(version) do
    if(version < @min_supported_version,
      do: {:error, [:header_init_version_too_old]},
      else: {:ok, %__MODULE__{version: version}}
    )
  end

  @spec decode(binary()) :: {:ok, t(), binary()} | RDB.error_t()

  def decode(<<@magic_string::binary, version::binary-size(4), rest::binary>>) do
    # Look for the magic string, then extract the version (an integer as 4 ASCII bytes). Return the header version and the rest of the data.

    case init(String.to_integer(version)) do
      {:ok, header} -> {:ok, header, rest}
      {:error, reasons} -> {:error, [:header_decode_init | reasons]}
    end
  end

  def decode(_data) do
    {:error, [:header_decode_missing_header_byte]}
  end
end

# TODO it's enough to hardcode these fields, not use a general map. Ignore unknown fields.
# redis-ver: The Redis Version that wrote the RDB
# redis-bits: Bit architecture of the system that wrote the RDB, either 32 or 64
# ctime: Creation time of the RDB
# used-mem: Used memory of the instance that wrote the RDB

defmodule Redis.RDB.Metadata do
  alias Redis.RDB
  @type t :: %{binary() => binary()}
  @metadata_start 0xFA

  @spec decode(binary(), t()) :: {:ok, t(), binary()} | RDB.error_t()
  def decode(input, acc \\ %{})

  # Decodes zero or more metadata sections.
  def decode(<<@metadata_start, rest::binary>>, acc) do
    case decode_metadata_pair(rest) do
      {:ok, key, value, new_rest} ->
        new_acc = Map.merge(acc, %{key => value})
        decode(new_rest, new_acc)

      {:error, reasons} ->
        {:error, [:metadata_decode_pair | reasons]}
    end
  end

  # If on subsequent recursive calls we find no metadata start byte, then we've decoded enough.
  def decode(data, acc) do
    {:ok, acc, data}
  end

  @spec decode_metadata_pair(binary()) :: {:ok, binary(), binary(), binary()} | RDB.error_t()
  defp decode_metadata_pair(data) do
    case RDB.decode_string(data) do
      {:ok, key, rest} ->
        case RDB.decode_string(rest) do
          {:ok, value, rest} ->
            {:ok, key, value, rest}

          {:error, reasons} ->
            {:error, [:decode_pair_bad_metadata_value | reasons]}
        end

      {:error, reasons} ->
        {:error, [:decode_pair_bad_metadata_key | reasons]}
    end
  end
end

defmodule Redis.RDB.Database do
  alias Redis.RDB
  # Each database subsection gets parsed as a KeyValueStore.
  @type t :: Redis.KeyValueStore.data_t()
  @database_start 0xFE
  @resizedb_start 0xFB
  @expiry_s_entry_start 0xFD
  @expiry_ms_entry_start 0xFC

  @spec decode(binary(), list(t())) :: {:ok, list(t()), binary()} | RDB.error_t()
  def decode(data, acc \\ [])

  # There are three pieces of information we can extract from a database subsection:
  # 1. this database subsection's index
  # 2. the hash table size (think of it as the database header)
  # 3. and each key-value entry for this database subsection
  # This function decodes zero or more database subsections, and returns them as an ordered list of Maps (each Map is a "database").
  def decode(<<@database_start, rest::binary>>, acc) do
    case decode_subsection(rest) do
      {:ok, {db_index, entries = %{}}, rest} ->
        # Update the list of databases based on the index key. This effectively re-orders the returned list based on the db index.
        new_acc = replace_at_padded(acc, db_index, entries, %{})
        # Recurse to decode the next database subsection.
        decode(rest, new_acc)

      {:error, reasons} ->
        {:error, [:database_decode_decode_subsection | reasons]}
    end
  end

  # Base case, no more database subsections to decode.
  def decode(data, acc) do
    {:ok, acc, data}
  end

  @spec decode_subsection(binary()) :: {:ok, {non_neg_integer(), t()}, binary()} | RDB.error_t()
  defp decode_subsection(<<db_index::8, rest::binary>>) do
    # Grab the index from the database selector, i.e. the index of the database we're about to decode.

    # Grab the resizedb section, which is only used for validation in our case since we won't bother with pre-allocating the database hashmap.
    case decode_resizedb(rest) do
      {:ok, {num_total_entries, num_expiry_entries}, rest} ->
        case decode_entries(rest, num_total_entries) do
          {:ok, entries = %{}, rest} ->
            # validate total entries and expiry entries
            case validate_entries(entries, num_total_entries, num_expiry_entries) do
              :ok ->
                {:ok, {db_index, entries}, rest}

              {:error, reasons} ->
                {:error, [:decode_subsection_validate_entries | reasons]}
            end

          {:error, reasons} ->
            {:error, [:decode_subsection_decode_entries | reasons]}
        end

      {:error, reasons} ->
        {:error, [:decode_subsection_decode_resize_db | reasons]}
    end
  end

  defp decode_subsection(_data) do
    {:error, [:decode_subsection_missing_db_index]}
  end

  @spec decode_resizedb(binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}, binary()} | RDB.error_t()
  defp decode_resizedb(<<@resizedb_start, rest::binary>>) do
    # The resizedb section simply has two length-encoded integers.
    case RDB.decode_int(rest) do
      {:ok, num_total_entries, rest} ->
        case RDB.decode_int(rest) do
          {:ok, num_expiry_entries, rest} ->
            {:ok, {num_total_entries, num_expiry_entries}, rest}

          {:error, reasons} ->
            {:error, [:decode_resize_db_invalid_num_expiry_entries | reasons]}
        end

      {:error, reasons} ->
        {:error, [:decode_resize_db_invalid_num_total_entries | reasons]}
    end
  end

  defp decode_resizedb(_data), do: {:error, [:invalid_resizedb]}

  @spec decode_entries(binary(), non_neg_integer(), t()) :: {:ok, t(), binary()} | RDB.error_t()
  defp decode_entries(data, num_remaining, acc \\ %{})

  defp decode_entries(data, 0, acc), do: {:ok, acc, data}

  defp decode_entries("", _num_remaining, _acc) do
    {:error, [:decode_entries_missing_eof]}
  end

  defp decode_entries(
         <<@expiry_s_entry_start, expiry_s::integer-32-little, rest::binary>>,
         num_remaining,
         acc
       ) do
    expiry_ms = expiry_s * 1000
    decode_entries_impl(rest, num_remaining, acc, expiry_ms)
  end

  defp decode_entries(
         <<@expiry_ms_entry_start, expiry_ms::integer-64-little, rest::binary>>,
         num_remaining,
         acc
       ) do
    decode_entries_impl(rest, num_remaining, acc, expiry_ms)
  end

  defp decode_entries(data, num_remaining, acc) do
    decode_entries_impl(data, num_remaining, acc)
  end

  @spec decode_entries_impl(binary(), non_neg_integer(), t(), non_neg_integer() | nil) ::
          {:ok, t(), binary()} | RDB.error_t()
  defp decode_entries_impl(data, num_remaining, acc, expiry_ms \\ nil)

  defp decode_entries_impl(<<0::8, rest::binary>>, num_remaining, acc, expiry_ms) do
    # Read key
    case RDB.decode_string(rest) do
      {:ok, key, rest} ->
        # Read value
        case RDB.decode_string(rest) do
          {:ok, value, rest} ->
            # Put them together with the expiry into the acc and recurse.
            value = Redis.Value.init(value, expiry_ms)
            new_acc = Map.merge(acc, %{key => value})
            decode_entries(rest, num_remaining - 1, new_acc)

          {:error, reasons} ->
            {:error, [:decode_entries_impl_invalid_entry_value | reasons]}
        end

      {:error, reasons} ->
        {:error, [:decode_entries_impl_invalid_entry_key | reasons]}
    end
  end

  # TODO we only support string types for now.
  defp decode_entries_impl(<<_value_type::8, _rest::binary>>, _num_remaining, _acc, _expiry_ms),
    do: {:error, [:decode_entries_impl_unimplemented_value_type]}

  @spec validate_entries(t(), non_neg_integer(), non_neg_integer()) :: :ok | RDB.error_t()
  defp validate_entries(entries, num_total, num_expiry) do
    length = map_size(entries)

    length_w_expiry =
      entries |> Enum.count(fn {_key, value} -> value.expiry_timestamp_epoch_ms != nil end)

    cond do
      num_expiry > num_total -> {:error, [:validate_entries_num_expiry_more_than_total]}
      length != num_total -> {:error, [:validate_entries_bad_num_total]}
      length_w_expiry != num_expiry -> {:error, [:validate_entries_bad_num_expiry]}
      true -> :ok
    end
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
  alias Redis.RDB
  @eof_start 0xFF

  @spec decode(binary(), binary()) :: {:ok, binary()} | RDB.error_t()

  def decode(<<@eof_start, crc::integer-64-little, rest::binary>>, all_data) do
    case crc_check(all_data, crc) do
      :ok -> {:ok, rest}
      {:error, reasons} -> {:error, [:eof_decode_crc_check | reasons]}
    end
  end

  def decode(_data, _all_data) do
    {:error, [:eof_decode_missing_eof_byte]}
  end

  @spec crc_check(binary(), non_neg_integer()) :: :ok | RDB.error_t()
  def crc_check(_all_data, _crc) do
    # TODO actually implement CRC checksum
    checksum_result = true

    if checksum_result == true do
      :ok
    else
      {:error, [:crc_check_failed_checksum]}
    end
  end
end
