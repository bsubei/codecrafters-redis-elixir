defmodule Redis.RDBTest do
  use ExUnit.Case
  alias Redis.{RDB, KeyValueStore, Value}
  doctest RDB

  describe "decode RDB data" do
    setup do
      # NOTE: because there is only one KeyValueStore in our application, we should reset it to the initial state after every test clause.
      on_exit(fn -> KeyValueStore.clear() end)
    end

    test "given invalid data should error out" do
      {:error, :invalid_header_section} = RDB.decode_rdb("")

      {:error, :invalid_header_section} =
        RDB.decode_rdb("no way will thiswork because, this is just random text")

      rdb_truncated_after_metadata_section =
        Base.decode16!(
          "524544495330303039fa0972656469732d76657205352e302e37fa0a72656469732d62697473c040fa056374696d65c2b8764367fa08757365642d6d656dc2f8260c00fa0c616f662d707265616d626c65c000",
          case: :lower
        )

      {:error, :invalid_eof_section} = RDB.decode_rdb(rdb_truncated_after_metadata_section)
    end

    # test "given a valid empty RDB file" do
    #   {:ok, %{}} = RDB.decode_rdb(RDB.get_empty_rdb())
    # end

    # test "given a valid non-empty RDB file" do
    #   # This string is a base16 encoded RDB file, and only has a single key-value pair: "hi" and "bye". It was generated by running:
    #   # xxd -p dump.rdb | tr -d '\n'
    #   base16_rdb =
    #     "524544495330303039fa0972656469732d76657205352e302e37fa0a72656469732d62697473c040fa056374696d65c2b8764367fa08757365642d6d656dc2f8260c00fa0c616f662d707265616d626c65c000fe00fb01000002686903627965ff11a65bf65e57de23"

    #   nonempty_rdb = Base.decode16!(base16_rdb, case: :lower)
    #   expected_kvstore = %{"hi" => Value.init("bye", nil)}
    #   {:ok, ^expected_kvstore} = RDB.decode_rdb(nonempty_rdb)
    # end
  end

  describe "decoding string encoded values" do
    test "for length prefixed strings" do
      # The length of the string is stored in the 6 LSBs.
      {:ok, "ABCDE", "unused"} = RDB.decode_string(<<0b00::2, 5::6, "ABCDEunused">>)

      # The length of the string is stored in the next byte along with the 6 LSBs.
      {:ok, "0123456789", "unused"} = RDB.decode_string(<<0b01::2, 10::14, "0123456789unused">>)

      # The length of the string is stored in the next 4 bytes. Use a big string with more than 2^16 bytes.
      expected_length = 16384

      expected_string =
        Range.new(0, expected_length - 1)
        |> Enum.map(fn i -> rem(i, 10) end)
        |> Enum.reduce("", fn i, acc -> acc <> Integer.to_string(i) end)

      {:ok, ^expected_string, "unused"} =
        RDB.decode_string(
          <<0b10::2, 0::6, expected_length::integer-32-little, expected_string::binary, "unused">>
        )
    end

    test "for integers as strings" do
      # 8-bit integer
      input = <<0b11::2, 0::6, 0x40::8, "rest">>
      {:ok, "64", "rest"} = RDB.decode_string(input)
      # 16-bit integer
      input = <<0b11::2, 1::6, 42::integer-16-little, "rest">>
      {:ok, "42", "rest"} = RDB.decode_string(input)
      input = <<0b11::2, 1::6, 15003::integer-16-little, "rest">>
      {:ok, "15003", "rest"} = RDB.decode_string(input)
      # 32-bit integer
      input = <<0b11::2, 2::6, 5_125_120::integer-32-little, "rest">>
      {:ok, "5125120", "rest"} = RDB.decode_string(input)
    end

    test "for LZF compressed strings" do
      input = <<0b11::2, 3::6, "whatever doesn't matter">>
      {:error, :unimplemented_lzf_string} = RDB.decode_string(input)
    end
  end

  describe "decode RDB header" do
    test "given invalid headers" do
      {:error, :invalid_header_section} = RDB.Header.decode([])
      {:error, :invalid_header_section} = RDB.Header.decode(<<"REDDIT">>)
      {:error, :version_too_old} = RDB.Header.decode(<<"REDIS", "0001">>)
    end

    test "given valid headers" do
      rest = "arbitrary text"
      {:ok, %RDB.Header{version: 9}, ""} = RDB.Header.decode("REDIS0009")

      {:ok, %RDB.Header{version: 20}, ^rest} =
        RDB.Header.decode("REDIS0020#{rest}")
    end
  end

  describe "decode RDB Metadata" do
    test "given invalid data" do
      {:error, :bad_metadata_key} = RDB.Metadata.decode(<<0xFA>>)
      {:error, :bad_metadata_value} = RDB.Metadata.decode(<<0xFA, 0x04, "toad">>)
    end

    test "given a single metadata section" do
      expected_metadata_sections = %{"toad" => "chicken"}
      input = <<0xFA, 0x04, "toad", 0x07, "chicken", "unused">>
      {:ok, ^expected_metadata_sections, "unused"} = RDB.Metadata.decode(input)
    end

    test "given a multiple metadata sections" do
      # First section: "redis-ver" and "5.0.7"
      # Second section: "redis-bits" and "64" as an 8-bit integer.
      # Third section: "ctime" and a unix timestamp as a 64 bit integer.
      bytes =
        Base.decode16!("FA0972656469732D76657205352E302E37") <>
          Base.decode16!("FA0A72656469732D62697473C040") <>
          Base.decode16!("FA056374696D65C2B8764367") <>
          "rest"

      expected_metadata_sections = %{
        "redis-ver" => "5.0.7",
        "redis-bits" => "64",
        "ctime" => "1732474552"
      }

      {:ok, ^expected_metadata_sections, "rest"} = RDB.Metadata.decode(bytes)
    end
  end

  describe "decode RDB Database" do
    # test "given invalid data" do
    # {:error, :bad_metadata_key} = RDB.Metadata.decode(<<0xFA>>)
    # {:error, :bad_metadata_value} = RDB.Metadata.decode(<<0xFA, 0x04, "toad">>)
    # end

    test "given a single database section" do
      bytes = Base.decode16!("fe00fb01000002686903627965ff", case: :lower) <> "rest"

      expected_database_sections = [
        %{"hi" => %Redis.Value{data: "bye", type: :string, expiry_timestamp_epoch_ms: nil}}
      ]

      {:ok, ^expected_database_sections, <<0xFF, "rest">>} = RDB.Database.decode(bytes)
    end

    # TODO test multiple database sections with gaps. e.g. 2 databases with db indices 5 and 22 (should have 23 databases in the result, with only two populated)
  end
end