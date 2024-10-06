defmodule Redis.RESPTest do
  use ExUnit.Case, async: true
  alias Redis.RESP
  doctest Redis.RESP

  describe "roundtrip encode and decode" do
    test "roundtrip PING simple string" do
      assert {:ok, "PING", ""} =
               RESP.decode(IO.iodata_to_binary(RESP.encode("PING", :simple_string)))
    end

    test "roundtrip PING bulk string" do
      assert {:ok, "PING", ""} =
               RESP.decode(IO.iodata_to_binary(RESP.encode("PING", :bulk_string)))
    end

    test "roundtrip array of bulk strings" do
      assert {:ok, ["GET", "THE_KEY"], ""} ==
               RESP.decode(IO.iodata_to_binary(RESP.encode(["GET", "THE_KEY"], :array)))
    end
  end
end
