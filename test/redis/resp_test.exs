defmodule Redis.RESPTest do
  use ExUnit.Case, async: true
  alias Redis.RESP
  doctest Redis.RESP

  describe "roundtrip encode and decode" do
    test "roundtrip PING simple string" do
      assert {"PING", ""} = RESP.decode(IO.iodata_to_binary(RESP.encode_simple_string("PING")))
    end

    test "roundtrip PING bulk string" do
      assert {"PING", ""} = RESP.decode(IO.iodata_to_binary(RESP.encode_bulk_string("PING")))
    end

    test "roundtrip array of bulk strings" do
      assert {["GET", "THE_KEY"], ""} ==
               RESP.decode(IO.iodata_to_binary(RESP.encode(["GET", "THE_KEY"])))
    end
  end
end
