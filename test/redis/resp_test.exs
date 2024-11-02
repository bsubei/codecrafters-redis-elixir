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

    test "decoding nested RESP arrays" do
      # Decoding a nested array of arrays of simple strings.
      assert {:ok, [["0,0", "0,1"], ["1,0", "1,1"]], "unused"} =
               Redis.RESP.decode("*2\r\n*2\r\n+0,0\r\n+0,1\r\n*2\r\n+1,0\r\n+1,1\r\nunused")

      # Decoding the nested array example from the codecrafters (this is the kind of responses our server should generate for XRANGE stream requests).
      input =
        "*2\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n"

      expected_output = [
        ["1526985054069-0", ["temperature", "36", "humidity", "95"]],
        ["1526985054079-0", ["temperature", "37", "humidity", "94"]]
      ]

      assert {:ok, ^expected_output, ""} = Redis.RESP.decode(input)
    end
  end
end
