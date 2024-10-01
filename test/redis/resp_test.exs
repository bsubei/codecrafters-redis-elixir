defmodule Redis.RESPTest do
  use ExUnit.Case, async: true
  alias Redis.RESP
  doctest Redis.RESP

  describe "roundtrip encode and decode" do
    test "roundtrip PING simple string" do
      assert RESP.decode(IO.iodata_to_binary(RESP.encode_simple_string("PING"))) == "PING"
    end

    test "roundtrip PING bulk string" do
      assert RESP.decode(IO.iodata_to_binary(RESP.encode_bulk_string("PING"))) == "PING"
    end

    test "roundtrip array of bulk strings" do
      assert RESP.decode(IO.iodata_to_binary(RESP.encode(["GET", "THE_KEY"]))) == [
               "GET",
               "THE_KEY"
             ]
    end
  end

  describe "decode/1" do
    # test "can decode ping" do
    #   input = "*1\r\n+PING\r\n"
    #   {:ok, message} = RESP.decode(input)
    #   assert message == "PING"

    #   input = binary_part(input, 0, byte_size(input) - byte_size(RESP.crlf()))
    #   assert RESP.decode(input) == :error
    # end

    # test "can decode broadcast messages" do
    #   binary = <<0x02, 3::16, "meg", 2::16, "hi", "rest">>
    #   assert {:ok, message, rest} = Chat.Protocol.decode_message(binary)
    #   assert message == %Broadcast{from_username: "meg", contents: "hi"}
    #   assert rest == "rest"

    #   assert Chat.Protocol.decode_message(<<0x02, 0x00>>) == :incomplete
    # end

    # test "returns :incomplete for empty data" do
    #   assert Chat.Protocol.decode_message("") == :incomplete
    # end

    # test "returns :error for unknown message types" do
    #   assert Chat.Protocol.decode_message(<<0x03, "rest">>) == :error
    # end
  end

  # describe "encode_message/1" do
  #   test "can encode Register messages" do
  #     message = %Register{username: "meg"}
  #     iodata = Chat.Protocol.encode_message(message)

  #     assert IO.iodata_to_binary(iodata) == <<0x01, 0x00, 0x03, "meg">>
  #   end
  # end
end
