defmodule Redis.RESP do
  @moduledoc """
  This RESP module is where all the logic for encoding and decoding the Redis protocol messages lives. See http://redis.io/topics/protocol for details.

  Largely inspired by the implementation in the "Network Programming in Elixir and Erlang" book published by The Pragmatic Bookshelf.

  Examples:
  - "*1\r\n+PING\r\n" is decoded to ["PING"]
  - "*1\r\n$4\r\nPING\r\n" is also decoded to ["PING"]
  - ["ECHO", "hi there"] is encoded as "*2\r\n$4\r\nECHO\r\n$8\r\nhi there\r\n".
  """
  @crlf "\r\n"
  def crlf, do: @crlf
  @crlf_iodata [?\r, ?\n]

  @doc ~S"""
    Encodes a list of Elixir terms as an iodata of a Redis (RESP) Array of Bulk Strings.

    ## Examples

        iex> iodata = Redis.RESP.encode(["SET", "mykey", "1"])
        iex> IO.iodata_to_binary(iodata)
        "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$1\r\n1\r\n"

  """
  @spec encode([String.Chars.t()]) :: iodata
  def encode(elems) when is_list(elems), do: encode(elems, [], 0)
  # General case for recursion.
  defp encode([first | rest], accumulator, count_so_far) do
    new_accumulator = [accumulator, encode_bulk_string(first)]
    encode(rest, new_accumulator, count_so_far + 1)
  end

  # Base case for recursion (we've gone through all the elements in the list).
  defp encode([], accumulator, count_so_far),
    do: [?*, Integer.to_string(count_so_far), @crlf_iodata, accumulator]

  @doc ~S"""
    Encodes the given binary to an iodata containing a RESP bulk string.
    ## Examples
        iex> IO.iodata_to_binary(Redis.RESP.encode_bulk_string("PING"))
        "$4\r\nPING\r\n"
  """
  def encode_bulk_string(""), do: [?$, "0"]

  def encode_bulk_string(text),
    do: [?$, Integer.to_string(byte_size(text)), @crlf_iodata, text, @crlf_iodata]

  @doc ~S"""
    Encodes the given binary to an iodata containing a RESP simple string.
    ## Examples
        iex> IO.iodata_to_binary(Redis.RESP.encode_simple_string("PING"))
        "+PING\r\n"
  """
  def encode_simple_string(text) when is_binary(text), do: [?+, text, @crlf_iodata]

  # @typedoc "Possible Redis values (i.e. the result of decoding RESP messages)."
  # @type redis_value :: binary | integer | nil | %Error{} | [redis_value]

  # @typedoc """
  # The result of decoding a redis value.

  # If the decoding was successful, we return the :ok arm with the decoded value and the remaining undecoded binary (called "rest").
  # If the decoding failed because the data was incomplete, we return a continuation. Otherwise, we raise a ParseError.
  # """
  # @type on_decode(value) :: {:ok, value, binary} | {:continuation, (binary -> on_decode(value))}

  # @spec decode(binary) :: on_decode(redis_value)
  # def decode(input) when is_binary(input), do: :unimplemented

  @doc ~S"""
  Decodes a RESP-encoded value from the given `data`.

  TODO use continuations to support handling incomplete messages.

  Returns `{:ok, value}` if a value is decoded successfully, or :error otherwise.

  ## Examples

      iex> Redis.RESP.decode("+OK\r\n")
      {:ok, "OK", ""}

      iex> Redis.RESP.decode("+nope\r\nignore")
      {:ok, "nope", "ignore"}

      iex> Redis.RESP.decode("$3\r\n\0hi\r\n")
      {:ok, <<0, "hi">>, ""}

      iex> Redis.RESP.decode("*2\r\n$2\r\nOK\r\n$3\r\nbye\r\nleftovercrud")
      {:ok, ["OK", "bye"], "leftovercrud"}


  """

  def decode(input) when is_binary(input), do: decode_impl(input)
  defp decode_impl("*" <> rest), do: decode_array(rest)
  defp decode_impl("+" <> rest), do: decode_simple_string(rest)
  defp decode_impl("$" <> rest), do: decode_bulk_string(rest)

  defp decode_array(input) do
    # First, grab the array length.
    {:ok, count, rest} = decode_positive_integer(input)
    decode_array_impl(rest, count)
  end

  defp decode_array_impl(data, count_remaining, accumulator \\ [])

  defp decode_array_impl(rest, 0, accumulator) do
    {:ok, accumulator, rest}
  end

  defp decode_array_impl(<<?$, data::binary>>, count_remaining, accumulator) do
    {:ok, this_elem, rest} = decode_bulk_string(data)
    decode_array_impl(rest, count_remaining - 1, accumulator ++ [this_elem])
  end

  # Recursively call decode_integer, skimming off the leftmost digit each time and accumulating all of them until we have the final number after hitting a crlf.
  defp decode_positive_integer(data, accumulator \\ 0)

  defp decode_positive_integer(<<@crlf, rest::binary>>, accumulator), do: {:ok, accumulator, rest}

  defp decode_positive_integer(<<digit, rest::binary>>, accumulator) when digit in ?0..?9,
    do: decode_positive_integer(rest, accumulator * 10 + (digit - ?0))

  # To decode a simple string, just keep grabbing bytes until you run into crlf.
  defp decode_simple_string(input), do: until_crlf(input)

  # Return all the bytes until the crlf.
  defp until_crlf(input, accumulator \\ "")

  defp until_crlf(<<@crlf, rest::binary>>, accumulator) do
    {:ok, accumulator, rest}
  end

  defp until_crlf(<<letter, rest::binary>>, accumulator) do
    until_crlf(rest, <<accumulator::binary, letter>>)
  end

  defp decode_bulk_string(input) do
    {:ok, length, rest} = decode_positive_integer(input)

    case length do
      -1 ->
        nil

      _ ->
        until_crlf(rest)
    end
  end
end
