defmodule Redis.RESP do
  @moduledoc """
  This RESP module is where all the logic for encoding and decoding the Redis protocol messages lives. See http://redis.io/topics/protocol for details.

  Largely inspired by the implementation in the "Network Programming in Elixir and Erlang" book published by The Pragmatic Bookshelf.

  Examples:
  - "*1\r\n+PING\r\n" is decoded to ["PING"]
  - "*1\r\n$4\r\nPING\r\n" is also decoded to ["PING"]
  - ["ECHO", "hi there"] is encoded as "*2\r\n$4\r\nECHO\r\n$8\r\nhi there\r\n".
  - "PING" is encoded as "+PING\r\n".
  """
  @crlf "\r\n"
  def crlf, do: @crlf
  @crlf_iodata [?\r, ?\n]

  @doc ~S"""
    Encodes a list of Elixir terms as an iodata of a Redis (RESP) Array of Bulk Strings.
    Alternatively, if a single element is provided, the output will be encoded as a simple string.

    ## Examples

        iex> iodata = Redis.RESP.encode(["SET", "mykey", "1"], :array)
        iex> IO.iodata_to_binary(iodata)
        "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$1\r\n1\r\n"

        iex> iodata = Redis.RESP.encode("PONG", :simple_string)
        iex> IO.iodata_to_binary(iodata)
        "+PONG\r\n"

        iex> iodata = Redis.RESP.encode("PING", :bulk_string)
        iex> IO.iodata_to_binary(iodata)
        "$4\r\nPING\r\n"

        iex> IO.iodata_to_binary(Redis.RESP.encode("", :bulk_string))
        "$-1\r\n"

        iex> IO.iodata_to_binary(Redis.RESP.encode("-999", :integer))
        ":-999\r\n"

        iex> IO.iodata_to_binary(Redis.RESP.encode("42", :integer))
        ":42\r\n"

        iex> assert_raise ArgumentError, fn -> IO.iodata_to_binary(Redis.RESP.encode("foobar", :integer)) end
  """
  @spec encode([String.Chars.t()] | String.Chars.t(), atom()) :: iodata
  def encode(input, encoding_type)

  def encode(input, :integer) when is_binary(input),
    do: [?:, Integer.to_string(String.to_integer(input)), @crlf_iodata]

  def encode(input, :simple_string) when is_binary(input), do: [?+, input, @crlf_iodata]

  def encode("", :bulk_string), do: [?$, "-1", @crlf_iodata]

  def encode(text, :bulk_string),
    do: [?$, Integer.to_string(byte_size(text)), @crlf_iodata, text, @crlf_iodata]

  # For encoding an array, use recursion.
  def encode(elems, :array) when is_list(elems), do: encode(elems, [], 0)
  # General case for recursion.
  defp encode([first | rest], accumulator, count_so_far) do
    new_accumulator = [accumulator, encode(first, :bulk_string)]
    encode(rest, new_accumulator, count_so_far + 1)
  end

  # Base case for recursion (we've gone through all the elements in the list).
  defp encode([], accumulator, count_so_far),
    do: [?*, Integer.to_string(count_so_far), @crlf_iodata, accumulator]

  # TODO probably should make this a macro.
  @spec make_simple_string(String.Chars.t()) :: String.Chars.t()
  def make_simple_string(input), do: IO.iodata_to_binary(encode(input, :simple_string))
  @spec make_bulk_string(String.Chars.t()) :: String.Chars.t()
  def make_bulk_string(input), do: IO.iodata_to_binary(encode(input, :bulk_string))
  @spec make_array([String.Chars.t()]) :: String.Chars.t()
  def make_array(input), do: IO.iodata_to_binary(encode(input, :array))

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
  @spec decode(binary()) :: {:ok, list(binary()) | binary(), binary()} | :error
  def decode(input) when is_binary(input), do: decode_impl(input)
  defp decode_impl("*" <> rest), do: decode_array(rest)
  defp decode_impl("+" <> rest), do: decode_simple_string(rest)
  defp decode_impl("$" <> rest), do: decode_bulk_string(rest)

  defp decode_array(input) do
    # First, grab the array length.
    case decode_positive_integer(input) do
      {:ok, count, rest} -> decode_array_impl(rest, count)
      :error -> :error
    end
  end

  defp decode_array_impl(data, count_remaining, accumulator \\ [])

  defp decode_array_impl(rest, 0, accumulator), do: {:ok, accumulator, rest}

  defp decode_array_impl(<<?$, data::binary>>, count_remaining, accumulator) do
    {:ok, this_elem, rest} = decode_bulk_string(data)
    decode_array_impl(rest, count_remaining - 1, accumulator ++ [this_elem])
  end

  defp decode_array_impl(_rest, _count_remaining, _accumulator), do: :error

  # Recursively call decode_integer, skimming off the leftmost digit each time and accumulating all of them until we have the final number after hitting a crlf.
  def decode_positive_integer(data, accumulator \\ 0)

  def decode_positive_integer(<<@crlf, rest::binary>>, accumulator), do: {:ok, accumulator, rest}

  def decode_positive_integer(<<digit, rest::binary>>, accumulator) when digit in ?0..?9,
    do: decode_positive_integer(rest, accumulator * 10 + (digit - ?0))

  def decode_positive_integer(_data, _accumulator), do: :error

  def decode_integer(data, accumulator \\ 0)

  # If the integer starts with a negative sign, then decode the rest as a positive and slap the negative on in the end.
  def decode_integer(<<?-, rest::binary>>, accumulator) do
    {:ok, accumulator, rest} = decode_positive_integer(rest, accumulator)
    {:ok, -accumulator, rest}
  end

  # Otherwise, decode it as a positive integer normally.
  def decode_integer(data, accumulator), do: decode_positive_integer(data, accumulator)

  # To decode a simple string, just keep grabbing bytes until you run into crlf.
  defp decode_simple_string(input), do: until_crlf(input)

  # Return all the bytes until the crlf.
  defp until_crlf(input, accumulator \\ "")

  defp until_crlf(<<@crlf, rest::binary>>, accumulator), do: {:ok, accumulator, rest}

  defp until_crlf(<<byte, rest::binary>>, accumulator) do
    until_crlf(rest, <<accumulator::binary, byte>>)
  end

  defp until_crlf(<<>>, _accumulator), do: :error

  defp decode_bulk_string(input) do
    case decode_positive_integer(input) do
      # TODO use the actual length when parsing.
      {:ok, _length, rest} ->
        until_crlf(rest)

      :error ->
        :error
    end
  end
end
