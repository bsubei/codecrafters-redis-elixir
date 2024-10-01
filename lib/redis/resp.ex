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

  Returns `{:ok, value}` if a value is decoded successfully, or :error otherwise.

  ## Examples

      iex> Redis.RESP.decode("+OK\r\n")
      {:ok, "OK", ""}

      iex> Redis.RESP.decode("+OK\r\ncruft")
      :error

      iex> Redis.RESP.decode(":42\r\n")
      {:ok, 42}

      iex> Redis.RESP.decode("$3\r\n\0hi\r\n")
      {:ok, <<0, "hi">>}


  """

  def decode(input) when is_binary(input), do: :unimplemented
end
