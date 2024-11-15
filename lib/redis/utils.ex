defmodule Redis.Utils do
  @doc ~S"""
    Like Enum.unzip, but generalizes to more than just 2-tuples.

    ## Examples

        iex> Redis.Utils.unzip([])
        []
        iex> expected = Enum.unzip([{1, 3}, {2, 4}])
        iex> Redis.Utils.unzip([{1, 3}, {2, 4}]) == expected
        true
        iex> Redis.Utils.unzip([{1, 3, 5}, {2, 4, 6}])
        {[1, 2], [3, 4], [5, 6]}
  """
  @spec unzip(list()) :: tuple()
  def unzip([]), do: []

  def unzip(inputs) do
    n = tuple_size(hd(inputs))
    starting_acc = List.duplicate([], n)

    inputs
    |> Enum.reduce(starting_acc, fn tuple_element, accumulator ->
      Enum.zip(Tuple.to_list(tuple_element), accumulator)
      |> Enum.map(fn {elem, acc} -> acc ++ [elem] end)
    end)
    |> List.to_tuple()
  end
end
