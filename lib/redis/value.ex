defmodule Redis.Value do
  @moduledoc """
  This module defines a "value" that is stored in a KeyValueStore.
  """
  @enforce_keys [:data, :type]
  @type t :: %__MODULE__{
          # TODO figure out the actual type here
          data: any(),
          type: atom(),
          expiry_timestamp_epoch_ms: integer() | nil
        }
  defstruct [:data, :type, expiry_timestamp_epoch_ms: nil]

  @spec init(any(), integer() | nil) :: %__MODULE__{}
  def init(data, expiry_timestamp_epoch_ms \\ nil) do
    %__MODULE__{
      data: data,
      type: type_of(data),
      expiry_timestamp_epoch_ms: expiry_timestamp_epoch_ms
    }
  end

  @spec type_of(any()) :: atom()
  def type_of(data) do
    # TODO define all the types here
    case data do
      nil -> :none
      _ -> :string
    end
  end
end
