defmodule Redis.Value do
  @moduledoc """
  This module defines a "value" that is stored in a KeyValueStore.
  """
  @type value_type() :: :stream | :string | :unknown

  @enforce_keys [:data, :type]
  @type t :: %__MODULE__{
          data: binary() | Redis.Stream.t(),
          type: value_type(),
          expiry_timestamp_epoch_ms: integer() | nil
        }
  defstruct [:data, :type, expiry_timestamp_epoch_ms: nil]

  @spec init(binary() | Redis.Stream.t(), integer() | nil) :: t()
  def init(data, expiry_timestamp_epoch_ms \\ nil) do
    %__MODULE__{
      data: data,
      type: type_of(data),
      expiry_timestamp_epoch_ms: expiry_timestamp_epoch_ms
    }
  end

  @spec type_of(binary() | Redis.Stream.t()) :: value_type()
  def type_of(data) do
    case data do
      %Redis.Stream{} -> :stream
      d when is_binary(d) -> :string
      _ -> :unknown
    end
  end
end
