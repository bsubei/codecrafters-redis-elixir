defmodule Redis.KeyValueStoreTest do
  # NOTE: because there is only one KeyValueStore in our application, we should run multiple tests asynchronously.
  use ExUnit.Case, async: false
  alias Redis.KeyValueStore

  describe "check basic behavior of the initialized KeyValueStore" do
    setup do
      # NOTE: because there is only one KeyValueStore in our application, we should reset it to the initial state after every test clause.
      on_exit(fn -> KeyValueStore.clear() end)
    end

    test "calling get on an empty KeyValueStore should return nil" do
      assert KeyValueStore.get("whatever") == nil
    end

    test "calling setting and getting on a KeyValueStore works" do
      assert KeyValueStore.get("fortytwo") == nil
      assert KeyValueStore.set("fortytwo", "42") == :ok
      assert KeyValueStore.get("fortytwo") == "42"
    end

    # TODO eventually add tests here that check for expiry, but the integration tests I have in connection_test.exs already cover this so it's fine.
  end
end
