defmodule Redis.ConnectionAcceptor do
  @moduledoc """
  This ConnectionAcceptor module defines the process that listens for incoming connections on a socket and creates
  a Connection process to handle each accepted incoming connection. In short, this is the part of the Redis
  server that listens for incoming clients and kicks off processes to handle each client.
  """

  use GenServer
  require Logger

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  @impl true
  def init(_) do
    port = Redis.ServerState.get_state().server_config.port

    listen_options = [
      :binary,
      active: true,
      exit_on_close: false,
      reuseaddr: true,
      backlog: 25
    ]

    # TODO consider changing this to a :once socket that gets reactivated after handling each incoming connection.
    # Create an "active" listening socket (i.e. bind to that port) and start attempting to accept incoming connections.
    case :gen_tcp.listen(port, listen_options) do
      {:ok, listen_socket} ->
        Logger.info("Started Redis server on port #{port}")
        # Send an :accept message so we start accepting connections once we exit this init().
        send(self(), :accept)
        {:ok, listen_socket}

      {:error, reason} ->
        # Abort, we can't start Redis if we can't listen to that port.
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:accept, listen_socket) do
    # Attempt to accept incoming connections.
    case :gen_tcp.accept(listen_socket, 2_000) do
      {:ok, socket} ->
        # Create a Connection GenServer and hand over our active connection from the
        # controlling process to it.
        {:ok, pid} = Redis.Connection.start_link(%{socket: socket})
        :ok = :gen_tcp.controlling_process(socket, pid)
        # Remember to keep accepting more connections.
        send(self(), :accept)
        {:noreply, listen_socket}

      {:error, :timeout} ->
        # Just try again if we time out.
        send(self(), :accept)
        {:noreply, listen_socket}

      {:error, reason} ->
        # Shut down the server otherwise.
        {:stop, reason, listen_socket}
    end
  end
end
