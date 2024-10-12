defmodule Redis.ConnectionManager do
  @moduledoc """
  This ConnectionManager module defines the process that listens for incoming connections on a socket and creates
  a Connection process to handle each accepted incoming connection. In short, this is the part of the Redis
  server that listens for incoming clients and kicks off processes to handle each client.

  Also, if this server is a replica, this module also defines how it initiates the syncing handshake with the master server.
  """

  use GenServer
  require Logger
  alias Redis.ServerState
  alias Redis.Connection

  @type t :: %__MODULE__{listen_socket: :gen_tcp.socket()}
  defstruct [:listen_socket]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  @impl true
  def init(_) do
    server_state = ServerState.get_state()
    port = server_state.cli_config.port

    # TODO do we need to mark our server state as "not synced yet" so we drop client requests before we're fully synced?
    # Kick off the sync handshake with master since we're a replica and we need to sync.
    case server_state.server_info.replication.role do
      :slave -> send(self(), :initiate_sync_handshake)
      :master -> nil
    end

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
        {:ok, %__MODULE__{listen_socket: listen_socket}}

      {:error, reason} ->
        # Abort, we can't start Redis if we can't listen to that port.
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:accept, %__MODULE__{listen_socket: listen_socket} = state) do
    # Attempt to accept incoming connections.
    case :gen_tcp.accept(listen_socket, 2_000) do
      {:ok, socket} ->
        # Create a Connection GenServer and hand over our active connection from the
        # controlling process to it.
        {:ok, pid} = Connection.start_link(%{socket: socket, handshake_status: :not_started})
        :ok = :gen_tcp.controlling_process(socket, pid)
        # Remember to keep accepting more connections.
        send(self(), :accept)
        {:noreply, state}

      {:error, :timeout} ->
        # Just try again if we time out.
        send(self(), :accept)
        {:noreply, state}

      {:error, reason} ->
        # Shut down the server otherwise.
        {:stop, reason, state}
    end
  end

  @impl true
  def handle_info(:initiate_sync_handshake, %__MODULE__{} = state) do
    master_socket = connect_to_master()

    # Send a PING to master to start the handshake.
    :ok = send_handshake_ping(master_socket)

    # Create a connection to master and hand off the socket to the Connection process (this transfers the reply to our PING to the Connection).
    # Also, mark the handshake status to indicate that we've already started the handshake, so the Connection can handle the reply and continue the rest of the handshake.
    {:ok, pid} = Connection.start_link(%{socket: master_socket, handshake_status: :ping_sent})
    :ok = :gen_tcp.controlling_process(master_socket, pid)

    {:noreply, state}
  end

  defp send_handshake_ping(master_socket) do
    case :gen_tcp.send(master_socket, Redis.RESP.encode(["PING"], :array)) do
      :ok ->
        :ok

      {:error, reason} ->
        IO.puts(
          "Failed to send PING to initiate sync handshake due to reason: #{inspect(reason)}... Retrying!"
        )

        send_handshake_ping(master_socket)
    end
  end

  defp connect_to_master() do
    [master_address, master_port] = ServerState.get_state().cli_config.replicaof |> String.split()

    opts = [
      :binary,
      active: true
    ]

    case :gen_tcp.connect(
           String.to_charlist(master_address),
           String.to_integer(master_port),
           opts,
           2_000
         ) do
      {:ok, master_socket} ->
        master_socket

      {:error, reason} ->
        IO.puts(
          "Failed to connect to master to initiate handshake due to reason: #{inspect(reason)}... Retrying!"
        )

        connect_to_master()
    end
  end
end
