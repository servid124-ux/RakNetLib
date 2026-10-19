package raknetlib.packet;

import raknetlib.RakNet;


/** Server -> client: the server is full. */
public final class NoFreeIncomingConnections extends ServerGuidPacket {
    public static final int ID = RakNet.NO_FREE_INCOMING_CONNECTIONS;

    public NoFreeIncomingConnections() { }

    public NoFreeIncomingConnections(long serverId) { this.serverId = serverId; }

    @Override public int id() { return ID; }

    public static NoFreeIncomingConnections from(byte[] data) { return parse(new NoFreeIncomingConnections(), data); }
}
