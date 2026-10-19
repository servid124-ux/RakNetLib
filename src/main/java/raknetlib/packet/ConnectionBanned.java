package raknetlib.packet;

import raknetlib.RakNet;


/** Server -> client: this IP is banned. */
public final class ConnectionBanned extends ServerGuidPacket {
    public static final int ID = RakNet.CONNECTION_BANNED;

    public ConnectionBanned() { }

    public ConnectionBanned(long serverId) { this.serverId = serverId; }

    @Override public int id() { return ID; }

    public static ConnectionBanned from(byte[] data) { return parse(new ConnectionBanned(), data); }
}
