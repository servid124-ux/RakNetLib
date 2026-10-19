package raknetlib.packet;

import raknetlib.RakNet;


/** Server -> client: too many connections from this IP. */
public final class IpRecentlyConnected extends ServerGuidPacket {
    public static final int ID = RakNet.IP_RECENTLY_CONNECTED;

    public IpRecentlyConnected() { }

    public IpRecentlyConnected(long serverId) { this.serverId = serverId; }

    @Override public int id() { return ID; }

    public static IpRecentlyConnected from(byte[] data) { return parse(new IpRecentlyConnected(), data); }
}
