package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;


/** Client -> server: "are you there?" (0x01), or the same only to servers with free slots (0x02). */
public final class UnconnectedPing extends Packet {
    public static final int ID = RakNet.UNCONNECTED_PING, ID_OPEN = RakNet.UNCONNECTED_PING_OPEN;

    public long pingId;
    public long clientGuid;
    /** true = 0x02, only answered when the server has free connections. */
    public boolean openConnectionsOnly;

    public UnconnectedPing() { }

    public UnconnectedPing(long pingId, long clientGuid) { this.pingId = pingId; this.clientGuid = clientGuid; }

    @Override public int id() { return openConnectionsOnly ? ID_OPEN : ID; }

    @Override protected boolean accepts(int id) { return id == ID || id == ID_OPEN; }

    @Override protected void onId(int id) { openConnectionsOnly = id == ID_OPEN; }

    @Override protected void write(BinaryStream o) { o.putLong(pingId); writeMagic(o); o.putLong(clientGuid); }

    @Override protected void read(BinaryStream in) {
        pingId = in.getLong();
        readMagic(in);
        clientGuid = in.remaining() >= 8 ? in.getLong() : 0;
    }

    public static UnconnectedPing from(byte[] data) { return parse(new UnconnectedPing(), data); }
}
