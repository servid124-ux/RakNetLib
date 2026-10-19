package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;


/** Server -> client: answer to {@link UnconnectedPing}, carries the MOTD. */
public final class UnconnectedPong extends Packet {
    public static final int ID = RakNet.UNCONNECTED_PONG;

    public long pingId;
    public long serverId;
    public String motd = "";

    public UnconnectedPong() { }

    public UnconnectedPong(long pingId, long serverId, String motd) {
        this.pingId = pingId; this.serverId = serverId; this.motd = motd;
    }

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) { o.putLong(pingId).putLong(serverId); writeMagic(o); o.putString(motd); }

    @Override protected void read(BinaryStream in) {
        pingId = in.getLong();
        serverId = in.getLong();
        readMagic(in);
        motd = in.getString();
    }

    public static UnconnectedPong from(byte[] data) { return parse(new UnconnectedPong(), data); }
}
