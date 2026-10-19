package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;


/** Inside a connection (both directions): latency probe. */
public final class ConnectedPing extends Packet {
    public static final int ID = RakNet.PING;

    public long time;

    public ConnectedPing() { }

    public ConnectedPing(long time) { this.time = time; }

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) { o.putLong(time); }

    @Override protected void read(BinaryStream in) { time = in.getLong(); }

    public static ConnectedPing from(byte[] data) { return parse(new ConnectedPing(), data); }
}
