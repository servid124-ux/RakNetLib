package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;


/** Inside a connection (both directions): answer to {@link ConnectedPing}. */
public final class ConnectedPong extends Packet {
    public static final int ID = RakNet.PONG;

    /** The time the ping carried, echoed back. */
    public long pingTime;
    /** The clock of whoever answers. */
    public long pongTime;

    public ConnectedPong() { }

    public ConnectedPong(long pingTime, long pongTime) { this.pingTime = pingTime; this.pongTime = pongTime; }

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) { o.putLong(pingTime).putLong(pongTime); }

    @Override protected void read(BinaryStream in) {
        pingTime = in.getLong();
        pongTime = in.remaining() >= 8 ? in.getLong() : 0;
    }

    public static ConnectedPong from(byte[] data) { return parse(new ConnectedPong(), data); }
}
