package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;


/** Either side: "I am closing this connection". No body. */
public final class DisconnectNotification extends Packet {
    public static final int ID = RakNet.CLIENT_DISCONNECT;

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) { }

    @Override protected void read(BinaryStream in) { }

    public static DisconnectNotification from(byte[] data) { return parse(new DisconnectNotification(), data); }
}
