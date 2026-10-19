package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;


/** Client -> server, first reliable packet after the offline handshake. */
public final class ConnectionRequest extends Packet {
    public static final int ID = RakNet.CLIENT_CONNECT;

    public long clientGuid;
    public long time;
    public boolean security;

    public ConnectionRequest() { }

    public ConnectionRequest(long clientGuid, long time) { this.clientGuid = clientGuid; this.time = time; }

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) { o.putLong(clientGuid).putLong(time).putByte(security ? 1 : 0); }

    @Override protected void read(BinaryStream in) {
        clientGuid = in.getLong();
        time = in.getLong();
        security = in.remaining() > 0 && in.getByte() != 0;
    }

    public static ConnectionRequest from(byte[] data) { return parse(new ConnectionRequest(), data); }
}
