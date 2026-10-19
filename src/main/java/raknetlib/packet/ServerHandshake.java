package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;

import java.net.InetSocketAddress;

/** Server -> client: answer to {@link ConnectionRequest}. */
public final class ServerHandshake extends Packet {
    public static final int ID = RakNet.SERVER_HANDSHAKE;

    public InetSocketAddress clientAddress;
    public int systemIndex;
    /** The time the client sent in its {@link ConnectionRequest}. */
    public long requestTime;
    /** The server clock. */
    public long time;

    public ServerHandshake() { }

    public ServerHandshake(InetSocketAddress clientAddress, long requestTime, long time) {
        this.clientAddress = clientAddress; this.requestTime = requestTime; this.time = time;
    }

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) {
        o.putAddress(clientAddress.getAddress(), clientAddress.getPort()).putShort(systemIndex);
        InternalAddresses.write(o);
        o.putLong(requestTime).putLong(time);
    }

    @Override protected void read(BinaryStream in) {
        clientAddress = in.getAddress();
        systemIndex = in.getShort() & 0xffff;
        InternalAddresses.skip(in);
        requestTime = in.getLong();
        time = in.getLong();
    }

    public static ServerHandshake from(byte[] data) { return parse(new ServerHandshake(), data); }
}
