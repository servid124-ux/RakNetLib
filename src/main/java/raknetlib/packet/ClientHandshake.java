package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;

import java.net.InetSocketAddress;

/** Client -> server: last step of the handshake. After this the connection is open. */
public final class ClientHandshake extends Packet {
    public static final int ID = RakNet.CLIENT_HANDSHAKE;

    public InetSocketAddress serverAddress;
    public long pingTime;
    public long pongTime;

    public ClientHandshake() { }

    public ClientHandshake(InetSocketAddress serverAddress, long pingTime, long pongTime) {
        this.serverAddress = serverAddress; this.pingTime = pingTime; this.pongTime = pongTime;
    }

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) {
        o.putAddress(serverAddress.getAddress(), serverAddress.getPort());
        InternalAddresses.write(o);
        o.putLong(pingTime).putLong(pongTime);
    }

    @Override protected void read(BinaryStream in) {
        serverAddress = in.getAddress();
        InternalAddresses.skip(in);
        pingTime = in.getLong();
        pongTime = in.getLong();
    }

    public static ClientHandshake from(byte[] data) { return parse(new ClientHandshake(), data); }
}
