package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;

import java.net.InetSocketAddress;

/** Client -> server, second step of the handshake: MTU and client GUID. */
public final class OpenConnectionRequest2 extends Packet {
    public static final int ID = RakNet.OPEN_CONNECTION_REQUEST_2;

    public InetSocketAddress serverAddress;
    public int mtu;
    public long clientGuid;

    public OpenConnectionRequest2() { }

    public OpenConnectionRequest2(InetSocketAddress serverAddress, int mtu, long clientGuid) {
        this.serverAddress = serverAddress; this.mtu = mtu; this.clientGuid = clientGuid;
    }

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) {
        writeMagic(o);
        o.putAddress(serverAddress.getAddress(), serverAddress.getPort()).putShort(mtu).putLong(clientGuid);
    }

    @Override protected void read(BinaryStream in) {
        readMagic(in);
        serverAddress = in.getAddress();
        mtu = in.getShort() & 0xffff;
        clientGuid = in.getLong();
    }

    public static OpenConnectionRequest2 from(byte[] data) { return parse(new OpenConnectionRequest2(), data); }
}
