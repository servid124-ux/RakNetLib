package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;

import java.net.InetSocketAddress;

/** Server -> client: handshake accepted, with the final MTU. */
public final class OpenConnectionReply2 extends Packet {
    public static final int ID = RakNet.OPEN_CONNECTION_REPLY_2;

    public long serverId;
    public InetSocketAddress clientAddress;
    public int mtu;
    public boolean encryption;

    public OpenConnectionReply2() { }

    public OpenConnectionReply2(long serverId, InetSocketAddress clientAddress, int mtu) {
        this.serverId = serverId; this.clientAddress = clientAddress; this.mtu = mtu;
    }

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) {
        writeMagic(o);
        o.putLong(serverId).putAddress(clientAddress.getAddress(), clientAddress.getPort())
            .putShort(mtu).putByte(encryption ? 1 : 0);
    }

    @Override protected void read(BinaryStream in) {
        readMagic(in);
        serverId = in.getLong();
        clientAddress = in.getAddress();
        mtu = in.getShort() & 0xffff;
        encryption = in.remaining() > 0 && in.getByte() != 0;
    }

    public static OpenConnectionReply2 from(byte[] data) { return parse(new OpenConnectionReply2(), data); }
}
