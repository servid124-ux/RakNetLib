package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;


/** Server -> client: answer to {@link OpenConnectionRequest1} with the MTU the server accepts. */
public final class OpenConnectionReply1 extends Packet {
    public static final int ID = RakNet.OPEN_CONNECTION_REPLY_1;

    public long serverId;
    public boolean security;
    public int mtu;

    public OpenConnectionReply1() { }

    public OpenConnectionReply1(long serverId, int mtu) { this.serverId = serverId; this.mtu = mtu; }

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) {
        writeMagic(o);
        o.putLong(serverId).putByte(security ? 1 : 0).putShort(mtu);
    }

    @Override protected void read(BinaryStream in) {
        readMagic(in);
        serverId = in.getLong();
        security = in.getByte() != 0;
        mtu = in.getShort() & 0xffff;
    }

    public static OpenConnectionReply1 from(byte[] data) { return parse(new OpenConnectionReply1(), data); }
}
