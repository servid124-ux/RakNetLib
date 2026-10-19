package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;


/** Server -> client: the RakNet protocol version of the client is not accepted. */
public final class IncompatibleProtocol extends Packet {
    public static final int ID = RakNet.INCOMPATIBLE_PROTOCOL;

    public int protocol = RakNet.PROTOCOL;
    public long serverId;

    public IncompatibleProtocol() { }

    public IncompatibleProtocol(int protocol, long serverId) { this.protocol = protocol; this.serverId = serverId; }

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) { o.putByte(protocol); writeMagic(o); o.putLong(serverId); }

    @Override protected void read(BinaryStream in) { protocol = in.getByte(); readMagic(in); serverId = in.getLong(); }

    public static IncompatibleProtocol from(byte[] data) { return parse(new IncompatibleProtocol(), data); }
}
