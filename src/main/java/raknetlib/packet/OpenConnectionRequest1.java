package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;


/**
 * Client -> server, first step of the handshake.
 * The packet is padded with zeros so that, with the 28 bytes of IP+UDP headers, it is as big as the MTU the client
 * wants to test. {@link #mtu} is that MTU (datagram length + 28), as in RakNet.
 */
public final class OpenConnectionRequest1 extends Packet {
    public static final int ID = RakNet.OPEN_CONNECTION_REQUEST_1;

    public int protocol = RakNet.PROTOCOL;
    public int mtu;

    public OpenConnectionRequest1() { }

    public OpenConnectionRequest1(int protocol, int mtu) { this.protocol = protocol; this.mtu = mtu; }

    @Override public int id() { return ID; }

    @Override protected void write(BinaryStream o) {
        writeMagic(o);
        o.putByte(protocol);
        int pad = mtu - RakNet.UDP_HEADER - 18;   // id + magic + protocol
        if (pad > 0) o.putBytes(new byte[pad]);
    }

    @Override protected void read(BinaryStream in) {
        mtu = in.length() + RakNet.UDP_HEADER;
        readMagic(in);
        protocol = in.getByte();
    }

    public static OpenConnectionRequest1 from(byte[] data) { return parse(new OpenConnectionRequest1(), data); }
}
