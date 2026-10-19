package raknetlib.packet;

import raknetlib.BinaryStream;

import java.net.InetAddress;

/** The block of 10 "internal" addresses that both handshake packets carry. Nobody uses them, but they must be there. */
final class InternalAddresses {
    static final int COUNT = 10;

    private InternalAddresses() { }

    static void write(BinaryStream o) {
        for (int i = 0; i < COUNT; i++) o.putAddress(i == 0 ? InetAddress.getLoopbackAddress() : zero(), 0);
    }

    static void skip(BinaryStream in) {
        for (int i = 0; i < COUNT; i++) in.getAddress();
    }

    private static InetAddress zero() {
        try { return InetAddress.getByAddress(new byte[4]); } catch (Exception e) { throw new IllegalStateException(e); }
    }
}
