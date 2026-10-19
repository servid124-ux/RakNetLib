package raknetlib;

import raknetlib.RakNet;

public final class RakNet {
    private RakNet() {}

    public static final int PROTOCOL = 8;
    public static final byte[] MAGIC = {
        0x00, (byte) 0xff, (byte) 0xff, 0x00, (byte) 0xfe, (byte) 0xfe, (byte) 0xfe, (byte) 0xfe,
        (byte) 0xfd, (byte) 0xfd, (byte) 0xfd, (byte) 0xfd, 0x12, 0x34, 0x56, 0x78
    };

    public static final int UNCONNECTED_PING = 0x01;
    public static final int UNCONNECTED_PING_OPEN = 0x02;
    public static final int OPEN_CONNECTION_REQUEST_1 = 0x05;
    public static final int OPEN_CONNECTION_REPLY_1 = 0x06;
    public static final int OPEN_CONNECTION_REQUEST_2 = 0x07;
    public static final int OPEN_CONNECTION_REPLY_2 = 0x08;
    public static final int ALREADY_CONNECTED = 0x12;
    public static final int NO_FREE_INCOMING_CONNECTIONS = 0x14;
    public static final int CONNECTION_BANNED = 0x17;
    public static final int INCOMPATIBLE_PROTOCOL = 0x19;
    public static final int IP_RECENTLY_CONNECTED = 0x1a;
    public static final int UNCONNECTED_PONG = 0x1c;

    public static final int PING = 0x00;
    public static final int PONG = 0x03;
    public static final int CLIENT_CONNECT = 0x09;
    public static final int SERVER_HANDSHAKE = 0x10;
    public static final int CLIENT_HANDSHAKE = 0x13;
    public static final int CLIENT_DISCONNECT = 0x15;

    public static final int ACK = 0xc0;
    public static final int NACK = 0xa0;

    public static final int UNRELIABLE = 0;
    public static final int UNRELIABLE_SEQUENCED = 1;
    public static final int RELIABLE = 2;
    public static final int RELIABLE_ORDERED = 3;
    public static final int RELIABLE_SEQUENCED = 4;
    public static final int UNRELIABLE_WITH_ACK_RECEIPT = 5;
    public static final int RELIABLE_WITH_ACK_RECEIPT = 6;
    public static final int RELIABLE_ORDERED_WITH_ACK_RECEIPT = 7;

    /** IPv4 (20) + UDP (8) header bytes. In RakNet the MTU counts them; the datagram payload is MTU - UDP_HEADER. */
    public static final int UDP_HEADER = 28;
    /** Negotiable MTU range, IP level (RakNet's MINIMUM_MTU_SIZE and the 1464 cap used by MCPE servers). */
    public static final int MIN_MTU = 400, MAX_MTU = 1464;

    public static int clampMtu(int mtu) { return Math.max(MIN_MTU, Math.min(mtu, MAX_MTU)); }

    /** Largest datagram payload (what goes inside the UDP packet) for a negotiated MTU. */
    public static int maxPayload(int mtu) { return mtu - UDP_HEADER; }

    // Datagram header flags (first byte of every connected datagram): 0x80 valid, 0x40 ACK, 0x20 NACK
    // (hasBAndAS in ACKs), 0x10 packet pair, 0x08 continuous send, 0x04 needsBAndAS. Real peers set the low bits freely.
    public static boolean isAckDatagram(int id) { return (id & 0xC0) == 0xC0; }
    public static boolean isNackDatagram(int id) { return (id & 0xE0) == 0xA0; }
    public static boolean isDataDatagram(int id) { return (id & 0xE0) == 0x80; }

    public static final int ORDER_CHANNELS = 32;
    public static final int MASK24 = 0xFFFFFF;

    private static final long ORIGIN = System.nanoTime();

    public static long nowMs() { return (System.nanoTime() - ORIGIN) / 1_000_000L + 1_000_000L; }

    public static int diff24(int a, int b) {
        return (((a - b) + 0x800000) & MASK24) - 0x800000;
    }

    public static int wrap24(int v) { return v & MASK24; }

    public static boolean isReliable(int r) { return r == 2 || r == 3 || r == 4 || r == 6 || r == 7; }

    public static boolean isSequenced(int r) { return r == 1 || r == 4; }

    public static boolean isOrdered(int r) { return r == 1 || r == 3 || r == 4 || r == 7; }

    public static boolean hasReceipt(int r) { return r >= 5 && r <= 7; }

    public static int wireReliability(int r) { return r == 5 ? 0 : r == 6 ? 2 : r == 7 ? 3 : r; }

    public static int upgradeForSplit(int wire) { return wire == 0 ? 2 : wire == 1 ? 4 : wire; }

    public static String describe(Throwable e) {
        String m = e.getMessage();
        String extra = m == null || m.isEmpty() ? "" : " (" + m + ")";
        if (e instanceof IndexOutOfBoundsException || e instanceof java.nio.BufferUnderflowException || e instanceof java.io.EOFException)
            return "truncated packet, missing data";
        if (e instanceof NegativeArraySizeException) return "negative length in a field";
        if (e instanceof ArithmeticException) return "invalid numeric operation" + extra;
        if (e instanceof NullPointerException) return "unexpected null value";
        if (e instanceof ClassCastException) return "unexpected data type";
        if (e instanceof NumberFormatException) return "malformed number" + extra;
        if (e instanceof IllegalArgumentException) return "invalid field" + extra;
        if (e instanceof IllegalStateException) return "invalid state" + extra;
        if (e instanceof UnsupportedOperationException) return "unsupported operation" + extra;
        return "unexpected error " + e.getClass().getSimpleName() + extra;
    }

    public static boolean magicAt(byte[] d, int off) {
        if (d.length < off + 16) return false;
        for (int i = 0; i < 16; i++) if (d[off + i] != MAGIC[i]) return false;
        return true;
    }
}
