package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;

import java.util.Arrays;

/**
 * Base class for every RakNet packet that has an ID byte.
 *
 * Each subclass lives in its own file and knows two things: how to write itself
 * ({@link #write}) and how to read itself ({@link #read}). The ID byte is handled here.
 *
 *   byte[] wire = new ConnectedPing(now).encode();      // build
 *   ConnectedPing p = ConnectedPing.from(wire);          // parse (throws if malformed)
 *   ConnectedPing q = new ConnectedPing();
 *   if (!q.tryDecode(wire)) return;                      // parse without exceptions
 */
public abstract class Packet {

    /** The packet ID (first byte on the wire). */
    public abstract int id();

    /** Writes the body, without the ID byte. */
    protected abstract void write(BinaryStream out);

    /** Reads the body; the ID byte was already consumed. May throw if the data is truncated or invalid. */
    protected abstract void read(BinaryStream in);

    /** Packets that share one class for several IDs (e.g. ping / ping-open) override this. */
    protected boolean accepts(int id) { return id == id(); }

    /** Called with the ID that was read, right after {@link #accepts}. */
    protected void onId(int id) { }

    public byte[] encode() {
        BinaryStream o = new BinaryStream();
        o.putByte(id());
        write(o);
        return o.toByteArray();
    }

    public void decode(BinaryStream in) {
        int got = in.getByte();
        if (!accepts(got)) {
            throw new IllegalArgumentException("unexpected packet id 0x" + Integer.toHexString(got)
                + " for " + getClass().getSimpleName());
        }
        onId(got);
        read(in);
    }

    public void decode(byte[] data) { decode(new BinaryStream(data)); }

    /** Like {@link #decode(byte[])}, but returns false instead of throwing. */
    public final boolean tryDecode(byte[] data) {
        try {
            decode(data);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    protected static <T extends Packet> T parse(T packet, byte[] data) {
        packet.decode(data);
        return packet;
    }

    protected static void writeMagic(BinaryStream out) { out.putBytes(RakNet.MAGIC); }

    protected static void readMagic(BinaryStream in) {
        if (!Arrays.equals(in.getBytes(16), RakNet.MAGIC)) throw new IllegalArgumentException("invalid RakNet magic");
    }
}
