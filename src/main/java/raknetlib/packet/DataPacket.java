package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;

import java.util.List;

/**
 * The RakNet datagram that carries frames (IDs 0x80-0x8F): 1 byte flags, 3 bytes sequence number (LE),
 * then one or more {@link EncapsulatedPacket}s.
 *
 * It is not a {@link Packet}: the frames are read one by one by the reliability layer, so a bad frame at the end
 * does not throw away the good frames before it.
 */
public final class DataPacket {
    public static final int ID = 0x84;
    public static final int HEADER_LENGTH = 4;

    private DataPacket() { }

    public static boolean isData(int id) { return RakNet.isDataDatagram(id); }

    /** Reads the flags and the sequence number. The stream is left at the first frame. */
    public static int readSequence(BinaryStream in) {
        in.getByte();
        return in.getLTriad() & RakNet.MASK24;
    }

    /** Builds a datagram from already-encoded frames. totalLength must be HEADER_LENGTH + sum of frame lengths. */
    public static byte[] build(int seq, List<byte[]> frames, int totalLength) {
        byte[] b = new byte[totalLength];
        b[0] = (byte) ID;
        b[1] = (byte) seq; b[2] = (byte) (seq >> 8); b[3] = (byte) (seq >> 16);
        int off = HEADER_LENGTH;
        for (byte[] f : frames) {
            System.arraycopy(f, 0, b, off, f.length);
            off += f.length;
        }
        return b;
    }
}
