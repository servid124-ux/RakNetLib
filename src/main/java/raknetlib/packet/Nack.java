package raknetlib.packet;

import raknetlib.RakNet;

import java.util.List;
import java.util.SortedSet;

/** Datagram-level NACK: "these datagrams never arrived". */
public final class Nack extends AcknowledgePacket {
    public static final int ID = RakNet.NACK;

    @Override public int id() { return ID; }

    public static List<byte[]> encodeAll(SortedSet<Integer> seqs, int maxSize) { return encodeRanges(ID, seqs, maxSize); }

    public static Nack from(byte[] data) { return parse(new Nack(), data); }
}
