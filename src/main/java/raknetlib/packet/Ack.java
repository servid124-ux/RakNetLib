package raknetlib.packet;

import raknetlib.RakNet;

import java.util.List;
import java.util.SortedSet;

/** Datagram-level ACK: "these datagrams arrived". */
public final class Ack extends AcknowledgePacket {
    public static final int ID = RakNet.ACK;

    @Override public int id() { return ID; }

    public static List<byte[]> encodeAll(SortedSet<Integer> seqs, int maxSize) { return encodeRanges(ID, seqs, maxSize); }

    public static Ack from(byte[] data) { return parse(new Ack(), data); }
}
