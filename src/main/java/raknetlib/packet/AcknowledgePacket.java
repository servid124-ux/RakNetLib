package raknetlib.packet;

import raknetlib.BinaryStream;
import raknetlib.RakNet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Common part of {@link Ack} and {@link Nack}: a list of datagram sequence numbers, sent as ranges.
 * Body: count (short) + records. Record = 1, seq  |  0, first, last.
 */
public abstract class AcknowledgePacket extends Packet {
    /** Safety cap when expanding ranges received from the network. */
    public static final int MAX_DECODED = 4096;

    public final TreeSet<Integer> sequences = new TreeSet<>();

    @Override protected void write(BinaryStream o) {
        byte[] all = encodeAll(Integer.MAX_VALUE).get(0);
        o.putBytes(all, 1, all.length - 1);
    }

    @Override protected void read(BinaryStream in) {
        int[] r = readRanges(in);
        for (int i = 0; i < r.length && sequences.size() < MAX_DECODED; i += 2) {
            int len = RakNet.diff24(r[i + 1], r[i]) + 1;
            for (int c = 0; c < len && sequences.size() < MAX_DECODED; c++) sequences.add((r[i] + c) & RakNet.MASK24);
        }
    }

    /** Splits the sequences in as many packets as needed so none is bigger than maxSize bytes. */
    public List<byte[]> encodeAll(int maxSize) { return encodeRanges(id(), sequences, maxSize); }

    static List<byte[]> encodeRanges(int id, SortedSet<Integer> seqs, int maxSize) {
        List<byte[]> out = new ArrayList<>();
        BinaryStream payload = new BinaryStream();
        int records = 0, size = 3;
        int start = -1, last = -1;
        boolean open = false;
        for (int cur : seqs) {
            if (open && cur == last + 1) { last = cur; continue; }
            if (open) {
                int need = start == last ? 4 : 7;
                if (records > 0 && (size + need > maxSize || records >= 0xffff)) {
                    out.add(finish(id, records, payload));
                    payload = new BinaryStream(); records = 0; size = 3;
                }
                writeRecord(payload, start, last); records++; size += need;
            }
            start = last = cur; open = true;
        }
        if (open) {
            int need = start == last ? 4 : 7;
            if (records > 0 && (size + need > maxSize || records >= 0xffff)) {
                out.add(finish(id, records, payload));
                payload = new BinaryStream(); records = 0;
            }
            writeRecord(payload, start, last); records++;
        }
        out.add(finish(id, records, payload));
        return out;
    }

    private static byte[] finish(int id, int records, BinaryStream payload) {
        BinaryStream o = new BinaryStream();
        o.putByte(id).putShort(records).putBytes(payload.toByteArray());
        return o.toByteArray();
    }

    private static void writeRecord(BinaryStream o, int start, int last) {
        if (start == last) o.putByte(1).putLTriad(start & RakNet.MASK24);
        else o.putByte(0).putLTriad(start & RakNet.MASK24).putLTriad(last & RakNet.MASK24);
    }

    /** Reads a whole ACK/NACK as pairs [start0, end0, start1, end1, ...] without expanding them. */
    public static int[] decodeRanges(byte[] data) {
        BinaryStream in = new BinaryStream(data);
        in.getByte();
        return readRanges(in);
    }

    private static int[] readRanges(BinaryStream in) {
        int count = in.getShort() & 0xffff;
        int[] tmp = new int[Math.min(count, 4096) * 2];
        int n = 0;
        for (int i = 0; i < count && n < tmp.length && !in.eof(); i++) {
            int start, end;
            if ((in.getByte() & 0xff) == 0) {
                start = in.getLTriad() & RakNet.MASK24;
                end = in.getLTriad() & RakNet.MASK24;
                if (RakNet.diff24(end, start) < 0) continue;
            } else {
                start = end = in.getLTriad() & RakNet.MASK24;
            }
            tmp[n++] = start; tmp[n++] = end;
        }
        return Arrays.copyOf(tmp, n);
    }
}
