package raknetlib;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

public final class Acknowledge {
    private Acknowledge() {}

    public static byte[] encode(int id, SortedSet<Integer> seqs) {
        return encodeAll(id, seqs, Integer.MAX_VALUE).get(0);
    }

    public static List<byte[]> encodeAll(int id, SortedSet<Integer> seqs, int maxSize) {
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
        if (start == last) {
            o.putByte(1).putLTriad(start & RakNet.MASK24);
        } else {
            o.putByte(0).putLTriad(start & RakNet.MASK24).putLTriad(last & RakNet.MASK24);
        }
    }

    public static int[] decodeRanges(byte[] data) {
        BinaryStream in = new BinaryStream(data);
        in.getByte();
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
        return java.util.Arrays.copyOf(tmp, n);
    }

    public static List<Integer> decode(byte[] data) {
        int[] r = decodeRanges(data);
        List<Integer> out = new ArrayList<>();
        for (int i = 0; i < r.length && out.size() < 4096; i += 2) {
            int len = RakNet.diff24(r[i + 1], r[i]) + 1;
            for (int c = 0; c < len && out.size() < 4096; c++) out.add((r[i] + c) & RakNet.MASK24);
        }
        return out;
    }
}
