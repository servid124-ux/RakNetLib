package raknetlib;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;

public final class GameBatch {
    public static final int BATCH_ID = 0x06;
    public static final int LEVEL = 6;

    public static final int MAX_INBOUND = 8 * 1024 * 1024;

    private static final int CACHE_MIN = 4096, CACHE_MAX = 256;
    private static final Map<byte[], byte[]> CACHE = Collections.synchronizedMap(
        new LinkedHashMap<byte[], byte[]>(64, 0.75f, true) {
            @Override protected boolean removeEldestEntry(Map.Entry<byte[], byte[]> e) { return size() > CACHE_MAX; }
        });

    private GameBatch() {}

    public static byte[] encode(Deflater d, byte[] packetWithId) {
        boolean cacheable = packetWithId.length >= CACHE_MIN;
        if (cacheable) {
            byte[] hit = CACHE.get(packetWithId);
            if (hit != null) return hit;
        }
        byte[] wire = build(d, packetWithId);
        if (cacheable) CACHE.put(packetWithId, wire);
        return wire;
    }

    public static void precomprimir(byte[] packetWithId) {
        Deflater d = new Deflater(LEVEL);
        try {
            encode(d, packetWithId);
        } finally {
            d.end();
        }
    }

    public static List<byte[]> decode(byte[] wire) throws DataFormatException {
        BinaryStream in = new BinaryStream(wire);
        in.getByte();
        in.getByte();
        byte[] raw = Zlib.inflate(in.getBytes(in.getInt()), MAX_INBOUND);
        BinaryStream b = new BinaryStream(raw);
        List<byte[]> out = new ArrayList<>();
        while (!b.eof()) out.add(b.getBytes(b.getInt()));
        return out;
    }

    private static byte[] build(Deflater d, byte[] pk) {
        byte[] inner = new BinaryStream().putInt(pk.length).putBytes(pk).toByteArray();
        byte[] z = deflate(d, inner);
        return new BinaryStream().putByte(0xfe).putByte(BATCH_ID).putInt(z.length).putBytes(z).toByteArray();
    }

    private static byte[] deflate(Deflater d, byte[] data) {
        d.setLevel(LEVEL);
        d.reset();
        d.setInput(data);
        d.finish();
        ByteArrayOutputStream out = new ByteArrayOutputStream(data.length / 2 + 64);
        byte[] tmp = new byte[4096];
        while (!d.finished()) out.write(tmp, 0, d.deflate(tmp));
        return out.toByteArray();
    }
}
