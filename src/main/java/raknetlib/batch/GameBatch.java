package raknetlib.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;
import raknetlib.BinaryStream;

/**
 * Wraps game packets in a batch ({@code 0xFE 0x06 + length + compressed data}) and unwraps them.
 *
 * Use the shared default ({@link #shared()}, zlib level 6) or build one with your own {@link Compressor}:
 * <pre>
 *   GameBatch batch = new GameBatch(new MyCompressor());
 *   byte[] wire = batch.wrap(idAndBody);
 *   List&lt;byte[]&gt; packets = batch.unwrap(wire);
 * </pre>
 * The static methods {@link #decode} and {@link #precomprimir} always use the shared default.
 */
public final class GameBatch {
    public static final int BATCH_ID = 0x06;
    public static final int LEVEL = 6;

    public static final int MAX_INBOUND = 8 * 1024 * 1024;

    private static final int CACHE_MIN = 4096, CACHE_MAX = 256;
    private static final GameBatch SHARED = new GameBatch(new ZlibCompressor());

    private final Compressor compressor;
    private final Map<byte[], byte[]> cache = Collections.synchronizedMap(
        new LinkedHashMap<byte[], byte[]>(64, 0.75f, true) {
            @Override protected boolean removeEldestEntry(Map.Entry<byte[], byte[]> e) { return size() > CACHE_MAX; }
        });

    public GameBatch(Compressor compressor) {
        if (compressor == null) throw new IllegalArgumentException("compressor is null");
        this.compressor = compressor;
    }

    /** The default batch (zlib level 6). The server uses it until you call setCompressor. */
    public static GameBatch shared() { return SHARED; }

    public Compressor getCompressor() { return compressor; }

    /** ID + body  ->  0xFE 0x06 + length + compressed. Payloads of 4 KB or more are cached (by identity). */
    public byte[] wrap(byte[] packetWithId) {
        boolean cacheable = packetWithId.length >= CACHE_MIN;
        if (cacheable) {
            byte[] hit = cache.get(packetWithId);
            if (hit != null) return hit;
        }
        byte[] inner = new BinaryStream().putInt(packetWithId.length).putBytes(packetWithId).toByteArray();
        byte[] z = compressor.compress(inner);
        byte[] wire = new BinaryStream().putByte(0xfe).putByte(BATCH_ID).putInt(z.length).putBytes(z).toByteArray();
        if (cacheable) cache.put(packetWithId, wire);
        return wire;
    }

    /** 0xFE 0x06 + length + compressed  ->  the packets inside, each as ID + body. */
    public List<byte[]> unwrap(byte[] wire) throws DataFormatException {
        BinaryStream in = new BinaryStream(wire);
        in.getByte();
        in.getByte();
        byte[] raw = compressor.decompress(in.getBytes(in.getInt()), MAX_INBOUND);
        BinaryStream b = new BinaryStream(raw);
        List<byte[]> out = new ArrayList<>();
        while (!b.eof()) out.add(b.getBytes(b.getInt()));
        return out;
    }

    /** Compresses ahead of time so the first send does not pay for it (e.g. chunks at startup). */
    public void precompress(byte[] packetWithId) { wrap(packetWithId); }

    /** Same as {@code GameBatch.shared().unwrap(wire)}. */
    public static List<byte[]> decode(byte[] wire) throws DataFormatException { return SHARED.unwrap(wire); }

    /** Same as {@code GameBatch.shared().precompress(packetWithId)}. */
    public static void precomprimir(byte[] packetWithId) { SHARED.precompress(packetWithId); }
}
