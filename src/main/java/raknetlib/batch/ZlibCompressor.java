package raknetlib.batch;

import java.io.ByteArrayOutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;

/** The default {@link Compressor}: zlib. When decompressing it also accepts raw deflate (see {@link Zlib#inflate}). */
public final class ZlibCompressor implements Compressor {
    private final int level;
    private final ThreadLocal<Deflater> deflater;

    public ZlibCompressor() { this(GameBatch.LEVEL); }

    /** @param level 0 (no compression) to 9 (smallest) */
    public ZlibCompressor(int level) {
        if (level < 0 || level > 9) throw new IllegalArgumentException("zlib level must be 0-9");
        this.level = level;
        this.deflater = ThreadLocal.withInitial(() -> new Deflater(this.level));
    }

    @Override public byte[] compress(byte[] data) {
        Deflater d = deflater.get();
        d.setLevel(level);
        d.reset();
        d.setInput(data);
        d.finish();
        ByteArrayOutputStream out = new ByteArrayOutputStream(data.length / 2 + 64);
        byte[] tmp = new byte[4096];
        while (!d.finished()) out.write(tmp, 0, d.deflate(tmp));
        return out.toByteArray();
    }

    @Override public byte[] decompress(byte[] data, int maxSize) throws DataFormatException {
        return Zlib.inflate(data, maxSize);
    }
}
