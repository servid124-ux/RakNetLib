package raknetlib.batch;

import java.util.zip.DataFormatException;

/**
 * How game batches are compressed. The default is {@link ZlibCompressor}.
 * If your game version needs something else (another zlib level, raw deflate, your own library), implement this
 * and give it to the server with {@code server.setCompressor(...)}.
 *
 * Both methods can be called from several threads at once, so do not keep shared mutable state without protecting it.
 */
public interface Compressor {

    /** Compresses data. Must never return null. */
    byte[] compress(byte[] data);

    /**
     * Decompresses data received from a player.
     *
     * @param maxSize the biggest result you may produce (protects against zip bombs). Throw if it would be bigger.
     * @throws DataFormatException if the data is corrupt; the batch is dropped
     */
    byte[] decompress(byte[] data, int maxSize) throws DataFormatException;
}
