package raknetlib;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BinaryStream {
    private byte[] buf;
    private int len;
    private int pos;

    public BinaryStream() { buf = new byte[256]; }

    public BinaryStream(byte[] data) { this(data, 0, data.length); }

    public BinaryStream(byte[] data, int off, int n) {
        buf = new byte[Math.max(n, 16)];
        System.arraycopy(data, off, buf, 0, n);
        len = n;
    }

    private void ensure(int extra) {
        int need = len + extra;
        if (need > buf.length) buf = Arrays.copyOf(buf, Math.max(need, buf.length * 2));
    }

    public BinaryStream putByte(int v) { ensure(1); buf[len++] = (byte) v; return this; }

    public BinaryStream putShort(int v) { ensure(2); buf[len++] = (byte) (v >> 8); buf[len++] = (byte) v; return this; }

    public BinaryStream putLShort(int v) { ensure(2); buf[len++] = (byte) v; buf[len++] = (byte) (v >> 8); return this; }

    public BinaryStream putInt(int v) {
        ensure(4);
        buf[len++] = (byte) (v >> 24); buf[len++] = (byte) (v >> 16); buf[len++] = (byte) (v >> 8); buf[len++] = (byte) v;
        return this;
    }

    public BinaryStream putLInt(int v) {
        ensure(4);
        buf[len++] = (byte) v; buf[len++] = (byte) (v >> 8); buf[len++] = (byte) (v >> 16); buf[len++] = (byte) (v >> 24);
        return this;
    }

    public BinaryStream putLTriad(int v) {
        ensure(3);
        buf[len++] = (byte) v; buf[len++] = (byte) (v >> 8); buf[len++] = (byte) (v >> 16);
        return this;
    }

    public BinaryStream putLong(long v) { putInt((int) (v >> 32)); putInt((int) v); return this; }

    public BinaryStream putFloat(float f) { return putInt(Float.floatToIntBits(f)); }

    public BinaryStream putDouble(double d) { return putLong(Double.doubleToLongBits(d)); }

    public BinaryStream putBytes(byte[] b) { return putBytes(b, 0, b.length); }

    public BinaryStream putBytes(byte[] b, int off, int n) {
        ensure(n);
        System.arraycopy(b, off, buf, len, n);
        len += n;
        return this;
    }

    public BinaryStream putString(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        putShort(b.length);
        return putBytes(b);
    }

    public BinaryStream putByteString(byte[] b) { putShort(b.length); return putBytes(b); }

    public BinaryStream putLLong(long v) { putLInt((int) v); putLInt((int) (v >> 32)); return this; }

    public BinaryStream putLFloat(float f) { return putLInt(Float.floatToIntBits(f)); }

    public byte[] getByteString() { return getBytes(getShort()); }

    public int getSignedShort() { int v = getShort(); return v >= 0x8000 ? v - 0x10000 : v; }

    public BinaryStream putAddress(InetAddress a, int port) {
        byte[] ip = (a instanceof Inet4Address) ? a.getAddress() : new byte[]{127, 0, 0, 1};
        putByte(4);
        for (byte b : ip) putByte(~b & 0xff);
        return putShort(port);
    }

    private void need(int n) {
        if (n < 0 || pos + n > len) throw new IndexOutOfBoundsException("underflow pos=" + pos + " need=" + n + " len=" + len);
    }

    public int getByte() { need(1); return buf[pos++] & 0xff; }

    public int getShort() { need(2); int v = ((buf[pos] & 0xff) << 8) | (buf[pos + 1] & 0xff); pos += 2; return v; }

    public int getLShort() { need(2); int v = ((buf[pos + 1] & 0xff) << 8) | (buf[pos] & 0xff); pos += 2; return v; }

    public int getInt() {
        need(4);
        int v = ((buf[pos] & 0xff) << 24) | ((buf[pos + 1] & 0xff) << 16) | ((buf[pos + 2] & 0xff) << 8) | (buf[pos + 3] & 0xff);
        pos += 4;
        return v;
    }

    public int getLInt() {
        need(4);
        int v = ((buf[pos + 3] & 0xff) << 24) | ((buf[pos + 2] & 0xff) << 16) | ((buf[pos + 1] & 0xff) << 8) | (buf[pos] & 0xff);
        pos += 4;
        return v;
    }

    public int getLTriad() {
        need(3);
        int v = ((buf[pos + 2] & 0xff) << 16) | ((buf[pos + 1] & 0xff) << 8) | (buf[pos] & 0xff);
        pos += 3;
        return v;
    }

    public long getLong() { long hi = getInt() & 0xffffffffL; long lo = getInt() & 0xffffffffL; return (hi << 32) | lo; }

    public float getFloat() { return Float.intBitsToFloat(getInt()); }

    public double getDouble() { return Double.longBitsToDouble(getLong()); }

    public byte[] getBytes(int n) { need(n); byte[] r = Arrays.copyOfRange(buf, pos, pos + n); pos += n; return r; }

    public byte[] getRemaining() { return getBytes(len - pos); }

    public String getString() { return new String(getBytes(getShort()), StandardCharsets.UTF_8); }

    public InetSocketAddress getAddress() {
        int ver = getByte();
        if (ver != 4) throw new IllegalStateException("solo IPv4 (version=" + ver + ")");
        byte[] ip = new byte[4];
        for (int i = 0; i < 4; i++) ip[i] = (byte) (~getByte() & 0xff);
        int port = getShort();
        try {
            return new InetSocketAddress(InetAddress.getByAddress(ip), port);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public int remaining() { return len - pos; }
    public boolean eof() { return pos >= len; }
    public int position() { return pos; }
    public void position(int p) { pos = p; }
    public int length() { return len; }
    public byte[] toByteArray() { return Arrays.copyOf(buf, len); }

}
