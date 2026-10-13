package raknetlib;

public class EncapsulatedPacket {
    public int reliability;
    public boolean hasSplit;
    public int messageIndex = -1;
    public int sequenceIndex = -1;
    public int orderIndex = -1;
    public int orderChannel;
    public int splitCount, splitId, splitIndex;
    public byte[] buffer = new byte[0];

    public static boolean hasMessageIndex(int r) { return RakNet.isReliable(r); }

    public static boolean hasSequenceIndex(int r) { return RakNet.isSequenced(r); }

    public static boolean hasOrder(int r) { return RakNet.isOrdered(r); }

    public static int headerLength(int reliability, boolean split) {
        return 3 + (hasMessageIndex(reliability) ? 3 : 0) + (hasSequenceIndex(reliability) ? 3 : 0)
            + (hasOrder(reliability) ? 4 : 0) + (split ? 10 : 0);
    }

    public static EncapsulatedPacket read(BinaryStream in) {
        EncapsulatedPacket p = new EncapsulatedPacket();
        int flags = in.getByte() & 0xff;
        p.reliability = (flags & 0xe0) >> 5;
        p.hasSplit = (flags & 0x10) != 0;
        int length = ((in.getShort() & 0xffff) + 7) >> 3;
        if (hasMessageIndex(p.reliability)) p.messageIndex = in.getLTriad() & RakNet.MASK24;
        if (hasSequenceIndex(p.reliability)) p.sequenceIndex = in.getLTriad() & RakNet.MASK24;
        if (hasOrder(p.reliability)) {
            p.orderIndex = in.getLTriad() & RakNet.MASK24;
            p.orderChannel = in.getByte() & 0xff;
        }
        if (p.hasSplit) {
            p.splitCount = in.getInt();
            p.splitId = in.getShort() & 0xffff;
            p.splitIndex = in.getInt();
        }
        p.buffer = in.getBytes(length);
        if (p.buffer.length != length) throw new IllegalArgumentException("Truncated encapsulated packet");
        return p;
    }

    public void write(BinaryStream out) {
        out.putByte((reliability << 5) | (hasSplit ? 0x10 : 0));
        out.putShort((buffer.length << 3) & 0xffff);
        if (hasMessageIndex(reliability)) out.putLTriad(messageIndex & RakNet.MASK24);
        if (hasSequenceIndex(reliability)) out.putLTriad(sequenceIndex & RakNet.MASK24);
        if (hasOrder(reliability)) {
            out.putLTriad(orderIndex & RakNet.MASK24);
            out.putByte(orderChannel);
        }
        if (hasSplit) {
            out.putInt(splitCount);
            out.putShort(splitId & 0xffff);
            out.putInt(splitIndex);
        }
        out.putBytes(buffer);
    }

    public byte[] toBinary() {
        BinaryStream o = new BinaryStream();
        write(o);
        return o.toByteArray();
    }

    public int totalLength() {
        return headerLength(reliability, hasSplit) + buffer.length;
    }
}
