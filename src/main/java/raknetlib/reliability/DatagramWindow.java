package raknetlib.reliability;

import raknetlib.RakNet;

public final class DatagramWindow {
    public static final int NEW = 0, DUP = 1;

    private final boolean[] seen;
    private final int mask, size;
    private int highest;

    public DatagramWindow(int size) {
        this.size = size;
        this.mask = size - 1;
        this.seen = new boolean[size];
        reset(0);
    }

    public void reset(int base) {
        java.util.Arrays.fill(seen, false);
        highest = (base - 1) & RakNet.MASK24;
    }

    public int highest() { return highest; }

    public int size() { return size; }

    public int mark(int seq) {
        int d = RakNet.diff24(seq, highest);
        if (d > 0) {
            if (d >= size) java.util.Arrays.fill(seen, false);
            else for (int i = 1; i < d; i++) seen[(highest + i) & mask] = false;
            seen[seq & mask] = true;
            highest = seq;
            return NEW;
        }
        if (d <= -size) return DUP;
        int s = seq & mask;
        if (seen[s]) return DUP;
        seen[s] = true;
        return NEW;
    }
}
