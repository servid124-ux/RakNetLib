package raknetlib;

final class SequenceWindow {
    static final int NEW = 0, DUP = 1, FAR = 2;

    private final boolean[] seen;
    private final int mask, size;
    private int next;

    SequenceWindow(int size) {
        this.size = size;
        this.mask = size - 1;
        this.seen = new boolean[size];
    }

    int next() { return next; }

    void reset(int base) {
        java.util.Arrays.fill(seen, false);
        next = base & RakNet.MASK24;
    }

    int mark(int idx) {
        int d = RakNet.diff24(idx, next);
        if (d < 0) return DUP;
        if (d >= size) return FAR;
        int s = idx & mask;
        if (seen[s]) return DUP;
        seen[s] = true;
        while (seen[next & mask]) {
            seen[next & mask] = false;
            next = (next + 1) & RakNet.MASK24;
        }
        return NEW;
    }

    boolean has(int idx) {
        int d = RakNet.diff24(idx, next);
        if (d < 0) return true;
        if (d >= size) return false;
        return seen[idx & mask];
    }
}
