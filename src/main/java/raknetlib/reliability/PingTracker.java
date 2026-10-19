package raknetlib.reliability;


import java.util.Arrays;

/** Remembers the last few pings sent (their timestamps) so a pong can be matched with the ping it answers. */
public final class PingTracker {
    private final long[] inFlight = new long[8];
    private int next;

    public void reset() { Arrays.fill(inFlight, 0); }

    /** Call when a ping carrying this id (its send time) goes out. */
    public void sent(long id) {
        inFlight[next] = id;
        next = (next + 1) % inFlight.length;
    }

    /** True the first time a pong with a known id arrives; the id is forgotten so a repeated pong is ignored. */
    public boolean answered(long id) {
        if (id == 0) return false;
        for (int i = 0; i < inFlight.length; i++) {
            if (inFlight[i] == id) { inFlight[i] = 0; return true; }
        }
        return false;
    }
}
