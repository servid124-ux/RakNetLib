package raknetlib.server;

import raknetlib.RakNet;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/** Limits datagrams per second per IP. An IP over the limit is blocked for a while. Network thread only. */
public final class RateLimiter {
    private static final long CLEAN_EVERY_MS = 10_000;

    private final Consumer<String> log;
    private final Map<InetAddress, long[]> rates = new HashMap<>();   // ip -> {windowStart, count, blockedUntil}
    private int perSecond = 2000;
    private long blockMs = 5_000;
    private long lastClean = RakNet.nowMs();

    public RateLimiter(Consumer<String> log) { this.log = log; }

    public void configure(int perSecond, long blockMs) { this.perSecond = perSecond; this.blockMs = blockMs; }

    public boolean allow(InetAddress ip, long now) {
        if (perSecond <= 0) return true;
        long[] r = rates.get(ip);
        if (r == null) { r = new long[]{now, 0, 0}; rates.put(ip, r); }
        if (r[2] > now) return false;
        if (now - r[0] >= 1000) { r[0] = now; r[1] = 0; }
        if (++r[1] > perSecond) {
            r[2] = now + blockMs;
            log.accept("IP " + ip.getHostAddress() + " blocked for " + blockMs + " ms for packet flooding (>" + perSecond + "/s)");
            return false;
        }
        return true;
    }

    /** Forgets IPs that are quiet. Cheap to call every tick. */
    public void cleanIfDue(long now) {
        if (now - lastClean <= CLEAN_EVERY_MS) return;
        lastClean = now;
        rates.values().removeIf(r -> r[2] < now && now - r[0] > 5000);
    }
}
