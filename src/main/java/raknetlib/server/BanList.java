package raknetlib.server;

import raknetlib.RakNet;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** IP bans with an optional expiry. Safe from any thread. */
public final class BanList {
    private final Map<InetAddress, Long> bans = new ConcurrentHashMap<>();

    /** millis <= 0 means forever. */
    public void ban(InetAddress ip, long millis) {
        bans.put(ip, millis <= 0 ? Long.MAX_VALUE : RakNet.nowMs() + millis);
    }

    public void unban(InetAddress ip) { bans.remove(ip); }

    public boolean isBanned(InetAddress ip) {
        Long until = bans.get(ip);
        if (until == null) return false;
        if (until < RakNet.nowMs()) { bans.remove(ip); return false; }
        return true;
    }
}
