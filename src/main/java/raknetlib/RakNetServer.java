package raknetlib;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class RakNetServer {
    public interface Listener {
        void onOpen(Session s);
        void onPacket(Session s, byte[] buffer);
        void onClose(Session s, String reason);

        default void onAckReceipt(Session s, int receiptId, boolean acked) {}
    }

    private static final long RECONNECT_IDLE_MS = 2_000;

    public final long serverId = new Random().nextLong();
    private final int port;
    private volatile Transport transport;
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    private Supplier<String> motd = () -> "MCPE;Java Server;84;0.15.10;0;20";
    private Listener listener;
    private volatile boolean running;
    private volatile boolean stopRequested;
    private final java.util.concurrent.ConcurrentLinkedQueue<Runnable> tasks = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private volatile Runnable ticker;
    private Consumer<String> logger = System.out::println;
    private Consumer<String> errorLogger = System.err::println;
    private final long startTime = RakNet.nowMs();

    private final Set<Integer> acceptedProtocols = new HashSet<>(Arrays.asList(6, RakNet.PROTOCOL));
    private int maxConnections = 0;
    private int maxPerIp = 0;
    private int maxHalfOpen = 512;
    private int rateLimit = 2000;
    private long rateBlockMs = 5_000;
    private final Map<InetAddress, Long> bans = new ConcurrentHashMap<>();
    private final Map<InetAddress, long[]> rates = new HashMap<>();
    private long lastRateClean = RakNet.nowMs();

    public RakNetServer(int port) { this.port = port; }

    public void setMotd(Supplier<String> motd) { this.motd = motd; }
    public void setListener(Listener l) { this.listener = l; }

    private volatile java.util.function.BiFunction<InetSocketAddress, byte[], byte[]> queryHandler;
    public void setQueryHandler(java.util.function.BiFunction<InetSocketAddress, byte[], byte[]> h) { this.queryHandler = h; }

    public void execute(Runnable r) {
        tasks.add(r);
        Transport t = transport;
        if (t != null) t.wakeup();
    }

    public void setTicker(Runnable r) { this.ticker = r; }
    public int getPort() { return port; }

    public String getTransportName() { Transport t = transport; return t == null ? "none" : t.name(); }
    public Collection<Session> getSessions() { return new ArrayList<>(sessions.values()); }
    public Session getSession(InetSocketAddress a) { return sessions.get(keyOf(a)); }
    public void setLogger(Consumer<String> logger) { this.logger = logger; }

    public void setErrorLogger(Consumer<String> errorLogger) { this.errorLogger = errorLogger; }

    public long getRakNetTimeMS() { return RakNet.nowMs() - startTime; }

    public void setAcceptedProtocols(int... protocols) {
        acceptedProtocols.clear();
        for (int p : protocols) acceptedProtocols.add(p);
    }
    private volatile boolean strict = true, kickOnListenerError = true;

    public void setStrict(boolean b) { strict = b; }
    public boolean isStrict() { return strict; }

    public void setKickOnListenerError(boolean b) { kickOnListenerError = b; }
    public boolean isKickOnListenerError() { return kickOnListenerError; }
    public void setMaxConnections(int n) { this.maxConnections = n; }
    public void setMaxConnectionsPerIp(int n) { this.maxPerIp = n; }
    public void setMaxHalfOpen(int n) { this.maxHalfOpen = n; }

    public void setRateLimit(int perSecond, long blockMs) { this.rateLimit = perSecond; this.rateBlockMs = blockMs; }

    public void ban(InetAddress ip, long millis) {
        bans.put(ip, millis <= 0 ? Long.MAX_VALUE : RakNet.nowMs() + millis);
        execute(() -> {
            for (Session s : getSessions()) if (s.address.getAddress().equals(ip)) s.close("You are banned from this server", true);
        });
    }
    public void unban(InetAddress ip) { bans.remove(ip); }
    public boolean isBanned(InetAddress ip) {
        Long until = bans.get(ip);
        if (until == null) return false;
        if (until < RakNet.nowMs()) { bans.remove(ip); return false; }
        return true;
    }

    public static String keyOf(InetSocketAddress a) { return a.getAddress().getHostAddress() + ":" + a.getPort(); }

    public void log(String s) { logger.accept(s); }

    public void logError(String s) { errorLogger.accept(s); }

    private long warnWindow, warnCount;

    public void logLimited(String s) {
        long now = RakNet.nowMs();
        if (now - warnWindow >= 1000) { warnWindow = now; warnCount = 0; }
        if (++warnCount <= 30) logger.accept(s);
        else if (warnCount == 31) logger.accept("(more warnings suppressed this second)");
    }

    public void send(InetSocketAddress to, byte[] data) {
        Transport t = transport;
        if (t != null) t.send(to, data);
    }

    public void bind() throws SocketException {
        if (transport != null) throw new IllegalStateException("bind() ya se llamo");
        transport = openTransport();
        log("RakNet transport: " + transport.name() + " (port " + port + ")");
    }

    private Transport openTransport() throws SocketException {
        String mode = System.getProperty("raknet.transport", "auto").trim().toLowerCase(Locale.ROOT);
        if (!mode.equals("auto") && !mode.equals("netty") && !mode.equals("socket")) {
            logError("raknet.transport='" + mode + "' no se reconoce (auto, netty o socket); uso auto");
            mode = "auto";
        }
        if (!mode.equals("socket")) {
            Transport t = tryNetty(mode.equals("netty"));
            if (t != null) return t;
        }
        SocketTransport st = new SocketTransport(this, port);
        st.open();
        return st;
    }

    private Transport tryNetty(boolean required) throws SocketException {
        Transport t;
        try {
            Class<?> c = Class.forName("raknetlib.NettyTransport");
            t = (Transport) c.getDeclaredConstructor(RakNetServer.class, int.class).newInstance(this, port);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {

            if (required) throw new SocketException("raknet.transport=netty pero Netty no esta en el classpath");
            return null;
        } catch (ReflectiveOperationException | LinkageError e) {
            if (required) throw new SocketException("No pude cargar NettyTransport: " + e);
            logError("RakNet: no pude cargar Netty (" + e + "); uso sockets Java");
            return null;
        }
        try {
            t.open();
        } catch (SocketException e) {
            throw e;
        } catch (RuntimeException | LinkageError e) {
            if (required) throw new SocketException("No pude iniciar Netty: " + e);
            logError("RakNet: Netty no pudo abrir el puerto (" + e + "); uso sockets Java");
            return null;
        }
        return t;
    }

    public void run() {
        Transport t = transport;
        if (t == null) throw new IllegalStateException("Llama a bind() antes de run()");
        running = true;
        if (stopRequested) running = false;
        Thread gameThread = null;
        if (ticker != null) {
            gameThread = new Thread(this::runTicker, "juego");
            gameThread.start();
        }
        try {
            t.run();
        } finally {
            running = false;
            if (gameThread != null) {
                try { gameThread.join(5_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }
    }

    private void runTicker() {
        Runnable t = ticker;
        long next = RakNet.nowMs();
        while (running) {
            long now = RakNet.nowMs();
            if (now < next) {
                try { Thread.sleep(next - now); } catch (InterruptedException e) { return; }
                continue;
            }
            next += 50;
            if (now - next > 1000) next = now + 50;
            try { t.run(); } catch (RuntimeException | StackOverflowError e) { logError("Error in tick: " + RakNet.describe(e)); e.printStackTrace(); }
        }
    }

    public void stop() {
        stopRequested = true;
        running = false;
        Transport t = transport;
        if (t != null) t.stop();
    }

    void onDatagram(InetSocketAddress from, byte[] data) {
        if (data.length == 0) return;
        try {
            if (queryHandler != null && data.length >= 2 && (data[0] & 0xff) == 0xFE && (data[1] & 0xff) == 0xFD) {
                byte[] reply = queryHandler.apply(from, data);
                if (reply != null) send(from, reply);
            } else {
                handleRaw(from, data, RakNet.nowMs());
            }
        } catch (RuntimeException | StackOverflowError e) {
            logLimited("Error processing datagram from " + from + ": " + RakNet.describe(e));
        }
    }

    void drainTasks() {
        Runnable task;
        while ((task = tasks.poll()) != null) {
            try { task.run(); } catch (RuntimeException e) { logError("Error in task: " + RakNet.describe(e)); e.printStackTrace(); }
        }
    }

    void netTick(long now) {
        drainTasks();
        for (Session s : new ArrayList<>(sessions.values())) {
            try { s.update(now); } catch (RuntimeException e) { logError("Error in session " + s.key + ": " + RakNet.describe(e)); s.close("Internal server error", false); }
        }
        if (now - lastRateClean > 10_000) cleanRates(now);
    }

    void closeAllSessions(String reason) {
        for (Session s : new ArrayList<>(sessions.values())) {
            try { s.close(reason, true); } catch (RuntimeException e) { logError("Error closing session " + s.key + ": " + RakNet.describe(e)); }
        }
    }

    private void handleRaw(InetSocketAddress from, byte[] data, long now) {
        InetAddress ip = from.getAddress();
        int id = data[0] & 0xff;
        if (isBanned(ip)) {
            if (id == RakNet.OPEN_CONNECTION_REQUEST_1 && RakNet.magicAt(data, 1)) replyWithGuid(from, RakNet.CONNECTION_BANNED);
            return;
        }
        if (!allow(ip, now)) return;
        if (id == RakNet.UNCONNECTED_PING || id == RakNet.UNCONNECTED_PING_OPEN) {
            if (data.length < 25 || !RakNet.magicAt(data, 9)) return;
            if (id == RakNet.UNCONNECTED_PING_OPEN && maxConnections > 0 && sessions.size() >= maxConnections) return;
            try {
                BinaryStream in = new BinaryStream(data);
                in.getByte();
                long pingId = in.getLong();
                String name = motd.get();
                BinaryStream o = new BinaryStream();
                o.putByte(RakNet.UNCONNECTED_PONG).putLong(pingId).putLong(serverId).putBytes(RakNet.MAGIC).putString(name);
                send(from, o.toByteArray());
            } catch (RuntimeException e) {
                log("ping invalido de " + from);
            }
            return;
        }
        if (id == RakNet.OPEN_CONNECTION_REQUEST_1) {
            handleRequest1(from, data, now);
            return;
        }
        Session s = sessions.get(keyOf(from));
        if (s != null) s.handle(data, now);
    }

    private void handleRequest1(InetSocketAddress from, byte[] data, long now) {
        if (data.length < 18 || !RakNet.magicAt(data, 1)) return;
        int protocol = data[17] & 0xff;
        if (!acceptedProtocols.contains(protocol)) {
            BinaryStream o = new BinaryStream();
            o.putByte(RakNet.INCOMPATIBLE_PROTOCOL).putByte(RakNet.PROTOCOL).putBytes(RakNet.MAGIC).putLong(serverId);
            send(from, o.toByteArray());
            return;
        }
        String key = keyOf(from);
        Session s = sessions.get(key);
        if (s != null && s.getState() == Session.CONNECTED) {

            if (s.getIdleMs(now) <= RECONNECT_IDLE_MS) return;
            s.close("Reconnecting", false);
            s = null;
        }
        if (s == null) {
            if (maxConnections > 0 && sessions.size() >= maxConnections) { replyWithGuid(from, RakNet.NO_FREE_INCOMING_CONNECTIONS); return; }
            int halfOpen = 0, perIp = 0;
            for (Session o : sessions.values()) {
                if (o.getState() != Session.CONNECTED) halfOpen++;
                if (o.address.getAddress().equals(from.getAddress())) perIp++;
            }
            if (maxPerIp > 0 && perIp >= maxPerIp) { replyWithGuid(from, RakNet.IP_RECENTLY_CONNECTED); return; }
            if (halfOpen >= maxHalfOpen) return;
            s = new Session(this, from, now);
            sessions.put(key, s);
        }
        s.handle(data, now);
    }

    private void replyWithGuid(InetSocketAddress to, int id) {
        BinaryStream o = new BinaryStream();
        o.putByte(id).putBytes(RakNet.MAGIC).putLong(serverId);
        send(to, o.toByteArray());
    }

    private boolean allow(InetAddress ip, long now) {
        if (rateLimit <= 0) return true;
        long[] r = rates.get(ip);
        if (r == null) { r = new long[]{now, 0, 0}; rates.put(ip, r); }
        if (r[2] > now) return false;
        if (now - r[0] >= 1000) { r[0] = now; r[1] = 0; }
        if (++r[1] > rateLimit) {
            r[2] = now + rateBlockMs;
            log("IP " + ip.getHostAddress() + " blocked for " + rateBlockMs + " ms for packet flooding (>" + rateLimit + "/s)");
            return false;
        }
        return true;
    }

    private void cleanRates(long now) {
        lastRateClean = now;
        rates.values().removeIf(r -> r[2] < now && now - r[0] > 5000);
    }

    public void sessionOpened(Session s) { if (listener != null) listener.onOpen(s); }

    public void sessionPacket(Session s, byte[] buf) { if (listener != null) listener.onPacket(s, buf); }

    public void sessionReceipt(Session s, int id, boolean acked) { if (listener != null) listener.onAckReceipt(s, id, acked); }

    public void sessionClosed(Session s, String reason, boolean wasOpen) {
        sessions.remove(s.key, s);
        if (wasOpen && listener != null) listener.onClose(s, reason);
    }
}
