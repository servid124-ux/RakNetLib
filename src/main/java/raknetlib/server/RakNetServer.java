package raknetlib.server;

import raknetlib.batch.Compressor;
import raknetlib.batch.GameBatch;
import raknetlib.RakNet;
import raknetlib.packet.ConnectionBanned;
import raknetlib.packet.IncompatibleProtocol;
import raknetlib.packet.IpRecentlyConnected;
import raknetlib.packet.NoFreeIncomingConnections;
import raknetlib.packet.OpenConnectionRequest1;
import raknetlib.packet.UnconnectedPing;
import raknetlib.packet.UnconnectedPong;
import raknetlib.transport.SocketTransport;
import raknetlib.transport.Transport;

import raknetlib.transport.TransportHandler;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class RakNetServer implements TransportHandler {
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
    private volatile int protocolVersion = RakNet.PROTOCOL;
    private volatile GameBatch gameBatch = GameBatch.shared();
    private int maxConnections = 0;
    private int maxPerIp = 0;
    private int maxHalfOpen = 512;
    private final BanList bans = new BanList();
    private final RateLimiter rateLimiter = new RateLimiter(s -> log(s));

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

    /**
     * Chooses the RakNet protocol version of this server. Only clients that use this version can connect, and it is
     * the version a client is told about when it is refused. To accept several versions, call
     * {@link #setAcceptedProtocols} afterwards. Call it before {@link #run()}.
     */
    public void setProtocolVersion(int version) {
        if (version < 0 || version > 255) throw new IllegalArgumentException("RakNet version must be 0-255");
        protocolVersion = version;
        setAcceptedProtocols(version);
    }

    public int getProtocolVersion() { return protocolVersion; }

    /**
     * Uses your own compression for game batches instead of zlib. Call it before players connect.
     * Players must understand it, since it is what they receive and what they are expected to send.
     */
    public void setCompressor(Compressor compressor) { gameBatch = new GameBatch(compressor); }

    public Compressor getCompressor() { return gameBatch.getCompressor(); }

    /** The batch writer/reader this server uses. Handy to precompress big packets: getGameBatch().precompress(pk). */
    public GameBatch getGameBatch() { return gameBatch; }
    private volatile boolean strict = true, kickOnListenerError = true;

    public void setStrict(boolean b) { strict = b; }
    public boolean isStrict() { return strict; }

    public void setKickOnListenerError(boolean b) { kickOnListenerError = b; }
    public boolean isKickOnListenerError() { return kickOnListenerError; }
    public void setMaxConnections(int n) { this.maxConnections = n; }
    public void setMaxConnectionsPerIp(int n) { this.maxPerIp = n; }
    public void setMaxHalfOpen(int n) { this.maxHalfOpen = n; }

    public void setRateLimit(int perSecond, long blockMs) { rateLimiter.configure(perSecond, blockMs); }

    public void ban(InetAddress ip, long millis) {
        bans.ban(ip, millis);
        execute(() -> {
            for (Session s : getSessions()) if (s.address.getAddress().equals(ip)) s.close("You are banned from this server", true);
        });
    }
    public void unban(InetAddress ip) { bans.unban(ip); }
    public boolean isBanned(InetAddress ip) { return bans.isBanned(ip); }

    public static String keyOf(InetSocketAddress a) { return a.getAddress().getHostAddress() + ":" + a.getPort(); }

    public void log(String s) { logger.accept(s); }

    @Override public void logError(String s) { errorLogger.accept(s); }

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
        SocketTransport st = new SocketTransport(this, port);
        st.open();
        return st;
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

    @Override public void onDatagram(InetSocketAddress from, byte[] data) {
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

    @Override public void drainTasks() {
        Runnable task;
        while ((task = tasks.poll()) != null) {
            try { task.run(); } catch (RuntimeException e) { logError("Error in task: " + RakNet.describe(e)); e.printStackTrace(); }
        }
    }

    @Override public void netTick(long now) {
        drainTasks();
        for (Session s : new ArrayList<>(sessions.values())) {
            try { s.update(now); } catch (RuntimeException e) { logError("Error in session " + s.key + ": " + RakNet.describe(e)); s.close("Internal server error", false); }
        }
        rateLimiter.cleanIfDue(now);
    }

    @Override public void closeAllSessions(String reason) {
        for (Session s : new ArrayList<>(sessions.values())) {
            try { s.close(reason, true); } catch (RuntimeException e) { logError("Error closing session " + s.key + ": " + RakNet.describe(e)); }
        }
    }

    private void handleRaw(InetSocketAddress from, byte[] data, long now) {
        InetAddress ip = from.getAddress();
        int id = data[0] & 0xff;
        if (isBanned(ip)) {
            if (id == OpenConnectionRequest1.ID && RakNet.magicAt(data, 1)) send(from, new ConnectionBanned(serverId).encode());
            return;
        }
        if (!rateLimiter.allow(ip, now)) return;
        if (id == UnconnectedPing.ID || id == UnconnectedPing.ID_OPEN) {
            handlePing(from, data);
            return;
        }
        if (id == OpenConnectionRequest1.ID) {
            handleRequest1(from, data, now);
            return;
        }
        Session s = sessions.get(keyOf(from));
        if (s != null) s.handle(data, now);
    }

    private void handlePing(InetSocketAddress from, byte[] data) {
        UnconnectedPing ping = new UnconnectedPing();
        if (data.length < 25 || !RakNet.magicAt(data, 9)) return;
        if (!ping.tryDecode(data)) { log("ping invalido de " + from); return; }
        if (ping.openConnectionsOnly && maxConnections > 0 && sessions.size() >= maxConnections) return;
        send(from, new UnconnectedPong(ping.pingId, serverId, motd.get()).encode());
    }

    private void handleRequest1(InetSocketAddress from, byte[] data, long now) {
        OpenConnectionRequest1 req = new OpenConnectionRequest1();
        if (data.length < 18 || !req.tryDecode(data)) return;
        if (!acceptedProtocols.contains(req.protocol)) {
            send(from, new IncompatibleProtocol(protocolVersion, serverId).encode());
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
            if (maxConnections > 0 && sessions.size() >= maxConnections) { send(from, new NoFreeIncomingConnections(serverId).encode()); return; }
            int halfOpen = 0, perIp = 0;
            for (Session o : sessions.values()) {
                if (o.getState() != Session.CONNECTED) halfOpen++;
                if (o.address.getAddress().equals(from.getAddress())) perIp++;
            }
            if (maxPerIp > 0 && perIp >= maxPerIp) { send(from, new IpRecentlyConnected(serverId).encode()); return; }
            if (halfOpen >= maxHalfOpen) return;
            s = new Session(this, from, now);
            sessions.put(key, s);
        }
        s.handle(data, now);
    }

    public void sessionOpened(Session s) { if (listener != null) listener.onOpen(s); }

    public void sessionPacket(Session s, byte[] buf) { if (listener != null) listener.onPacket(s, buf); }

    public void sessionReceipt(Session s, int id, boolean acked) { if (listener != null) listener.onAckReceipt(s, id, acked); }

    public void sessionClosed(Session s, String reason, boolean wasOpen) {
        sessions.remove(s.key, s);
        if (wasOpen && listener != null) listener.onClose(s, reason);
    }
}
