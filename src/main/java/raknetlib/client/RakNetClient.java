package raknetlib.client;

import raknetlib.RakNet;
import raknetlib.batch.Compressor;
import raknetlib.batch.GameBatch;
import raknetlib.packet.AlreadyConnected;
import raknetlib.packet.ClientHandshake;
import raknetlib.packet.ConnectedPing;
import raknetlib.packet.ConnectedPong;
import raknetlib.packet.ConnectionBanned;
import raknetlib.packet.ConnectionRequest;
import raknetlib.packet.DisconnectNotification;
import raknetlib.packet.IncompatibleProtocol;
import raknetlib.packet.IpRecentlyConnected;
import raknetlib.packet.NoFreeIncomingConnections;
import raknetlib.packet.OpenConnectionReply1;
import raknetlib.packet.OpenConnectionReply2;
import raknetlib.packet.OpenConnectionRequest1;
import raknetlib.packet.OpenConnectionRequest2;
import raknetlib.packet.ServerHandshake;
import raknetlib.packet.UnconnectedPing;
import raknetlib.packet.UnconnectedPong;
import raknetlib.reliability.PingTracker;
import raknetlib.reliability.ReliabilityLayer;
import raknetlib.server.Session;

import java.io.IOException;
import java.net.*;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RakNetClient {
    public interface Listener {
        void onConnected();
        void onPacket(byte[] buffer);
        void onDisconnected(String reason);
        default void onAckReceipt(int receiptId, boolean acked) {}
    }

    private static final int IDLE = 0, REQ1 = 1, REQ2 = 2, CONNECTING = 3, CONNECTED = 4;
    private static final int[] MTU_TRIES = {1464, 1200, 576};
    private static final long RETRY_MS = 400, TIMEOUT_MS = 10_000;

    private final InetSocketAddress server;
    private final long guid = new Random().nextLong();
    private DatagramSocket socket;
    private Listener listener;
    private volatile boolean running;
    private volatile int state = IDLE;
    private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private ReliabilityLayer layer;
    private int mtu = RakNet.MAX_MTU;
    private volatile int protocolVersion = RakNet.PROTOCOL;
    private volatile GameBatch gameBatch = GameBatch.shared();
    private long serverGuid;
    private long lastAttempt, lastPacket, startTime;
    private int attempt;
    private volatile int ping = -1;
    private long lastPingSent;
    private String closeReason;
    private final PingTracker pings = new PingTracker();

    public RakNetClient(InetSocketAddress server) { this.server = server; }

    public void setListener(Listener l) { this.listener = l; }

    /** Chooses the RakNet protocol version the client announces. Call it before start() or run(). */
    public void setProtocolVersion(int version) {
        if (version < 0 || version > 255) throw new IllegalArgumentException("RakNet version must be 0-255");
        protocolVersion = version;
    }
    public int getProtocolVersion() { return protocolVersion; }

    /** Your own compression for unwrapping what the server sends: getGameBatch().unwrap(buf). */
    public void setCompressor(Compressor compressor) { gameBatch = new GameBatch(compressor); }
    public GameBatch getGameBatch() { return gameBatch; }
    public boolean isConnected() { return state == CONNECTED; }
    public int getPing() { return ping; }
    public int getMtu() { return mtu; }
    public long getGuid() { return guid; }
    public void execute(Runnable r) { tasks.add(r); }

    public Thread start() throws SocketException {
        open();
        Thread t = new Thread(this::loop, "raknet-client");
        t.setDaemon(true);
        t.start();
        return t;
    }

    public void run() throws SocketException {
        open();
        loop();
    }

    private void open() throws SocketException {
        socket = new DatagramSocket();
        socket.setSoTimeout(5);
        running = true;
        state = REQ1;
        startTime = lastPacket = RakNet.nowMs();
    }

    /** Sends a game packet (ID + body) inside a compressed batch, reliable ordered. */
    public void sendGame(byte[] gamePacketWithId) {
        send(gameBatch.wrap(gamePacketWithId), RakNet.RELIABLE_ORDERED, 0);
    }

    /** Sends a game packet as 0xFE + ID + body, without batching or compression. */
    public void sendGameUncompressed(byte[] gamePacketWithId) {
        byte[] b = new byte[gamePacketWithId.length + 1];
        b[0] = (byte) 0xfe;
        System.arraycopy(gamePacketWithId, 0, b, 1, gamePacketWithId.length);
        send(b, RakNet.RELIABLE_ORDERED, 0);
    }

    public void send(byte[] payload, int reliability, int channel) {
        execute(() -> { if (layer != null && state == CONNECTED) layer.send(payload, reliability, channel, false); });
    }

    public void disconnect() {
        execute(() -> {
            if (layer != null && state == CONNECTED) {
                byte[] dc = new DisconnectNotification().encode();
                layer.send(dc, RakNet.UNRELIABLE, 0, true);
                layer.send(dc, RakNet.UNRELIABLE, 0, true);
            }
            finish("Disconnected");
        });
    }

    private void finish(String reason) {
        if (!running) return;
        running = false;
        closeReason = reason;
    }

    private void raw(byte[] b) {
        try { socket.send(new DatagramPacket(b, b.length, server)); } catch (IOException ignored) {}
    }

    private void loop() {
        byte[] rx = new byte[2048];
        try {
            while (running) {
                try {
                    DatagramPacket p = new DatagramPacket(rx, rx.length);
                    socket.receive(p);
                    if (p.getLength() > 0 && p.getSocketAddress().equals(server)) {
                        long now = RakNet.nowMs();
                        lastPacket = now;
                        try { handle(java.util.Arrays.copyOf(p.getData(), p.getLength())); }
                        catch (RuntimeException e) {  }
                    }
                } catch (SocketTimeoutException ignored) {
                } catch (IOException e) {
                    finish("Network error: " + e.getMessage());
                }
                Runnable t;
                while ((t = tasks.poll()) != null) t.run();
                long now = RakNet.nowMs();
                tick(now);
            }
        } finally {
            boolean wasConnected = state == CONNECTED;
            state = IDLE;
            if (layer != null) layer.shutdown();
            socket.close();
            if (listener != null) listener.onDisconnected(closeReason == null ? "Closed" : closeReason);
        }
    }

    private void tick(long now) {
        if (state != CONNECTED && now - startTime > TIMEOUT_MS) { finish("Connection timed out"); return; }
        if (state == CONNECTED && now - lastPacket > Session.TIMEOUT_MS) { finish("Timed out"); return; }
        if (state == REQ1 && now - lastAttempt >= RETRY_MS) {
            lastAttempt = now;
            int size = MTU_TRIES[attempt++ % MTU_TRIES.length];
            raw(new OpenConnectionRequest1(protocolVersion, size).encode());
        } else if (state == REQ2 && now - lastAttempt >= RETRY_MS) {
            lastAttempt = now;
            sendRequest2();
        } else if (state == CONNECTING && now - lastAttempt >= RETRY_MS) {
            lastAttempt = now;
            sendConnectionRequest();
        } else if (state == CONNECTED && layer != null) {
            layer.update(now);
            if (now - lastPingSent >= 1000) {
                lastPingSent = now;
                pings.sent(now);
                layer.send(new ConnectedPing(now).encode(), RakNet.UNRELIABLE, 0, false);
            }
            layer.flush();
        }
    }

    private void sendRequest2() {
        raw(new OpenConnectionRequest2(server, mtu, guid).encode());
    }

    private void sendConnectionRequest() {
        layer.send(new ConnectionRequest(guid, RakNet.nowMs() - startTime).encode(), RakNet.RELIABLE, 0, true);
    }

    private void handle(byte[] d) {
        int id = d[0] & 0xff;
        if (id == OpenConnectionReply1.ID && state == REQ1) {
            OpenConnectionReply1 reply = OpenConnectionReply1.from(d);
            serverGuid = reply.serverId;
            mtu = RakNet.clampMtu(reply.mtu);
            state = REQ2;
            lastAttempt = RakNet.nowMs();
            sendRequest2();
        } else if (id == OpenConnectionReply2.ID && state == REQ2) {
            OpenConnectionReply2 reply = OpenConnectionReply2.from(d);
            mtu = RakNet.clampMtu(reply.mtu);
            layer = new ReliabilityLayer(RakNet.maxPayload(mtu), this::raw, new ReliabilityLayer.Sink() {
                @Override public void onFrame(byte[] payload) { route(payload); }
                @Override public void onReceipt(int rid, boolean acked) { if (listener != null) listener.onAckReceipt(rid, acked); }
                @Override public void onFatal(String reason) { finish(reason); }
            }, RakNet::nowMs);
            state = CONNECTING;
            lastAttempt = RakNet.nowMs();
            sendConnectionRequest();
        } else if (id == IncompatibleProtocol.ID) {
            finish("Incompatible RakNet protocol version");
        } else if (id == NoFreeIncomingConnections.ID) {
            finish("Server is full");
        } else if (id == ConnectionBanned.ID) {
            finish("You are banned from this server");
        } else if (id == IpRecentlyConnected.ID) {
            finish("Too many connections from this IP");
        } else if (id == AlreadyConnected.ID) {
            finish("Already connected");
        } else if (layer != null && (state == CONNECTING || state == CONNECTED)) {
            if (RakNet.isDataDatagram(id)) layer.handleDatagram(d);
            else if (RakNet.isAckDatagram(id)) layer.handleAck(d);
            else if (RakNet.isNackDatagram(id)) layer.handleNack(d);
        }
    }

    private void route(byte[] buf) {
        if (buf.length == 0) return;
        int id = buf[0] & 0xff;
        if (id == ServerHandshake.ID && state == CONNECTING) {
            long t = RakNet.nowMs() - startTime;
            layer.send(new ClientHandshake(server, t, t).encode(), RakNet.RELIABLE_ORDERED, 0, true);
            state = CONNECTED;
            if (listener != null) listener.onConnected();
        } else if (id == ConnectedPing.ID) {
            ConnectedPing ping = new ConnectedPing();
            if (!ping.tryDecode(buf)) return;
            layer.send(new ConnectedPong(ping.time, RakNet.nowMs() - startTime).encode(), RakNet.UNRELIABLE, 0, false);
        } else if (id == ConnectedPong.ID) {
            ConnectedPong pong = new ConnectedPong();
            if (!pong.tryDecode(buf)) return;
            if (pings.answered(pong.pingTime)) ping = (int) Math.max(0, RakNet.nowMs() - pong.pingTime);
        } else if (id == DisconnectNotification.ID) {
            finish("Server closed the connection");
        } else if (id >= 0x80 && state == CONNECTED) {
            if (listener != null) listener.onPacket(buf);
        }
    }

    public static String ping(InetSocketAddress server, int timeoutMs) {
        try (DatagramSocket s = new DatagramSocket()) {
            s.setSoTimeout(timeoutMs);
            byte[] b = new UnconnectedPing(System.currentTimeMillis(), 0).encode();
            s.send(new DatagramPacket(b, b.length, server));
            byte[] rx = new byte[2048];
            DatagramPacket p = new DatagramPacket(rx, rx.length);
            s.receive(p);
            byte[] d = java.util.Arrays.copyOf(p.getData(), p.getLength());
            if ((d[0] & 0xff) != UnconnectedPong.ID) return null;
            return UnconnectedPong.from(d).motd;
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }
}
