package raknetlib;

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
    private long serverGuid;
    private long lastAttempt, lastPacket, startTime;
    private int attempt;
    private volatile int ping = -1;
    private long lastPingSent;
    private String closeReason;
    private final long[] pingsInFlight = new long[8];
    private int pingIdx;

    public RakNetClient(InetSocketAddress server) { this.server = server; }

    public void setListener(Listener l) { this.listener = l; }
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

    public void sendGame(byte[] gamePacketWithId) {
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
                byte[] dc = {(byte) RakNet.CLIENT_DISCONNECT};
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
            byte[] b = new byte[size];
            b[0] = (byte) RakNet.OPEN_CONNECTION_REQUEST_1;
            System.arraycopy(RakNet.MAGIC, 0, b, 1, 16);
            b[17] = (byte) RakNet.PROTOCOL;
            raw(b);
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
                pingsInFlight[pingIdx] = now;
                pingIdx = (pingIdx + 1) % pingsInFlight.length;
                BinaryStream o = new BinaryStream();
                o.putByte(RakNet.PING).putLong(now);
                layer.send(o.toByteArray(), RakNet.UNRELIABLE, 0, false);
            }
            layer.flush();
        }
    }

    private void sendRequest2() {
        BinaryStream o = new BinaryStream();
        o.putByte(RakNet.OPEN_CONNECTION_REQUEST_2).putBytes(RakNet.MAGIC)
            .putAddress(server.getAddress(), server.getPort()).putShort(mtu).putLong(guid);
        raw(o.toByteArray());
    }

    private void sendConnectionRequest() {
        BinaryStream o = new BinaryStream();
        o.putByte(RakNet.CLIENT_CONNECT).putLong(guid).putLong(RakNet.nowMs() - startTime).putByte(0);
        layer.send(o.toByteArray(), RakNet.RELIABLE, 0, true);
    }

    private void handle(byte[] d) {
        int id = d[0] & 0xff;
        if (id == RakNet.OPEN_CONNECTION_REPLY_1 && state == REQ1) {
            BinaryStream in = new BinaryStream(d);
            in.getByte();
            in.getBytes(16);
            serverGuid = in.getLong();
            in.getByte();
            mtu = Math.max(RakNet.MIN_MTU, Math.min(in.getShort() & 0xffff, RakNet.MAX_MTU));
            state = REQ2;
            lastAttempt = RakNet.nowMs();
            sendRequest2();
        } else if (id == RakNet.OPEN_CONNECTION_REPLY_2 && state == REQ2) {
            BinaryStream in = new BinaryStream(d);
            in.getByte();
            in.getBytes(16);
            in.getLong();
            in.getAddress();
            mtu = Math.max(RakNet.MIN_MTU, Math.min(in.getShort() & 0xffff, RakNet.MAX_MTU));
            layer = new ReliabilityLayer(mtu, this::raw, new ReliabilityLayer.Sink() {
                @Override public void onFrame(byte[] payload) { route(payload); }
                @Override public void onReceipt(int rid, boolean acked) { if (listener != null) listener.onAckReceipt(rid, acked); }
                @Override public void onFatal(String reason) { finish(reason); }
            }, RakNet::nowMs);
            state = CONNECTING;
            lastAttempt = RakNet.nowMs();
            sendConnectionRequest();
        } else if (id == RakNet.INCOMPATIBLE_PROTOCOL) {
            finish("Incompatible RakNet protocol version");
        } else if (id == RakNet.NO_FREE_INCOMING_CONNECTIONS) {
            finish("Server is full");
        } else if (id == RakNet.CONNECTION_BANNED) {
            finish("You are banned from this server");
        } else if (id == RakNet.IP_RECENTLY_CONNECTED) {
            finish("Too many connections from this IP");
        } else if (id == RakNet.ALREADY_CONNECTED) {
            finish("Already connected");
        } else if (layer != null && (state == CONNECTING || state == CONNECTED)) {
            if (id >= 0x80 && id <= 0x8f) layer.handleDatagram(d);
            else if (id == RakNet.ACK) layer.handleAck(d);
            else if (id == RakNet.NACK) layer.handleNack(d);
        }
    }

    private void route(byte[] buf) {
        if (buf.length == 0) return;
        int id = buf[0] & 0xff;
        if (id == RakNet.SERVER_HANDSHAKE && state == CONNECTING) {
            BinaryStream o = new BinaryStream();
            o.putByte(RakNet.CLIENT_HANDSHAKE).putAddress(server.getAddress(), server.getPort());
            for (int i = 0; i < 10; i++) o.putAddress(i == 0 ? InetAddress.getLoopbackAddress() : zero(), 0);
            long t = RakNet.nowMs() - startTime;
            o.putLong(t).putLong(t);
            layer.send(o.toByteArray(), RakNet.RELIABLE_ORDERED, 0, true);
            state = CONNECTED;
            if (listener != null) listener.onConnected();
        } else if (id == RakNet.PING) {
            if (buf.length < 9) return;
            BinaryStream in = new BinaryStream(buf);
            in.getByte();
            BinaryStream o = new BinaryStream();
            o.putByte(RakNet.PONG).putLong(in.getLong()).putLong(RakNet.nowMs() - startTime);
            layer.send(o.toByteArray(), RakNet.UNRELIABLE, 0, false);
        } else if (id == RakNet.PONG) {
            if (buf.length < 9) return;
            BinaryStream in = new BinaryStream(buf);
            in.getByte();
            long pid = in.getLong();
            for (int i = 0; i < pingsInFlight.length; i++) {
                if (pid != 0 && pingsInFlight[i] == pid) {
                    pingsInFlight[i] = 0;
                    ping = (int) Math.max(0, RakNet.nowMs() - pid);
                    return;
                }
            }
        } else if (id == RakNet.CLIENT_DISCONNECT) {
            finish("Server closed the connection");
        } else if (id >= 0x80 && state == CONNECTED) {
            if (listener != null) listener.onPacket(buf);
        }
    }

    private static InetAddress zero() {
        try { return InetAddress.getByAddress(new byte[4]); } catch (Exception e) { throw new IllegalStateException(e); }
    }

    public static String ping(InetSocketAddress server, int timeoutMs) {
        try (DatagramSocket s = new DatagramSocket()) {
            s.setSoTimeout(timeoutMs);
            BinaryStream o = new BinaryStream();
            o.putByte(RakNet.UNCONNECTED_PING).putLong(System.currentTimeMillis()).putBytes(RakNet.MAGIC).putLong(0);
            byte[] b = o.toByteArray();
            s.send(new DatagramPacket(b, b.length, server));
            byte[] rx = new byte[2048];
            DatagramPacket p = new DatagramPacket(rx, rx.length);
            s.receive(p);
            byte[] d = java.util.Arrays.copyOf(p.getData(), p.getLength());
            if ((d[0] & 0xff) != RakNet.UNCONNECTED_PONG) return null;
            BinaryStream in = new BinaryStream(d);
            in.getByte(); in.getLong(); in.getLong(); in.getBytes(16);
            int len = in.getShort() & 0xffff;
            return new String(in.getBytes(len), java.nio.charset.StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }
}
