package raknetlib;

import java.util.zip.Deflater;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;

public class Session {
    public static final int UNCONNECTED = 0, CONNECTING_1 = 1, CONNECTING_2 = 2, CONNECTED = 3;
    public static final int MAX_SPLIT_SIZE = ReliabilityLayer.MAX_SPLIT_PARTS, MAX_SPLIT_COUNT = ReliabilityLayer.MAX_SPLITS,
        WINDOW_SIZE = ReliabilityLayer.WINDOW;
    public static final long TIMEOUT_MS = 10_000;

    @Deprecated public static final long RESEND_MS = ReliabilityLayer.INITIAL_RTO;

    private static final long PING_INTERVAL_MS = 1_000, PING_STALE_MS = 5_000;

    public static final long HANDSHAKE_TIMEOUT_MS = 8_000;

    private final RakNetServer server;
    public final InetSocketAddress address;
    public final String key;
    private volatile int state = UNCONNECTED;
    private int mtu = 508;
    private long clientId;
    private volatile Object attachment;
    private ReliabilityLayer layer;
    private byte[] reply2;

    private volatile long lastUpdate;
    private long createdAt;
    private long connectedAt;
    private volatile boolean closed;
    private final Deflater deflater = new Deflater(GameBatch.LEVEL);

    private long lastPingSent, lastPong;
    private final long[] pingsInFlight = new long[8];
    private int pingIdx;
    private volatile int ping = -1;

    public Session(RakNetServer server, InetSocketAddress address, long now) {
        this.server = server;
        this.address = address;
        this.key = RakNetServer.keyOf(address);
        this.lastUpdate = now;
        this.createdAt = now;
    }

    public int getState() { return state; }
    public int getMtu() { return mtu; }
    public long getClientId() { return clientId; }

    public int getPing() { return ping; }

    public synchronized int getAckRtt() { return layer == null ? -1 : layer.getRtt(); }
    public boolean isClosed() { return closed; }
    public Object getAttachment() { return attachment; }
    public void setAttachment(Object o) { attachment = o; }

    public long getIdleMs(long now) { return now - lastUpdate; }

    public long getIdleMs() { return getIdleMs(RakNet.nowMs()); }
    public long getConnectedAt() { return connectedAt; }
    public synchronized long getBytesIn() { return layer == null ? 0 : layer.getBytesIn(); }
    public synchronized long getBytesOut() { return layer == null ? 0 : layer.getBytesOut(); }

    public synchronized long getLostDatagrams() { return layer == null ? 0 : layer.getLostDatagrams(); }

    public synchronized int getBacklog() { return layer == null ? 0 : layer.getBacklog(); }

    private void resetState() {
        layer = null;
        reply2 = null;
        ping = -1; lastPingSent = 0; lastPong = 0; Arrays.fill(pingsInFlight, 0);
    }

    public synchronized void handle(byte[] data, long now) {
        if (closed || data.length == 0) return;
        lastUpdate = now;
        int id = data[0] & 0xff;
        try {
            if (id == RakNet.OPEN_CONNECTION_REQUEST_1) {
                handleRequest1(data);
            } else if (id == RakNet.OPEN_CONNECTION_REQUEST_2) {
                handleRequest2(data);
            } else if (layer != null && (state == CONNECTED || state == CONNECTING_2)) {
                if (id >= 0x80 && id <= 0x8f) layer.handleDatagram(data);
                else if (id == RakNet.ACK) layer.handleAck(data);
                else if (id == RakNet.NACK) layer.handleNack(data);
            }
        } catch (RuntimeException e) {
            server.logLimited("Invalid packet from " + key + " (id 0x" + Integer.toHexString(id) + "): " + RakNet.describe(e));

            boolean wire = (id >= 0x80 && id <= 0x8f) || id == RakNet.ACK || id == RakNet.NACK;
            if (server.isStrict() && wire && state == CONNECTED) close("Invalid packet: " + RakNet.describe(e), true);
        }
    }

    private void handleRequest1(byte[] data) {
        if (state == CONNECTED) return;

        if (state != CONNECTING_2) resetState();
        int mtuReq = Math.max(RakNet.MIN_MTU, Math.min(data.length, RakNet.MAX_MTU));
        BinaryStream o = new BinaryStream();
        o.putByte(RakNet.OPEN_CONNECTION_REPLY_1).putBytes(RakNet.MAGIC).putLong(server.serverId).putByte(0).putShort(mtuReq);
        server.send(address, o.toByteArray());
        if (state != CONNECTING_2) state = CONNECTING_1;
    }

    private void handleRequest2(byte[] data) {
        if (state == CONNECTED) {
            BinaryStream o = new BinaryStream();
            o.putByte(RakNet.ALREADY_CONNECTED).putBytes(RakNet.MAGIC).putLong(server.serverId);
            server.send(address, o.toByteArray());
            return;
        }
        if ((state != CONNECTING_1 && state != CONNECTING_2) || !RakNet.magicAt(data, 1)) return;
        BinaryStream in = new BinaryStream(data);
        in.getByte();
        in.getBytes(16);
        in.getAddress();
        int mtuReq = in.getShort() & 0xffff;
        long guid = in.getLong();
        if (state == CONNECTING_2) {
            if (guid == clientId && reply2 != null) {
                server.send(address, reply2);
                return;
            }
            resetState();
        }
        clientId = guid;
        mtu = Math.max(RakNet.MIN_MTU, Math.min(mtuReq, RakNet.MAX_MTU));
        BinaryStream o = new BinaryStream();
        o.putByte(RakNet.OPEN_CONNECTION_REPLY_2).putBytes(RakNet.MAGIC).putLong(server.serverId)
            .putAddress(address.getAddress(), address.getPort()).putShort(mtu).putByte(0);
        reply2 = o.toByteArray();
        layer = new ReliabilityLayer(mtu, raw -> server.send(address, raw), new ReliabilityLayer.Sink() {
            @Override public void onFrame(byte[] payload) { route(payload); }
            @Override public void onReceipt(int id, boolean acked) { if (!closed) server.sessionReceipt(Session.this, id, acked); }
            @Override public void onFatal(String reason) { close(reason, false); }
            @Override public void onListenerError(RuntimeException e) {
                server.logError("Exception while handling a packet from " + key + ": " + RakNet.describe(e)); e.printStackTrace();
                if (server.isKickOnListenerError()) close("Error processing your packet: " + RakNet.describe(e), true);
            }
        }, RakNet::nowMs);
        server.send(address, reply2);
        state = CONNECTING_2;
    }

    private void route(byte[] buf) {
        if (closed || buf.length == 0) return;
        int id = buf[0] & 0xff;
        if (id < 0x80) {
            if (id == RakNet.CLIENT_DISCONNECT) {
                close("Client disconnected", false);
            } else if (id == RakNet.PING) {
                if (buf.length < 9) return;
                BinaryStream in = new BinaryStream(buf);
                in.getByte();
                long pingId = in.getLong();
                BinaryStream o = new BinaryStream();
                o.putByte(RakNet.PONG).putLong(pingId).putLong(server.getRakNetTimeMS());
                sendEncapsulated(o.toByteArray(), RakNet.UNRELIABLE, 0, false);
            } else if (id == RakNet.PONG) {
                if (state == CONNECTED) handlePong(buf);
            } else if (id == RakNet.CLIENT_CONNECT && (state == CONNECTING_2 || state == CONNECTED)) {
                if (buf.length < 17) return;
                BinaryStream in = new BinaryStream(buf);
                in.getByte();
                in.getLong();
                long sendPing = in.getLong();
                BinaryStream o = new BinaryStream();
                o.putByte(RakNet.SERVER_HANDSHAKE).putAddress(address.getAddress(), address.getPort()).putShort(0);
                for (int i = 0; i < 10; i++) {
                    o.putAddress(i == 0 ? InetAddress.getLoopbackAddress() : zeroAddr(), 0);
                }
                o.putLong(sendPing).putLong(server.getRakNetTimeMS());
                sendEncapsulated(o.toByteArray(), RakNet.UNRELIABLE, 0, true);
            } else if (id == RakNet.CLIENT_HANDSHAKE && state == CONNECTING_2) {
                state = CONNECTED;
                connectedAt = System.currentTimeMillis();
                server.sessionOpened(this);
            }
        } else if (state == CONNECTED) {
            if (buf.length >= 2 && (buf[0] & 0xff) == 0xfe && (buf[1] & 0xff) == GameBatch.BATCH_ID) deliverBatch(buf);
            else server.sessionPacket(this, buf);
        }
    }

    private void deliverBatch(byte[] buf) {
        java.util.List<byte[]> paquetes;
        try {
            paquetes = GameBatch.decode(buf);
        } catch (Exception e) {
            return;
        }
        for (byte[] pk : paquetes) {
            byte[] con = new byte[pk.length + 1];
            con[0] = (byte) 0xfe;
            System.arraycopy(pk, 0, con, 1, pk.length);
            server.sessionPacket(this, con);
        }
    }

    private void handlePong(byte[] buf) {
        if (buf.length < 9) return;
        BinaryStream in = new BinaryStream(buf);
        in.getByte();
        long id = in.getLong();
        for (int i = 0; i < pingsInFlight.length; i++) {
            if (id != 0 && pingsInFlight[i] == id) {
                pingsInFlight[i] = 0;
                long rtt = lastUpdate - id;
                if (rtt >= 0) { ping = (int) Math.min(rtt, Integer.MAX_VALUE); lastPong = lastUpdate; }
                return;
            }
        }
    }

    private void pingTick(long now) {
        if (ping >= 0 && now - lastPong > PING_STALE_MS) ping = -1;
        if (now - lastPingSent < PING_INTERVAL_MS) return;
        lastPingSent = now;
        pingsInFlight[pingIdx] = now;
        pingIdx = (pingIdx + 1) % pingsInFlight.length;
        BinaryStream o = new BinaryStream();
        o.putByte(RakNet.PING).putLong(now);
        sendEncapsulated(o.toByteArray(), RakNet.UNRELIABLE, 0, false);
    }

    private static InetAddress zeroAddr() {
        try { return InetAddress.getByAddress(new byte[4]); } catch (Exception e) { throw new IllegalStateException(e); }
    }

    public synchronized int sendEncapsulated(byte[] payload, int reliability, int channel, boolean immediate) {
        if (closed || layer == null) return -1;
        return layer.send(payload, reliability, channel, immediate);
    }

    public void kick(String reason) { close(reason, true); }

    public void sendGame(byte[] gamePacketWithId, boolean immediate) {
        sendGame(gamePacketWithId, RakNet.RELIABLE_ORDERED, 0, immediate);
    }

    public synchronized int sendGame(byte[] gamePacketWithId, int reliability, int channel, boolean immediate) {
        if (closed) return -1;
        return sendEncapsulated(GameBatch.encode(deflater, gamePacketWithId), reliability, channel, immediate);
    }

    public synchronized void update(long now) {
        if (closed) return;
        if (now - lastUpdate > TIMEOUT_MS) {
            close("Timed out", false);
            return;
        }
        if (state != CONNECTED && now - createdAt > HANDSHAKE_TIMEOUT_MS) {
            close("Handshake timed out", false);
            return;
        }
        if (layer == null) return;
        layer.update(now);
        if (closed) return;
        if (state == CONNECTED) pingTick(now);
        layer.flush();
    }

    public synchronized void close(String reason, boolean notifyClient) {
        if (closed) return;
        if (notifyClient && state == CONNECTED && layer != null) {
            byte[] dc = {(byte) RakNet.CLIENT_DISCONNECT};
            layer.send(dc, RakNet.UNRELIABLE, 0, true);
            layer.send(dc, RakNet.UNRELIABLE, 0, true);
        }
        closed = true;
        deflater.end();
        if (layer != null) layer.shutdown();
        boolean wasOpen = state == CONNECTED;
        state = UNCONNECTED;
        server.sessionClosed(this, reason, wasOpen);
    }
}
