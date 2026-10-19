package raknetlib.server;

import raknetlib.RakNet;
import raknetlib.batch.GameBatch;
import raknetlib.packet.AlreadyConnected;
import raknetlib.packet.ClientHandshake;
import raknetlib.packet.ConnectedPing;
import raknetlib.packet.ConnectedPong;
import raknetlib.packet.ConnectionRequest;
import raknetlib.packet.DisconnectNotification;
import raknetlib.packet.OpenConnectionReply1;
import raknetlib.packet.OpenConnectionReply2;
import raknetlib.packet.OpenConnectionRequest1;
import raknetlib.packet.OpenConnectionRequest2;
import raknetlib.packet.ServerHandshake;
import raknetlib.reliability.PingTracker;
import raknetlib.reliability.ReliabilityLayer;

import java.net.InetSocketAddress;
import java.util.List;

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
    private int mtu = RakNet.MIN_MTU;
    private long clientId;
    private volatile Object attachment;
    private ReliabilityLayer layer;
    private byte[] reply2;

    private volatile long lastUpdate;
    private long createdAt;
    private long connectedAt;
    private volatile boolean closed;

    private long lastPingSent, lastPong;
    private final PingTracker pings = new PingTracker();
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
        ping = -1; lastPingSent = 0; lastPong = 0; pings.reset();
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
                if (RakNet.isDataDatagram(id)) layer.handleDatagram(data);
                else if (RakNet.isAckDatagram(id)) layer.handleAck(data);
                else if (RakNet.isNackDatagram(id)) layer.handleNack(data);
            }
        } catch (RuntimeException e) {
            server.logLimited("Invalid packet from " + key + " (id 0x" + Integer.toHexString(id) + "): " + RakNet.describe(e));

            boolean wire = RakNet.isDataDatagram(id) || RakNet.isAckDatagram(id) || RakNet.isNackDatagram(id);
            if (server.isStrict() && wire && state == CONNECTED) close("Invalid packet: " + RakNet.describe(e), true);
        }
    }

    private void handleRequest1(byte[] data) {
        if (state == CONNECTED) return;

        if (state != CONNECTING_2) resetState();
        OpenConnectionRequest1 req = OpenConnectionRequest1.from(data);
        int mtuReq = RakNet.clampMtu(req.mtu);
        server.send(address, new OpenConnectionReply1(server.serverId, mtuReq).encode());
        if (state != CONNECTING_2) state = CONNECTING_1;
    }

    private void handleRequest2(byte[] data) {
        if (state == CONNECTED) {
            server.send(address, new AlreadyConnected(server.serverId).encode());
            return;
        }
        if ((state != CONNECTING_1 && state != CONNECTING_2) || !RakNet.magicAt(data, 1)) return;
        OpenConnectionRequest2 req = OpenConnectionRequest2.from(data);
        long guid = req.clientGuid;
        if (state == CONNECTING_2) {
            if (guid == clientId && reply2 != null) {
                server.send(address, reply2);
                return;
            }
            resetState();
        }
        clientId = guid;
        mtu = RakNet.clampMtu(req.mtu);
        reply2 = new OpenConnectionReply2(server.serverId, address, mtu).encode();
        layer = new ReliabilityLayer(RakNet.maxPayload(mtu), raw -> server.send(address, raw), new ReliabilityLayer.Sink() {
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
            if (id == DisconnectNotification.ID) {
                close("Client disconnected", false);
            } else if (id == ConnectedPing.ID) {
                ConnectedPing ping = new ConnectedPing();
                if (!ping.tryDecode(buf)) return;
                sendEncapsulated(new ConnectedPong(ping.time, server.getRakNetTimeMS()).encode(), RakNet.UNRELIABLE, 0, false);
            } else if (id == ConnectedPong.ID) {
                if (state == CONNECTED) handlePong(buf);
            } else if (id == ConnectionRequest.ID && (state == CONNECTING_2 || state == CONNECTED)) {
                ConnectionRequest req = new ConnectionRequest();
                if (!req.tryDecode(buf)) return;
                sendEncapsulated(new ServerHandshake(address, req.time, server.getRakNetTimeMS()).encode(), RakNet.UNRELIABLE, 0, true);
            } else if (id == ClientHandshake.ID && state == CONNECTING_2) {
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
        List<byte[]> paquetes;
        try {
            paquetes = server.getGameBatch().unwrap(buf);
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
        ConnectedPong pong = new ConnectedPong();
        if (!pong.tryDecode(buf)) return;
        if (!pings.answered(pong.pingTime)) return;
        long rtt = lastUpdate - pong.pingTime;
        if (rtt >= 0) { ping = (int) Math.min(rtt, Integer.MAX_VALUE); lastPong = lastUpdate; }
    }

    private void pingTick(long now) {
        if (ping >= 0 && now - lastPong > PING_STALE_MS) ping = -1;
        if (now - lastPingSent < PING_INTERVAL_MS) return;
        lastPingSent = now;
        pings.sent(now);
        sendEncapsulated(new ConnectedPing(now).encode(), RakNet.UNRELIABLE, 0, false);
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
        return sendEncapsulated(server.getGameBatch().wrap(gamePacketWithId), reliability, channel, immediate);
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
            byte[] dc = new DisconnectNotification().encode();
            layer.send(dc, RakNet.UNRELIABLE, 0, true);
            layer.send(dc, RakNet.UNRELIABLE, 0, true);
        }
        closed = true;
        if (layer != null) layer.shutdown();
        boolean wasOpen = state == CONNECTED;
        state = UNCONNECTED;
        server.sessionClosed(this, reason, wasOpen);
    }
}
