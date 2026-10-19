package raknetlib.transport;

import raknetlib.RakNet;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;

public final class SocketTransport implements Transport {
    private final TransportHandler server;
    private final int port;
    private volatile DatagramSocket socket;
    private volatile boolean running;
    private volatile boolean stopRequested;

    public SocketTransport(TransportHandler server, int port) {
        this.server = server;
        this.port = port;
    }

    @Override public String name() { return "socket"; }

    @Override public void open() throws SocketException {
        DatagramSocket s = new DatagramSocket(null);
        try {
            s.setReuseAddress(true);
            try { s.setReceiveBufferSize(1 << 20); s.setSendBufferSize(1 << 20); } catch (SocketException ignored) { }
            s.bind(new InetSocketAddress(port));
            s.setSoTimeout(2);
        } catch (SocketException e) {
            s.close();
            throw e;
        }
        socket = s;
    }

    @Override public void run() {
        running = true;
        if (stopRequested) running = false;
        byte[] rx = new byte[2048];
        DatagramPacket p = new DatagramPacket(rx, rx.length);
        long nextTick = RakNet.nowMs();
        while (running) {

            for (int i = 0; i < 256 && running; i++) {
                try {
                    p.setLength(rx.length);
                    socket.receive(p);
                    if (p.getLength() > 0) {
                        server.onDatagram((InetSocketAddress) p.getSocketAddress(), Arrays.copyOf(p.getData(), p.getLength()));
                    }
                } catch (SocketTimeoutException e) {
                    break;
                } catch (IOException e) {
                    if (running) server.logError("Error receiving: " + e);
                    break;
                }
            }
            server.drainTasks();
            long now = RakNet.nowMs();
            if (now >= nextTick) {
                nextTick = now + 10;
                server.netTick(now);
            }
        }
        server.closeAllSessions("Server closed");
        socket.close();
    }

    @Override public void stop() { stopRequested = true; running = false; }

    @Override public void send(InetSocketAddress to, byte[] data) {
        DatagramSocket s = socket;
        if (s == null) return;
        try {
            s.send(new DatagramPacket(data, data.length, to));
        } catch (IOException e) {
            server.logError("Error sending to " + to + ": " + e);
        }
    }

    @Override public void wakeup() { }
}
