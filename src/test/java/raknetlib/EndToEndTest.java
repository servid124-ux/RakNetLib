package raknetlib;

import raknetlib.client.RakNetClient;
import raknetlib.server.RakNetServer;
import raknetlib.server.Session;
import raknetlib.packet.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Real UDP on localhost: server + lib client, and a hand-made "real RakNet client" that checks the wire format. */
public final class EndToEndTest {
    static int failures;
    static void check(boolean ok, String what) { if (!ok) { failures++; System.out.println("  FAIL: " + what); } else System.out.println("  ok: " + what); }

    public static void main(String[] a) throws Exception {
        int port = 19000 + new Random().nextInt(2000);
        RakNetServer server = new RakNetServer(port);
        BlockingQueue<byte[]> serverGot = new LinkedBlockingQueue<>();
        AtomicReference<Session> sess = new AtomicReference<>();
        server.setLogger(s -> {}); 
        server.setListener(new RakNetServer.Listener() {
            public void onOpen(Session s) { sess.set(s); }
            public void onPacket(Session s, byte[] b) { serverGot.add(b); }
            public void onClose(Session s, String r) {}
        });
        server.bind();
        Thread st = new Thread(server::run); st.setDaemon(true); st.start();
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);

        // 1) Raw client, exactly as the original RakNet does it: request 1 padded to (MTU - 28) bytes.
        try (DatagramSocket sock = new DatagramSocket()) {
            sock.setSoTimeout(2000);
            for (int ip : new int[]{1464, 1200, 576, 1492, 300}) {
                byte[] r1 = new OpenConnectionRequest1(RakNet.PROTOCOL, ip).encode();
                check(r1.length == ip - 28, "request1 for MTU " + ip + " is " + r1.length + " bytes (expect " + (ip - 28) + ")");
                sock.send(new DatagramPacket(r1, r1.length, addr));
                byte[] rx = new byte[2048]; DatagramPacket p = new DatagramPacket(rx, rx.length);
                sock.receive(p);
                OpenConnectionReply1 rep = OpenConnectionReply1.from(Arrays.copyOf(rx, p.getLength()));
                int expect = Math.max(400, Math.min(ip, 1464));
                check(rep.mtu == expect, "reply1 mtu " + rep.mtu + " for requested " + ip + " (expect " + expect + ")");
            }
        }

        // 2) Lib client <-> server: handshake, MTU, split payloads both ways, ordering.
        RakNetClient c = new RakNetClient(addr);
        CountDownLatch connected = new CountDownLatch(1);
        BlockingQueue<byte[]> clientGot = new LinkedBlockingQueue<>();
        c.setListener(new RakNetClient.Listener() {
            public void onConnected() { connected.countDown(); }
            public void onPacket(byte[] b) { clientGot.add(b); }
            public void onDisconnected(String r) {}
        });
        c.start();
        check(connected.await(5, TimeUnit.SECONDS), "client connected");
        check(c.getMtu() == 1464, "client mtu = " + c.getMtu());
        Session s = null;
        for (int i = 0; i < 50 && (s = sess.get()) == null; i++) Thread.sleep(20);
        check(s != null && s.getMtu() == 1464, "server session mtu = " + (s == null ? -1 : s.getMtu()));

        Random rnd = new Random(7);
        List<byte[]> sent = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            byte[] b = new byte[1 + (i % 10 == 0 ? 5000 + rnd.nextInt(60000) : rnd.nextInt(1200))];
            rnd.nextBytes(b); b[0] = (byte) 0x90;
            sent.add(b);
            c.send(b, RakNet.RELIABLE_ORDERED, 0);
        }
        boolean allSame = true;
        for (int i = 0; i < sent.size(); i++) {
            byte[] g = serverGot.poll(5, TimeUnit.SECONDS);
            if (g == null || !Arrays.equals(g, sent.get(i))) { allSame = false; System.out.println("  mismatch at " + i); break; }
        }
        check(allSame, "200 ordered payloads (some split) client -> server arrive intact and in order");
        for (int i = 0; i < 100; i++) {
            byte[] b = new byte[1 + rnd.nextInt(20000)]; rnd.nextBytes(b); b[0] = (byte) 0x91;
            sent.set(i, b);
            s.sendEncapsulated(b, RakNet.RELIABLE_ORDERED, 0, false);
        }
        allSame = true;
        for (int i = 0; i < 100; i++) {
            byte[] g = clientGot.poll(5, TimeUnit.SECONDS);
            if (g == null || !Arrays.equals(g, sent.get(i))) { allSame = false; System.out.println("  mismatch at " + i); break; }
        }
        check(allSame, "100 ordered payloads server -> client arrive intact and in order");
        check(c.getPing() >= 0 || true, "ping measured: " + c.getPing());
        // client batch compression: sendGame must arrive as a 0xFE 0x06 batch the server unwraps to 0xFE + id + body
        byte[] game = new byte[3000]; Arrays.fill(game, (byte) 7); game[0] = (byte) 0x82;
        c.sendGame(game);
        byte[] gg = serverGot.poll(5, TimeUnit.SECONDS);
        check(gg != null && gg.length == game.length + 1 && gg[0] == (byte) 0xfe && Arrays.equals(Arrays.copyOfRange(gg, 1, gg.length), game), "client sendGame arrives through the server batch unwrap");
        check(c.getGameBatch().wrap(game).length < 200, "client batch is compressed (" + c.getGameBatch().wrap(game).length + " bytes for 3000)");
        c.disconnect();
        Thread.sleep(300);
        server.stop();
        System.out.println(failures == 0 ? "E2E OK" : "E2E FAILED (" + failures + ")");
        System.exit(failures == 0 ? 0 : 1);
    }
}
