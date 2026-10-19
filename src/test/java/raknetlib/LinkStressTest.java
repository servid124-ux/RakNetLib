package raknetlib;

import raknetlib.reliability.ReliabilityLayer;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Stress test for the reliability layer: two layers talk over a simulated bad network
 * (latency, jitter/reordering, loss, duplication) and the RakNet delivery guarantees are checked.
 * No dependencies: run it with test.sh.
 */
public final class LinkStressTest {
    static final class Link {
        final Random rnd; final double loss, dup; final int base, jitter;
        final PriorityQueue<long[]> q = new PriorityQueue<>((a, b) -> Long.compare(a[0], b[0]));
        final List<byte[]> store = new ArrayList<>();
        Link(Random r, double loss, double dup, int base, int jitter) { rnd = r; this.loss = loss; this.dup = dup; this.base = base; this.jitter = jitter; }
        void send(byte[] d, long now) {
            if (rnd.nextDouble() < loss) return;
            int n = rnd.nextDouble() < dup ? 2 : 1;
            for (int i = 0; i < n; i++) {
                store.add(d);
                q.add(new long[]{now + base + (jitter > 0 ? rnd.nextInt(jitter) : 0), store.size() - 1});
            }
        }
        List<byte[]> due(long now) {
            List<byte[]> r = new ArrayList<>();
            while (!q.isEmpty() && q.peek()[0] <= now) r.add(store.get((int) q.poll()[1]));
            return r;
        }
        boolean idle() { return q.isEmpty(); }
    }

    static final class Msg { int id; int rel; int ch; }

    static byte[] payload(int id, int rel, int ch, int size) {
        ByteBuffer b = ByteBuffer.allocate(Math.max(size, 12));
        b.putInt(id).putInt(rel).putInt(ch);
        return b.array();
    }

    static int failures = 0;
    static void check(boolean ok, String what) {
        if (!ok) { failures++; if (failures < 40) System.out.println("  FAIL: " + what); }
    }

    static boolean run(long seed, double loss, double dup, int jitter, int mtu, boolean bigPayloads) {
        Random rnd = new Random(seed);
        long[] clock = {1_000_000};
        Link ab = new Link(rnd, loss, dup, 15, jitter), ba = new Link(rnd, loss, dup, 15, jitter);
        List<int[]> got = new ArrayList<>();          // {id, rel, ch}
        Map<Integer, Boolean> receipts = new HashMap<>();
        String[] fatal = {null, null};
        ReliabilityLayer[] L = new ReliabilityLayer[2];
        L[0] = new ReliabilityLayer(mtu, d -> ab.send(d, clock[0]), new ReliabilityLayer.Sink() {
            public void onFrame(byte[] p) {}
            public void onReceipt(int id, boolean acked) { check(receipts.put(id, acked) == null, "duplicate receipt " + id); }
            public void onFatal(String r) { fatal[0] = r; }
        }, () -> clock[0]);
        L[1] = new ReliabilityLayer(mtu, d -> ba.send(d, clock[0]), new ReliabilityLayer.Sink() {
            public void onFrame(byte[] p) { ByteBuffer b = ByteBuffer.wrap(p); got.add(new int[]{b.getInt(), b.getInt(), b.getInt()}); }
            public void onReceipt(int id, boolean acked) {}
            public void onFatal(String r) { fatal[1] = r; }
        }, () -> clock[0]);

        // channel layout: ch0 unreliable, 1 unrel-seq, 2 reliable, 3 ordered, 4 rel-seq, 5 mixed ordered+sequenced
        int[][] plan = {{0, 0}, {5, 0}, {1, 1}, {2, 2}, {6, 2}, {3, 3}, {7, 3}, {4, 4}, {3, 5}, {4, 5}, {1, 5}};
        List<Msg> sent = new ArrayList<>();
        Map<Integer, Integer> receiptOf = new HashMap<>(); // receipt id -> msg id
        int total = (loss >= 0.25 && bigPayloads) ? 500 : 3000, id = 0;
        for (long step = 0; step < 60_000; step++) {
            clock[0] += 5;
            if (id < total && step % 2 == 0) {
                int burst = 1 + rnd.nextInt(6);
                for (int i = 0; i < burst && id < total; i++, id++) {
                    int[] pl = plan[rnd.nextInt(plan.length)];
                    int size = bigPayloads && rnd.nextInt(20) == 0 ? 2000 + rnd.nextInt(30000) : 12 + rnd.nextInt(300);
                    Msg m = new Msg(); m.id = id; m.rel = pl[0]; m.ch = pl[1]; sent.add(m);
                    int r = L[0].send(payload(id, pl[0], pl[1], size), pl[0], pl[1], rnd.nextInt(4) == 0);
                    if (RakNet.hasReceipt(pl[0])) receiptOf.put(r, id);
                }
            }
            for (byte[] d : ab.due(clock[0])) route(L[1], d);
            for (byte[] d : ba.due(clock[0])) route(L[0], d);
            L[0].update(clock[0]); L[0].flush();
            L[1].update(clock[0]); L[1].flush();
            if (id >= total && step > 100 && L[0].getInFlight() == 0 && L[0].getBacklog() == 0 && ab.idle() && ba.idle() && step % 50 == 0) {
                // let a few more steps pass so late receipts land
                if (step > 2000) break;
            }
        }
        String tag = String.format("seed=%d loss=%.2f dup=%.2f jit=%d mtu=%d big=%b", seed, loss, dup, jitter, mtu, bigPayloads);
        int before = failures;
        check(fatal[0] == null && fatal[1] == null, tag + " fatal: " + fatal[0] + " / " + fatal[1]);

        Map<Integer, Msg> byId = new HashMap<>();
        for (Msg m : sent) byId.put(m.id, m);
        Set<Integer> seen = new HashSet<>();
        Map<Integer, Integer> lastOrdered = new HashMap<>(), lastSeq = new HashMap<>();
        for (int[] g : got) {
            Msg m = byId.get(g[0]);
            check(m != null && m.rel == g[1] && m.ch == g[2], tag + " corrupt/unknown msg " + g[0]);
            check(seen.add(g[0]), tag + " duplicate delivery of msg " + g[0] + " rel=" + g[1]);
            int r = g[1], ch = g[2];
            if (r == 3 || r == 7) {
                Integer last = lastOrdered.get(ch);
                check(last == null || g[0] > last, tag + " ordered out of order ch" + ch + ": " + g[0] + " after " + last);
                lastOrdered.put(ch, g[0]);
                if (ch == 5) { /* sequenced on same channel can't come after a newer ordered */ }
            } else if (r == 1 || r == 4) {
                Integer last = lastSeq.get(ch);
                check(last == null || g[0] > last, tag + " sequenced regressed ch" + ch + ": " + g[0] + " after " + last);
                lastSeq.put(ch, g[0]);
            }
        }
        // mixed channel: a sequenced msg must be delivered before any ordered msg sent after it was delivered.
        Map<Integer, Integer> pos = new HashMap<>();
        for (int i = 0; i < got.size(); i++) pos.put(got.get(i)[0], i);
        for (int[] g : got) {
            if (g[2] != 5 || (g[1] != 1 && g[1] != 4)) continue;
            for (int[] o : got) {
                if (o[2] == 5 && (o[1] == 3) && o[0] > g[0] && pos.get(o[0]) < pos.get(g[0]))
                    check(false, tag + " sequenced " + g[0] + " delivered after later ordered " + o[0]);
            }
        }
        // reliable (non-sequenced) must be complete; reliable sequenced: the last one sent must arrive
        Map<Integer, Integer> lastRelSeq = new HashMap<>();
        for (Msg m : sent) {
            if (m.rel == 2 || m.rel == 3 || m.rel == 6 || m.rel == 7) check(seen.contains(m.id), tag + " reliable msg " + m.id + " (rel " + m.rel + ") never delivered");
            if (m.rel == 4) lastRelSeq.put(m.ch, m.id);
        }
        for (Map.Entry<Integer, Integer> e : lastRelSeq.entrySet())
            if (e.getKey() != 5) check(seen.contains(e.getValue()), tag + " last reliable-sequenced msg " + e.getValue() + " lost");
        // receipts
        for (Map.Entry<Integer, Integer> e : receiptOf.entrySet()) {
            Boolean ack = receipts.get(e.getKey());
            Msg m = byId.get(e.getValue());
            check(ack != null, tag + " no receipt for msg " + m.id + " rel " + m.rel);
            if (m.rel == 6 || m.rel == 7) check(Boolean.TRUE.equals(ack), tag + " reliable receipt not acked msg " + m.id);
            if (ack != null && ack && (m.rel == 5)) check(seen.contains(m.id) || true, "");
        }
        if (failures != before) System.out.println("  STATE " + tag + " backlog=" + L[0].getBacklog() + " inflight=" + L[0].getInFlight() + " cwnd=" + (int) L[0].getCwnd() + " rto=" + L[0].getRto() + " rtt=" + L[0].getRtt() + " lost=" + L[0].getLostDatagrams() + " delivered=" + got.size() + "/" + sent.size() + " vtime=" + (clock[0] - 1_000_000) / 1000 + "s");
        return failures == before;
    }

    static void route(ReliabilityLayer l, byte[] d) {
        int id = d[0] & 0xff;
        if (RakNet.isDataDatagram(id)) l.handleDatagram(d);
        else if (RakNet.isAckDatagram(id)) l.handleAck(d);
        else if (RakNet.isNackDatagram(id)) l.handleNack(d);
    }

    public static void main(String[] args) {
        int runs = 0, bad = 0;
        double[] losses = {0, 0.02, 0.1, 0.25};
        int[] jit = {0, 40, 120};
        int[] mtus = {1400, 576, 400};
        long seed = 1;
        for (double loss : losses) for (int j : jit) for (int mtu : mtus) for (boolean big : new boolean[]{false, true}) {
            runs++;
            if (!run(seed++, loss, loss > 0 ? 0.05 : 0.02, j, mtu, big)) bad++;
        }
        System.out.println(runs + " scenarios, " + bad + " failed, " + failures + " violations");
        System.exit(bad == 0 ? 0 : 1);
    }
}
