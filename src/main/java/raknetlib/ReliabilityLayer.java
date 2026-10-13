package raknetlib;

import java.util.*;
import java.util.function.LongSupplier;

final class ReliabilityLayer {
    interface Output { void send(byte[] data); }

    interface Sink {

        void onFrame(byte[] payload);

        void onReceipt(int id, boolean acked);

        void onFatal(String reason);

        default void onListenerError(RuntimeException e) {}
    }

    static final int WINDOW = 2048;
    static final int ORDER_WINDOW = 4096, MAX_HELD = 8192;
    static final int MAX_SPLITS = 256, MAX_SPLIT_PARTS = 512, MAX_SPLIT_BYTES = 4 << 20, MAX_SPLIT_SLOTS = 32768;
    static final long SPLIT_TTL_MS = 30_000;
    static final long MIN_RTO = 200, INITIAL_RTO = 1000, MAX_RTO = 3000, STALL_MS = 30_000;
    static final double MIN_CWND = 8, INIT_CWND = 16, MAX_CWND = 1024, INIT_SSTHRESH = 128;
    static final int MAX_BACKLOG = 16384;
    static final int MAX_UNRELIABLE_BACKLOG = 1024;

    private static final class Receipt {
        final int id; int remaining; boolean failed;
        Receipt(int id) { this.id = id; }
    }

    private static final class Frame {
        final byte[] data; final boolean reliable; final Receipt receipt;
        final long msg;
        boolean acked; long firstSent;
        Frame(byte[] data, boolean reliable, Receipt receipt, long msg) { this.data = data; this.reliable = reliable; this.receipt = receipt; this.msg = msg; }
    }

    private static final class Datagram {
        int seq; long sendTime; int length = 4; boolean limited;
        final ArrayList<Frame> frames = new ArrayList<>(4);
        void add(Frame f) { frames.add(f); length += f.data.length; }
    }

    private static final class Split {
        final EncapsulatedPacket[] parts; int received, bytes; long touched;
        Split(int n, long now) { parts = new EncapsulatedPacket[n]; touched = now; }
    }

    private final Output out;
    private final Sink sink;
    private final LongSupplier clock;
    private final int mtu;
    private boolean dead;

    private final DatagramWindow dgWindow = new DatagramWindow(WINDOW);
    private final SequenceWindow relWindow = new SequenceWindow(WINDOW);
    private final TreeSet<Integer> ackQueue = new TreeSet<>(), nackQueue = new TreeSet<>();
    private final int[] orderNext = new int[RakNet.ORDER_CHANNELS], seqNext = new int[RakNet.ORDER_CHANNELS];
    @SuppressWarnings({"unchecked", "rawtypes"})
    private final HashMap<Integer, EncapsulatedPacket>[] orderHeld = new HashMap[RakNet.ORDER_CHANNELS],
        seqHeld = new HashMap[RakNet.ORDER_CHANNELS];
    private int heldCount;
    private final HashMap<Integer, Split> splits = new HashMap<>();
    private int splitBytes, splitSlots;

    private int nextSplitId, nextSeq, nextReceipt;

    private long msgCounter;
    private final TreeSet<Long> pendingMsgs = new TreeSet<>();
    private final int[] orderWrite = new int[RakNet.ORDER_CHANNELS], seqWrite = new int[RakNet.ORDER_CHANNELS];
    private final LinkedHashMap<Integer, Datagram> inflight = new LinkedHashMap<>();
    private final ArrayDeque<Datagram> outQ = new ArrayDeque<>(), resendQ = new ArrayDeque<>();
    private final ArrayList<Frame> resendFrames = new ArrayList<>();
    private Datagram cur = new Datagram();

    private double cwnd = INIT_CWND, ssthresh = INIT_SSTHRESH;
    private double srtt = -1, rttvar;
    private long rto = INITIAL_RTO;
    private int recoverySeq;
    private long lastPurge;

    private long bytesIn, bytesOut, datagramsOut, resentFrames, lostDatagrams, droppedUnreliable;

    ReliabilityLayer(int mtu, Output out, Sink sink, LongSupplier clock) {
        this.mtu = mtu;
        this.out = out;
        this.sink = sink;
        this.clock = clock;
    }

    int getMtu() { return mtu; }

    int getRtt() { return srtt < 0 ? -1 : (int) Math.round(srtt); }
    long getRto() { return rto; }
    double getCwnd() { return cwnd; }
    int getInFlight() { return inflight.size(); }
    int getBacklog() { return outQ.size() + resendQ.size(); }
    long getBytesIn() { return bytesIn; }
    long getBytesOut() { return bytesOut; }
    long getDatagramsOut() { return datagramsOut; }
    long getResentFrames() { return resentFrames; }
    long getLostDatagrams() { return lostDatagrams; }
    long getDroppedUnreliable() { return droppedUnreliable; }
    boolean isDead() { return dead; }

    void shutdown() { dead = true; }

    void setIndexBase(int base) {
        base &= RakNet.MASK24;
        msgCounter = base; nextSeq = recoverySeq = base;
        Arrays.fill(orderWrite, base); Arrays.fill(orderNext, base);
        dgWindow.reset(base); relWindow.reset(base);
    }

    private void fatal(String reason) {
        if (dead) return;
        dead = true;
        sink.onFatal(reason);
    }

    void handleDatagram(byte[] data) {
        if (dead) return;
        bytesIn += data.length;
        BinaryStream in = new BinaryStream(data);
        in.getByte();
        int seq = in.getLTriad() & RakNet.MASK24;
        int before = dgWindow.highest();
        int st = dgWindow.mark(seq);
        ackQueue.add(seq);
        if (st == DatagramWindow.DUP) return;
        nackQueue.remove(seq);
        int gap = RakNet.diff24(seq, before) - 1;
        if (gap > 0 && gap < dgWindow.size()) {
            for (int i = 1; i <= gap; i++) nackQueue.add((before + i) & RakNet.MASK24);
        }
        while (!dead && !in.eof()) {
            EncapsulatedPacket pk = EncapsulatedPacket.read(in);
            if (pk.buffer.length == 0) break;
            handleFrame(pk);
        }
    }

    private void handleFrame(EncapsulatedPacket f) {
        if (f.messageIndex >= 0) {
            if (relWindow.mark(f.messageIndex) != SequenceWindow.NEW) return;
        }
        if (f.hasSplit) {
            f = assemble(f);
            if (f == null) return;
        }
        deliver(f);
    }

    private EncapsulatedPacket assemble(EncapsulatedPacket p) {
        if (p.splitCount <= 0 || p.splitCount > MAX_SPLIT_PARTS || p.splitIndex < 0 || p.splitIndex >= p.splitCount) {
            fatal("Invalid split packet");
            return null;
        }
        long now = clock.getAsLong();
        Split s = splits.get(p.splitId);
        if (s == null) {
            if (splits.size() >= MAX_SPLITS || splitSlots + p.splitCount > MAX_SPLIT_SLOTS) { fatal("Too many simultaneous split packets"); return null; }
            s = new Split(p.splitCount, now);
            splitSlots += p.splitCount;
            splits.put(p.splitId, s);
        } else if (s.parts.length != p.splitCount) {
            fatal("Inconsistent split packet");
            return null;
        }
        if (s.parts[p.splitIndex] != null) return null;
        s.bytes += p.buffer.length;
        splitBytes += p.buffer.length;
        if (splitBytes > MAX_SPLIT_BYTES) { fatal("Split packet too large"); return null; }
        s.parts[p.splitIndex] = p;
        s.received++;
        s.touched = now;
        if (s.received < s.parts.length) return null;
        splits.remove(p.splitId);
        splitBytes -= s.bytes;
        splitSlots -= s.parts.length;
        byte[] whole = new byte[s.bytes];
        int off = 0;
        for (EncapsulatedPacket part : s.parts) {
            System.arraycopy(part.buffer, 0, whole, off, part.buffer.length);
            off += part.buffer.length;
        }
        EncapsulatedPacket first = s.parts[0], full = new EncapsulatedPacket();
        full.reliability = first.reliability;
        full.orderIndex = first.orderIndex;
        full.sequenceIndex = first.sequenceIndex;
        full.orderChannel = first.orderChannel;
        full.buffer = whole;
        return full;
    }

    private void deliver(EncapsulatedPacket f) {
        int r = f.reliability;
        if (!RakNet.isOrdered(r)) { emit(f.buffer); return; }
        int ch = f.orderChannel;
        if (ch < 0 || ch >= RakNet.ORDER_CHANNELS) return;
        int delta = RakNet.diff24(f.orderIndex, orderNext[ch]);
        if (RakNet.isSequenced(r)) {
            if (delta < 0) return;
            if (delta == 0) { deliverSequenced(ch, f); return; }
            if (delta >= ORDER_WINDOW) return;
            if (seqHeld[ch] == null) seqHeld[ch] = new HashMap<>();
            EncapsulatedPacket old = seqHeld[ch].get(f.orderIndex);
            if (old == null) {
                if (heldCount >= MAX_HELD) { fatal("Order queue full"); return; }
                heldCount++;
                seqHeld[ch].put(f.orderIndex, f);
            } else if (RakNet.diff24(f.sequenceIndex, old.sequenceIndex) > 0) {
                seqHeld[ch].put(f.orderIndex, f);
            }
            return;
        }

        if (delta < 0) return;
        if (delta == 0) {
            orderNext[ch] = (orderNext[ch] + 1) & RakNet.MASK24;
            seqNext[ch] = 0;
            emit(f.buffer);
            drain(ch);
            return;
        }
        if (delta >= ORDER_WINDOW || heldCount >= MAX_HELD) { fatal("Order queue full"); return; }
        if (orderHeld[ch] == null) orderHeld[ch] = new HashMap<>();
        if (orderHeld[ch].put(f.orderIndex, f) == null) heldCount++;
    }

    private void deliverSequenced(int ch, EncapsulatedPacket f) {
        if (RakNet.diff24(f.sequenceIndex, seqNext[ch]) < 0) return;
        seqNext[ch] = (f.sequenceIndex + 1) & RakNet.MASK24;
        emit(f.buffer);
    }

    private void drain(int ch) {
        while (!dead) {
            if (seqHeld[ch] != null) {
                EncapsulatedPacket s = seqHeld[ch].remove(orderNext[ch]);
                if (s != null) { heldCount--; deliverSequenced(ch, s); }
            }
            if (orderHeld[ch] == null) return;
            EncapsulatedPacket o = orderHeld[ch].remove(orderNext[ch]);
            if (o == null) return;
            heldCount--;
            orderNext[ch] = (orderNext[ch] + 1) & RakNet.MASK24;
            seqNext[ch] = 0;
            emit(o.buffer);
        }
    }

    private void emit(byte[] payload) {
        try { sink.onFrame(payload); }
        catch (RuntimeException e) { try { sink.onListenerError(e); } catch (RuntimeException ignored) {} }
    }

    void handleAck(byte[] data) {
        if (dead) return;
        long now = clock.getAsLong();
        eachSeq(Acknowledge.decodeRanges(data), seq -> ackDatagram(seq, now));
        pump();
    }

    void handleNack(byte[] data) {
        if (dead) return;
        eachSeq(Acknowledge.decodeRanges(data), seq -> {
            Datagram d = inflight.remove(seq);
            if (d != null) loseDatagram(d, false);
        });
        rebuildResend();
        pump();
    }

    private interface SeqVisitor { void visit(int seq); }

    private void eachSeq(int[] ranges, SeqVisitor v) {
        for (int i = 0; i < ranges.length && !dead; i += 2) {
            int start = ranges[i], end = ranges[i + 1];
            long len = (long) RakNet.diff24(end, start) + 1;
            if (len <= inflight.size()) {
                int s = start;
                for (long c = 0; c < len; c++, s = (s + 1) & RakNet.MASK24) v.visit(s);
            } else {
                for (Integer key : new ArrayList<>(inflight.keySet())) {
                    if (RakNet.diff24(key, start) >= 0 && RakNet.diff24(end, key) >= 0) v.visit(key);
                }
            }
        }
    }

    private void ackDatagram(int seq, long now) {
        Datagram d = inflight.remove(seq);
        if (d == null) return;
        updateRtt(Math.max(0, now - d.sendTime));
        if (d.limited) cwnd = Math.min(MAX_CWND, cwnd < ssthresh ? cwnd + 1 : cwnd + 1.0 / cwnd);
        for (Frame f : d.frames) {
            if (f.acked) continue;
            f.acked = true;
            if (f.msg >= 0) pendingMsgs.remove(f.msg);
            Receipt rc = f.receipt;
            if (rc != null && --rc.remaining == 0 && !rc.failed) sink.onReceipt(rc.id, true);
        }
    }

    private void updateRtt(long sample) {
        if (srtt < 0) { srtt = sample; rttvar = sample / 2.0; }
        else { rttvar = 0.75 * rttvar + 0.25 * Math.abs(srtt - sample); srtt = 0.875 * srtt + 0.125 * sample; }
        rto = (long) Math.max(MIN_RTO, Math.min(MAX_RTO, srtt + Math.max(4 * rttvar, 30)));
    }

    private void loseDatagram(Datagram d, boolean timeout) {
        lostDatagrams++;
        for (Frame f : d.frames) {
            if (f.acked) continue;
            if (f.reliable) { resendFrames.add(f); resentFrames++; }
            else if (f.receipt != null && !f.receipt.failed) { f.receipt.failed = true; sink.onReceipt(f.receipt.id, false); }
        }
        if (RakNet.diff24(d.seq, recoverySeq) >= 0) {
            recoverySeq = nextSeq;
            ssthresh = Math.max(cwnd / 2, MIN_CWND);
            cwnd = timeout ? MIN_CWND : ssthresh;
        }
    }

    int send(byte[] payload, int reliability, int channel, boolean immediate) {
        if (dead || payload.length == 0) return -1;
        if (reliability < 0 || reliability > 7) throw new IllegalArgumentException("Invalid reliability " + reliability);
        if (channel < 0 || channel >= RakNet.ORDER_CHANNELS) throw new IllegalArgumentException("Invalid channel " + channel);
        int wire = RakNet.wireReliability(reliability);
        Receipt rc = RakNet.hasReceipt(reliability) ? new Receipt(nextReceipt++ & 0x7fffffff) : null;
        int room = mtu - 4;
        boolean split = EncapsulatedPacket.headerLength(wire, false) + payload.length > room;
        if (split) {
            int per0 = room - EncapsulatedPacket.headerLength(RakNet.upgradeForSplit(wire), true);
            if ((long) payload.length > (long) per0 * MAX_SPLIT_PARTS || payload.length > MAX_SPLIT_BYTES)
                throw new IllegalArgumentException("Payload too large: " + payload.length + " bytes (max " + (long) per0 * MAX_SPLIT_PARTS + " with MTU " + mtu + ")");
            wire = RakNet.upgradeForSplit(wire);
        } else if (!RakNet.isReliable(wire) && getBacklog() > MAX_UNRELIABLE_BACKLOG) {

            droppedUnreliable++;
            if (rc != null) sink.onReceipt(rc.id, false);
            return rc != null ? rc.id : -1;
        }
        int oi = -1, si = -1;
        if (RakNet.isOrdered(wire)) {
            oi = orderWrite[channel];
            if (RakNet.isSequenced(wire)) {
                si = seqWrite[channel];
                seqWrite[channel] = (si + 1) & RakNet.MASK24;
            } else {
                orderWrite[channel] = (oi + 1) & RakNet.MASK24;
                seqWrite[channel] = 0;
            }
        }
        if (!split) {
            EncapsulatedPacket p = new EncapsulatedPacket();
            p.reliability = wire; p.buffer = payload; p.orderChannel = channel; p.orderIndex = oi; p.sequenceIndex = si;
            long msg = -1;
            if (RakNet.isReliable(wire)) { msg = takeMsg(); p.messageIndex = (int) (msg & RakNet.MASK24); }
            if (rc != null) rc.remaining = 1;
            enqueue(new Frame(p.toBinary(), RakNet.isReliable(wire), rc, msg), immediate);
        } else {
            int per = room - EncapsulatedPacket.headerLength(wire, true);
            int count = (payload.length + per - 1) / per;
            int sid = nextSplitId;
            nextSplitId = (nextSplitId + 1) & 0xffff;
            if (rc != null) rc.remaining = count;
            for (int i = 0; i < count && !dead; i++) {
                EncapsulatedPacket part = new EncapsulatedPacket();
                part.reliability = wire; part.hasSplit = true;
                part.splitCount = count; part.splitId = sid; part.splitIndex = i;
                part.orderChannel = channel; part.orderIndex = oi; part.sequenceIndex = si;
                long msg = takeMsg();
                part.messageIndex = (int) (msg & RakNet.MASK24);
                part.buffer = Arrays.copyOfRange(payload, i * per, Math.min(payload.length, (i + 1) * per));
                enqueue(new Frame(part.toBinary(), true, rc, msg), false);
            }
            if (immediate) { closeCurrent(); pump(); }
        }
        return rc != null ? rc.id : -1;
    }

    private long takeMsg() {
        long c = msgCounter++;
        pendingMsgs.add(c);
        return c;
    }

    private void enqueue(Frame f, boolean immediate) {
        if (cur.length + f.data.length > mtu) closeCurrent();
        cur.add(f);
        if (immediate) { closeCurrent(); pump(); }
    }

    private void closeCurrent() {
        if (cur.frames.isEmpty()) return;
        outQ.add(cur);
        cur = new Datagram();
        if (outQ.size() > MAX_BACKLOG) fatal("Send queue full (client not responding)");
    }

    private void rebuildResend() {
        if (resendFrames.isEmpty()) return;
        Datagram d = new Datagram();
        for (Frame f : resendFrames) {
            if (f.acked) continue;
            if (d.length + f.data.length > mtu) { resendQ.add(d); d = new Datagram(); }
            d.add(f);
        }
        if (!d.frames.isEmpty()) resendQ.add(d);
        resendFrames.clear();
    }

    private void pump() {
        while (!dead && inflight.size() < (int) cwnd) {
            Datagram d = resendQ.peek();
            if (d != null) resendQ.poll();
            else {
                d = outQ.peek();
                if (d == null) break;
                if (!fitsReceiverWindow(d)) break;
                outQ.poll();
            }
            transmit(d);
        }
    }

    private boolean fitsReceiverWindow(Datagram d) {
        long lowest = pendingMsgs.isEmpty() ? msgCounter : pendingMsgs.first();
        for (Frame f : d.frames) if (f.msg >= 0 && f.msg - lowest >= WINDOW - 16) return false;
        return true;
    }

    private void transmit(Datagram d) {
        long now = clock.getAsLong();
        d.seq = nextSeq;
        nextSeq = (nextSeq + 1) & RakNet.MASK24;
        d.sendTime = now;
        d.limited = !outQ.isEmpty() || !resendQ.isEmpty() || inflight.size() + 1 >= (int) cwnd;
        byte[] b = new byte[d.length];
        b[0] = (byte) 0x84;
        b[1] = (byte) d.seq; b[2] = (byte) (d.seq >> 8); b[3] = (byte) (d.seq >> 16);
        int off = 4;
        for (Frame f : d.frames) {
            if (f.firstSent == 0) f.firstSent = now;
            System.arraycopy(f.data, 0, b, off, f.data.length);
            off += f.data.length;
        }
        inflight.put(d.seq, d);
        bytesOut += b.length;
        datagramsOut++;
        out.send(b);
    }

    void update(long now) {
        if (dead) return;
        flushAcks();
        expireInflight(now);
        if (dead) return;
        rebuildResend();
        closeCurrent();
        pump();
        if (now - lastPurge >= 1000) { lastPurge = now; purgeSplits(now); }
    }

    private void flushAcks() {
        if (!ackQueue.isEmpty()) {
            for (byte[] b : Acknowledge.encodeAll(RakNet.ACK, ackQueue, mtu)) out.send(b);
            ackQueue.clear();
        }
        if (!nackQueue.isEmpty()) {
            for (byte[] b : Acknowledge.encodeAll(RakNet.NACK, nackQueue, mtu)) out.send(b);
            nackQueue.clear();
        }
    }

    private void expireInflight(long now) {
        boolean timedOut = false;
        for (Iterator<Datagram> it = inflight.values().iterator(); it.hasNext(); ) {
            Datagram d = it.next();
            if (now - d.sendTime < rto) break;
            it.remove();
            timedOut = true;
            for (Frame f : d.frames) {
                if (f.reliable && !f.acked && f.firstSent > 0 && now - f.firstSent > STALL_MS) {
                    fatal("Client is not acknowledging data (timed out)");
                    return;
                }
            }
            loseDatagram(d, true);
        }
        if (timedOut) rto = Math.min(rto * 2, MAX_RTO);
    }

    private void purgeSplits(long now) {
        for (Iterator<Split> it = splits.values().iterator(); it.hasNext(); ) {
            Split s = it.next();
            if (now - s.touched > SPLIT_TTL_MS) { splitBytes -= s.bytes; splitSlots -= s.parts.length; it.remove(); }
        }
    }

    void flush() {
        if (dead) return;
        closeCurrent();
        pump();
    }
}
