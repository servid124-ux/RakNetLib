package raknetlib.transport;


import java.net.InetSocketAddress;

/** What a {@link Transport} needs from whoever owns it (the server). The transport only moves datagrams. */
public interface TransportHandler {
    /** A datagram arrived. */
    void onDatagram(InetSocketAddress from, byte[] data);

    /** Run the tasks queued with execute(). */
    void drainTasks();

    /** Called about every 10 ms: resends, pings, timeouts. */
    void netTick(long now);

    /** The transport is shutting down. */
    void closeAllSessions(String reason);

    void logError(String message);
}
