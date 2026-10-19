package raknetlib;

import java.net.InetSocketAddress;
import java.net.SocketException;

interface Transport {

    String name();

    void open() throws SocketException;

    void run();

    void stop();

    void send(InetSocketAddress to, byte[] data);

    void wakeup();
}
