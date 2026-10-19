package raknetlib.transport;


import java.net.InetSocketAddress;
import java.net.SocketException;

public interface Transport {

    public String name();

    public void open() throws SocketException;

    public void run();

    public void stop();

    public void send(InetSocketAddress to, byte[] data);

    public void wakeup();
}
