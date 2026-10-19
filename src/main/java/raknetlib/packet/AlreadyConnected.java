package raknetlib.packet;

import raknetlib.RakNet;


/** Server -> client: this address already has a connection. */
public final class AlreadyConnected extends ServerGuidPacket {
    public static final int ID = RakNet.ALREADY_CONNECTED;

    public AlreadyConnected() { }

    public AlreadyConnected(long serverId) { this.serverId = serverId; }

    @Override public int id() { return ID; }

    public static AlreadyConnected from(byte[] data) { return parse(new AlreadyConnected(), data); }
}
