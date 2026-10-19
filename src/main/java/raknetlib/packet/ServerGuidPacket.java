package raknetlib.packet;

import raknetlib.BinaryStream;


/** Offline packets whose whole body is: magic + server GUID. */
abstract class ServerGuidPacket extends Packet {
    public long serverId;

    @Override protected void write(BinaryStream o) { writeMagic(o); o.putLong(serverId); }

    @Override protected void read(BinaryStream in) { readMagic(in); serverId = in.getLong(); }
}
