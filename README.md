# RakNetLib

RakNet transport for the **Minecraft Pocket Edition 0.15.10** protocol (protocol 84), written in Java 11+.

It does the work under the game, for both **servers** and **clients**:

- UDP socket and RakNet handshake
- Reliable delivery (ACK/NACK, resends, split packets, ordering channels)
- Ping/pong and latency measurement
- Connection limits, IP bans and rate limiting
- zlib compression of game batches (`0xFE 0x06`)
- Query hook (`0xFE 0xFD`) and MOTD replies

> **It does not know what the game packets mean.** Decoding and building game packets (login, chunks, movement, ...)
> is the job of the project that uses it.

| | |
|---|---|
| Version | `1.7.0-beta` |
| RakNet protocol | 8 |
| Game protocol (default MOTD) | 84 (MCPE 0.15.10) |
| Java | 11 or newer |
| Dependencies | Netty 4.2.18.Final, downloaded automatically when you compile (used when present, plain sockets otherwise) |

## Contents

- [Installation](#installation)
- [Core concepts](#core-concepts)
- [Server guide](#server-guide)
- [Client guide](#client-guide)
- [API reference](#api-reference)
- [Transports](#transports)
- [Threading model](#threading-model)
- [Troubleshooting](#troubleshooting)
- [Pending](#pending)

## Installation

There are two ways to get RakNetLib. Pick one.

| | Option 1: JAR | Option 2: Source code |
|---|---|---|
| What you get | A ready-made `RakNetLib.jar` | The full project, which you compile yourself |
| You need | Java 11+ | JDK 11+ (and optionally Maven) |
| Best for | Using the library quickly | Reading, changing or contributing to the code |

RakNetLib is a plain **Java 11+** library. The package is `raknetlib` and the Maven coordinates are `raknetlib:RakNetLib:1.7.0-beta`.

### Option 1: Download the JAR

1. Go to the [**Releases**](https://github.com/servid124-ux/RakNetLib/releases) page of the GitHub repository and download `RakNetLib.jar`.
2. Put it in your project (for example in a `libs/` folder) and add it to your classpath.

```sh
# compile your code against the jar
javac -cp libs/RakNetLib.jar MyServer.java

# run it (on Windows use ; instead of :)
java -cp libs/RakNetLib.jar:. MyServer
```

In an IDE, add the jar as a library. With Maven, you can install the downloaded jar into your local repository:

```sh
mvn install:install-file -Dfile=RakNetLib.jar -DgroupId=raknetlib -DartifactId=RakNetLib -Dversion=1.7.0-beta -Dpackaging=jar
```

and then use it in your `pom.xml`:

```xml
<dependency>
  <groupId>raknetlib</groupId>
  <artifactId>RakNetLib</artifactId>
  <version>1.7.0-beta</version>
</dependency>
```

### Option 2: Build from source

Clone the repository and compile it yourself:

```sh
git clone https://github.com/servid124-ux/RakNetLib.git
cd RakNetLib
```

**With the build script** (no Maven needed, only a JDK; works on Termux too):

```sh
sh build.sh      # produces target/RakNetLib.jar
```

`build.sh` needs the Netty jars (`netty-common`, `netty-buffer`, `netty-resolver`, `netty-transport`, version 4.2.18.Final):

- `NETTY_JARS=/some/folder sh build.sh` uses the jars you already have in that folder.
- Otherwise it downloads them from Maven Central into `~/.cache/raknetlib` (needs `curl` or `wget`, and internet the first time only).
- `NO_NETTY=1 sh build.sh` builds without Netty. Only the socket transport is compiled and the library has no dependencies.

`target/netty.cp` lists the Netty jars used by the build, so a server build can bundle them.

**With Maven:**

```sh
mvn package      # builds target/RakNetLib.jar
mvn install      # also installs raknetlib:RakNetLib:1.7.0-beta in your local Maven repository
```

Then add the `<dependency>` shown in Option 1 to your project.

### About Netty

**Netty is installed automatically when you compile** (Option 2). You do not download it yourself:

- `sh build.sh` downloads the Netty jars (4.2.18.Final) from Maven Central into `~/.cache/raknetlib` the first time, and compiles against them.
- `mvn package` / `mvn install` downloads Netty from the `pom.xml` the same way.

If you have no internet, use `NO_NETTY=1 sh build.sh`. RakNetLib still works: it falls back to plain Java sockets with the same API.

Netty is not packed inside `RakNetLib.jar`. When you run your own program with the jar, add the Netty jars to your classpath if you want the Netty transport. `build.sh` leaves the list of the jars it used in `target/netty.cp`. If they are missing, the library uses sockets.

See [Transports](#transports) for how the library chooses between them.

## Core concepts

### Packet format at the API boundary

The library uses the same convention on both sides, but the direction matters:

| Direction | You give / receive | Format |
|---|---|---|
| **Send** (server `Session.sendGame`, client `sendGame`) | You give | `ID + body` (**no** `0xFE`) |
| **Receive on the server** (`onPacket`) | You get | `0xFE + ID + body`, one packet per call (batches are unwrapped for you) |
| **Receive on the client** (`onPacket`) | You get | The raw payload. Game data arrives as a **batch** `0xFE 0x06 ...`. Unwrap it with `GameBatch.decode` (see [Client guide](#client-guide)) |

### Reliability

Every send takes a reliability mode. Game packets default to `RELIABLE_ORDERED` on channel `0`.

| Constant | Value | Meaning |
|---|---|---|
| `UNRELIABLE` | 0 | May be lost, may arrive out of order |
| `UNRELIABLE_SEQUENCED` | 1 | May be lost, old packets are dropped |
| `RELIABLE` | 2 | Always arrives, any order |
| `RELIABLE_ORDERED` | 3 | Always arrives, in order (default for game packets) |
| `RELIABLE_SEQUENCED` | 4 | Always arrives, only the newest in the sequence is delivered |
| `UNRELIABLE_WITH_ACK_RECEIPT` | 5 | Like 0, and you get a receipt via `onAckReceipt` |
| `RELIABLE_WITH_ACK_RECEIPT` | 6 | Like 2, with receipt |
| `RELIABLE_ORDERED_WITH_ACK_RECEIPT` | 7 | Like 3, with receipt |

Channels go from `0` to `RakNet.ORDER_CHANNELS - 1` (32 channels). Ordering is kept per channel.

### Batches and compression

- `sendGame(packet, ...)` wraps your packet in a `0xFE 0x06` batch, compressed with zlib (level 6). Each send is a batch of one packet.
- **Identity cache:** payloads of 4 KB or more are compressed once and reused for every recipient (LRU, 256 entries).
  **Do not modify a large array after sending it**: the next send would carry the old content.
- `GameBatch.precomprimir(packet)` compresses ahead of time so the first send does not pay for it (useful at startup, e.g. for chunks).
- Incoming batches are decompressed up to `GameBatch.MAX_INBOUND` (8 MB). A corrupt batch is dropped.

## Server guide

### 1. Minimal server

An echo server: it sends back every game packet it receives.

```java
import java.util.Arrays;
import raknetlib.RakNetServer;
import raknetlib.Session;

public class EchoServer {
    public static void main(String[] args) throws Exception {
        RakNetServer server = new RakNetServer(19132);

        server.setListener(new RakNetServer.Listener() {
            public void onOpen(Session s) {
                System.out.println("Connected: " + s.key);
            }
            public void onPacket(Session s, byte[] buf) {
                // buf = 0xFE + ID + body   ->   sendGame wants ID + body
                s.sendGame(Arrays.copyOfRange(buf, 1, buf.length), false);
            }
            public void onClose(Session s, String reason) {
                System.out.println("Closed: " + s.key + " (" + reason + ")");
            }
        });

        server.bind();   // opens the UDP port (SocketException if it is in use)
        server.run();    // blocks this thread until server.stop()
    }
}
```

### 2. Handling game packets

The first byte after `0xFE` is the packet ID. Dispatch on it:

```java
public void onPacket(Session s, byte[] buf) {
    int id = buf[1] & 0xff;
    switch (id) {
        case 0x82: handleLogin(s, buf);  break;   // your game logic
        case 0x84: handleMove(s, buf);   break;
        default:   break;
    }
}
```

Use `BinaryStream` to read and write fields:

```java
import raknetlib.BinaryStream;

// Read
BinaryStream in = new BinaryStream(buf);
in.getByte();                 // 0xFE
int id = in.getByte() & 0xff; // packet ID
int value = in.getInt();

// Write (ID + body, no 0xFE)
byte[] out = new BinaryStream()
        .putByte(0x80)
        .putInt(1234)
        .putString("hello")
        .toByteArray();
s.sendGame(out, false);
```

> Note: `BinaryStream` is big-endian by default. The `L` methods (`putLInt`, `getLShort`, ...) are little-endian.

### 3. Sending

```java
// Default: RELIABLE_ORDERED, channel 0
s.sendGame(packet, false);

// immediate = true closes the current datagram and sends now, instead of waiting to pack more frames into it
s.sendGame(packet, true);

// Custom reliability and channel. Returns a receipt id for modes 5, 6 and 7 (or -1).
int receipt = s.sendGame(packet, RakNet.RELIABLE_ORDERED_WITH_ACK_RECEIPT, 1, false);

// Raw RakNet payload, with no batch and no 0xFE
s.sendEncapsulated(rawPayload, RakNet.UNRELIABLE, 0, false);
```

Receipts arrive in the listener:

```java
public void onAckReceipt(Session s, int receiptId, boolean acked) {
    // acked == false only for the unreliable receipt variant, when the datagram was lost
}
```

Broadcast to everyone:

```java
for (Session s : server.getSessions()) {
    s.sendGame(packet, false);
}
```

### 4. Per-session data

Attach your own object (a player, for example) to a session:

```java
public void onOpen(Session s) {
    s.setAttachment(new Player(s));
}
public void onPacket(Session s, byte[] buf) {
    Player p = (Player) s.getAttachment();
    p.handle(buf);
}
```

### 5. Kicking, closing and banning

```java
s.kick("Server is full");              // disconnects and tells the client why
s.close("Timed out", false);           // closes without notifying the client

server.ban(ip, 60_000);                // ban an InetAddress for 60 s
server.unban(ip);
server.isBanned(ip);
```

### 6. MOTD and Query

The MOTD is what the game's server list shows. The default is `MCPE;Java Server;84;0.15.10;0;20`.

```java
server.setMotd(() -> "MCPE;My Server;84;0.15.10;" + server.getSessions().size() + ";100");
```

The supplier is called on the network thread, so keep it fast.

To answer Query packets (`0xFE 0xFD`):

```java
server.setQueryHandler((InetSocketAddress from, byte[] packet) -> {
    return buildQueryReply(packet);   // return the reply bytes, or null to ignore
});
```

### 7. Game tick

`setTicker` runs a task 20 times per second on **its own game thread**, so a heavy tick does not stall the network:

```java
server.setTicker(() -> world.tick());
```

### 8. Limits and protection

```java
server.setMaxConnections(100);        // 0 = no limit (default)
server.setMaxConnectionsPerIp(3);
server.setMaxHalfOpen(512);           // handshakes in progress (512 by default)
server.setRateLimit(2000, 5000);      // datagrams/s per IP, block time in ms. 0 turns it off
server.setStrict(true);               // malformed packet from a connected player => disconnect (default)
server.setKickOnListenerError(true);  // if your Listener throws, the session is disconnected (default)
server.setAcceptedProtocols(8);       // accepted RakNet protocol versions
```

### 9. Logging

```java
server.setLogger(msg -> System.out.println("[raknet] " + msg));
server.setErrorLogger(msg -> System.err.println("[raknet] " + msg));
```

### 10. Stopping

`stop()` is safe from any thread. It sends a disconnect to every session, closes the sessions and the port, and makes `run()` return.

```java
Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
```

## Client guide

`RakNetClient` does the full handshake and uses the same reliability layer as the server. It is good for **bots, tools and server tests**.

> The client uses plain Java sockets (see [Pending](#pending)).

### 1. Ping a server (no connection)

```java
InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 19132);
String motd = RakNetClient.ping(addr, 1000);   // null if no answer within 1000 ms
System.out.println(motd);                      // e.g. MCPE;My Server;84;0.15.10;0;20
```

### 2. Connect and talk

```java
import java.net.InetSocketAddress;
import raknetlib.GameBatch;
import raknetlib.RakNetClient;

public class Bot {
    public static void main(String[] args) throws Exception {
        RakNetClient client = new RakNetClient(new InetSocketAddress("127.0.0.1", 19132));

        client.setListener(new RakNetClient.Listener() {
            public void onConnected() {
                System.out.println("Connected, MTU " + client.getMtu());
                // ID + body, no 0xFE. The client adds 0xFE and sends it RELIABLE_ORDERED on channel 0.
                client.sendGame(new byte[] { (byte) 0x82, 1, 2, 3 });
            }

            public void onPacket(byte[] buf) {
                try {
                    // The server sends batches: 0xFE 0x06 + zlib data. Unwrap them.
                    if ((buf[0] & 0xff) == 0xfe && (buf[1] & 0xff) == GameBatch.BATCH_ID) {
                        for (byte[] pk : GameBatch.decode(buf)) {   // each pk = ID + body (no 0xFE)
                            System.out.println("Packet id=" + (pk[0] & 0xff) + " len=" + pk.length);
                        }
                    }
                } catch (java.util.zip.DataFormatException e) {
                    System.err.println("Corrupt batch");
                }
            }

            public void onDisconnected(String reason) {
                System.out.println("Disconnected: " + reason);
            }
        });

        client.start();        // runs on its own daemon thread and returns the Thread
        Thread.sleep(10_000);  // keep your program alive (the client thread is a daemon)
        client.disconnect();
    }
}
```

Use `client.run()` instead of `start()` if you want it to block the calling thread until the connection ends.

### 3. Sending from the client

```java
client.sendGame(idAndBody);                                   // 0xFE added, RELIABLE_ORDERED, channel 0
client.send(rawPayload, RakNet.RELIABLE_ORDERED, 0);          // raw payload, exactly as you give it
```

Both are safe to call from any thread. If the client is not connected yet, the send is ignored, so wait for `onConnected()`.

### 4. State and utilities

```java
client.isConnected();
client.getPing();    // ms, -1 until measured
client.getMtu();
client.getGuid();    // random client id
client.execute(() -> { /* runs on the client thread */ });
```

### 5. Client behaviour you should know

- **No batch on send.** `sendGame` sends the packet with `0xFE` but **without** a zlib batch. The server accepts that.
- **No batch unwrapping on receive.** You get the raw `0xFE 0x06 ...` payload. Use `GameBatch.decode`.
- The connection times out after 10 s without traffic, and `onDisconnected` is called with the reason.
- MTU negotiation tries 1464, 1200 and 576 bytes in order.

## API reference

### RakNetServer

Constructor: `new RakNetServer(int port)`.

| Method | What it does |
|---|---|
| `bind()` | Opens the UDP port (Netty or sockets, see [Transports](#transports)). Throws `SocketException` if the port is in use. |
| `run()` | Runs the network loop until `stop()`. Blocks the calling thread. |
| `stop()` | Stops the loop, closes the sessions (notifying their clients) and the port. Safe from any thread. |
| `getPort()` | UDP port. |
| `getTransportName()` | `"netty"`, `"socket"`, or `"none"` before `bind()`. |
| `setListener(Listener)` | Receives the session events. |
| `setTicker(Runnable)` | Runs 20 times per second, on the game thread. |
| `execute(Runnable)` | Runs on the network thread. Safe from any thread. |
| `setMotd(Supplier<String>)` | Text returned to pings. Called on the network thread. |
| `setQueryHandler(BiFunction<InetSocketAddress, byte[], byte[]>)` | Answers Query packets (`0xFE 0xFD`). Returns the reply, or `null`. |
| `getSessions()`, `getSession(InetSocketAddress)` | Connected sessions. |
| `ban(InetAddress, long millis)`, `unban(InetAddress)`, `isBanned(InetAddress)` | IP bans. |
| `setMaxConnections(int)` | Maximum connections. `0` = no limit (default). |
| `setMaxConnectionsPerIp(int)` | Maximum connections per IP. |
| `setMaxHalfOpen(int)` | Half-open handshakes allowed (512 by default). |
| `setRateLimit(int perSecond, long blockMs)` | Datagrams per second per IP (2000 by default; `0` turns the limit off). An IP over the limit is blocked for `blockMs` (5000 by default). |
| `setStrict(boolean)` / `isStrict()` | On by default. A malformed packet from a connected player disconnects that player. |
| `setKickOnListenerError(boolean)` / `isKickOnListenerError()` | On by default. If your `Listener` throws, the session is disconnected. |
| `setAcceptedProtocols(int...)` | Accepted RakNet protocol versions. |
| `send(InetSocketAddress, byte[])` | Sends a raw datagram. |
| `setLogger(Consumer<String>)`, `setErrorLogger(Consumer<String>)` | Where the logs go. |
| `log(String)`, `logError(String)`, `logLimited(String)` | Write to those logs. `logLimited` avoids flooding the log. |
| `getRakNetTimeMS()` | RakNet clock, in ms. |
| `static keyOf(InetSocketAddress)` | `ip:port` key for an address. |

Internal, do not call them yourself: `sessionOpened`, `sessionPacket`, `sessionReceipt`, `sessionClosed`. `Session` calls them.

### RakNetServer.Listener

```java
void onOpen(Session s);                  // session connected
void onPacket(Session s, byte[] buf);    // game packet: 0xFE + ID + body
void onClose(Session s, String reason);  // session closed
default void onAckReceipt(Session s, int receiptId, boolean acked) { }  // send with reliability 5, 6 or 7
```

### Session

Data (read only):

- `address` (`InetSocketAddress`) and `key` (`"ip:port"`).
- `getState()`: `Session.UNCONNECTED`, `CONNECTING_1`, `CONNECTING_2` or `CONNECTED`.
- `getMtu()`, `getClientId()`, `getConnectedAt()` (ms since the epoch).
- `getPing()`: ms. `-1` until the first reply, and again after 5 s without one.
- `getAckRtt()`, `getIdleMs()`, `getBytesIn()`, `getBytesOut()`, `getLostDatagrams()`, `getBacklog()`, `isClosed()`.
- `getAttachment()` / `setAttachment(Object)`: your own data per session.

Sending:

- `sendGame(byte[] packetWithId, boolean immediate)`: game packet, batched automatically.
- `sendGame(byte[] packetWithId, int reliability, int channel, boolean immediate)`: same, with reliability and channel. Returns the receipt id (or `-1`).
- `sendEncapsulated(byte[] payload, int reliability, int channel, boolean immediate)`: sent as is, without batch or `0xFE`. For raw RakNet packets. Returns the receipt id.

Closing:

- `kick(String reason)`: disconnects and tells the client why.
- `close(String reason, boolean notifyClient)`.

Timeouts: `Session.TIMEOUT_MS` (10 s without traffic) and `Session.HANDSHAKE_TIMEOUT_MS` (8 s).

### RakNetClient

| Member | What it does |
|---|---|
| `new RakNetClient(InetSocketAddress server)` | Creates a client. Nothing happens until `start()` or `run()`. |
| `setListener(Listener)` | Receives the connection events. |
| `start()` | Connects on a new daemon thread and returns the `Thread`. Throws `SocketException`. |
| `run()` | Connects and blocks until disconnected. |
| `disconnect()` | Sends the disconnect packets and stops the client. |
| `isConnected()` | `true` after the handshake finishes. |
| `getPing()`, `getMtu()`, `getGuid()` | Latency in ms, negotiated MTU, client GUID. |
| `sendGame(byte[] packetWithId)` | Sends `0xFE + packet`, `RELIABLE_ORDERED`, channel 0, without batch. |
| `send(byte[] payload, int reliability, int channel)` | Sends a raw payload. |
| `execute(Runnable)` | Runs on the client thread. |
| `static ping(InetSocketAddress, int timeoutMs)` | Returns the server's MOTD, or `null` if it does not answer. |

`RakNetClient.Listener`:

```java
void onConnected();
void onPacket(byte[] buffer);                 // raw payload; game data comes as a 0xFE 0x06 batch
void onDisconnected(String reason);
default void onAckReceipt(int receiptId, boolean acked) { }
```

### GameBatch (static)

- `BATCH_ID` (`0x06`), `LEVEL` (`6`), `MAX_INBOUND` (8 MB).
- `decode(byte[] wire)`: unwraps a batch (`0xFE 0x06 ...`) and returns its packets as `ID + body`. Throws `DataFormatException` if it is corrupt.
- `precomprimir(byte[] packetWithId)`: compresses ahead of time and stores the result in the cache.
- `encode(Deflater d, byte[] packetWithId)`: used by `Session` internally. You rarely need it.

### Zlib (static)

- `deflate(byte[] data, int level)`.
- `inflate(byte[] data, int maxSize)`: tries zlib and, if that fails, raw deflate. Throws `DataFormatException`.

### BinaryStream

- Constructors: `new BinaryStream()`, `new BinaryStream(byte[])`, `new BinaryStream(byte[], int off, int n)`.
- Writing: `putByte`, `putShort`, `putInt`, `putLong`, `putFloat`, `putDouble`, `putBytes`, `putString`, `putByteString`, `putAddress`, `putLTriad`.
- Reading: `getByte`, `getShort`, `getInt`, `getLong`, `getFloat`, `getDouble`, `getBytes(n)`, `getString`, `getByteString`, `getAddress`, `getLTriad`.
- Position: `eof()`, `remaining()`, `position()`, `length()`, `toByteArray()`.
- Big-endian by default. The `L` methods (`putLShort`, `putLInt`, `putLLong`, `putLFloat`, `putLTriad`, `getLShort`, `getLInt`, `getLTriad`) are little-endian.

### RakNet (constants and utilities)

- `PROTOCOL` (8) and `MAGIC`.
- Packet IDs: `UNCONNECTED_PING`, `OPEN_CONNECTION_REQUEST_1`, ..., `ACK` (`0xC0`), `NACK` (`0xA0`).
- Reliability: `UNRELIABLE` (0), `UNRELIABLE_SEQUENCED` (1), `RELIABLE` (2), `RELIABLE_ORDERED` (3), `RELIABLE_SEQUENCED` (4), and the receipt variants (5, 6, 7).
- `MIN_MTU` (400), `MAX_MTU` (1432), `ORDER_CHANNELS` (32).
- `nowMs()`: RakNet's monotonic clock, in ms.

## Transports

`RakNetServer` picks the UDP transport when you call `bind()`. The public API is the same with both.

| Transport | When it is used | Notes |
|---|---|---|
| Netty 4.2 | Netty is on the classpath (default when it is there) | Event loop, pooled buffers, write watermarks. |
| Java sockets | Netty is missing, or `-Draknet.transport=socket` | No dependencies. |

Force one with `-Draknet.transport=auto|netty|socket` (default `auto`). With `netty`, `bind()` throws a `SocketException` if
Netty is not available, instead of falling back. `getTransportName()` tells you which one is in use.

What Netty changes:

- **One thread for the UDP channel**, named `RakNet-red`. Reception, sends, `execute()` tasks and the session tick all run there.
- **No polling.** The thread sleeps in the selector until a datagram arrives or the tick is due (sockets wake up every 2 ms).
- **Cheaper sends** from pooled direct buffers.
- **Backpressure.** If the outbound buffer goes above 512 KB, new datagrams are dropped instead of piling up in memory. RakNet resends reliable frames on its own, and the drops are logged.
- **Clean shutdown.** `stop()` sends the disconnect packets to every session before it closes the channel.

## Threading model

- `run()` blocks the calling thread until `stop()`. The network thread handles datagrams, ACKs, resends, handshakes and timeouts.
  With Netty it is the thread `RakNet-red`; with sockets, it is the thread that called `run()`.
- `setTicker(r)`: `r` runs 20 times per second on its own thread (the game thread), started by `run()`.
- `execute(r)`: runs `r` on the network thread. Safe from any thread.
- The `Listener` callbacks (`onOpen`, `onPacket`, `onClose`, `onAckReceipt`) run on the network thread. **Keep them short**:
  if your game logic is heavy, hand the packet to your own thread.
- `Session` methods are safe from any thread: each session has its own monitor (`synchronized`).
- `RakNetClient` send methods are safe from any thread. Its listener runs on the client thread.

## Troubleshooting

| Problem | Likely cause |
|---|---|
| `SocketException` on `bind()` | The UDP port is already in use, or `-Draknet.transport=netty` was forced without Netty. |
| Client never calls `onConnected` | Wrong IP/port, firewall blocking UDP, or the server only accepts a different RakNet protocol (`setAcceptedProtocols`). |
| Client program exits right away | `start()` uses a daemon thread. Keep `main` alive, or use `run()`. |
| Client `onPacket` gets unreadable bytes | The data is a `0xFE 0x06` batch. Unwrap it with `GameBatch.decode`. |
| Players are disconnected for "Invalid packet" | `setStrict(true)` (default) and the client sent a malformed packet. |
| Players are kicked when your code throws | `setKickOnListenerError(true)` (default). Catch your own exceptions, or turn it off. |
| A large packet shows old content | You modified the array after sending it. Create a new array for each distinct payload. |
| Packets dropped under heavy load (Netty) | The outbound buffer passed 512 KB. Send less per tick, or spread the sends out. |

## Pending

- The client does not compress its batches yet.
- The client still uses plain sockets. A Netty client would help if you run many bots at once.
