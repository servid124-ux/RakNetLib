package raknetlib;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class NettyTransport implements Transport {
    private static final long TICK_MS = 10;
    private static final int RECV_BUF = 2048;
    private static final int SOCKET_BUF = 1 << 20;
    private static final int WM_LOW = 32 * 1024, WM_HIGH = 512 * 1024;

    private final RakNetServer server;
    private final int port;
    private final CountDownLatch closed = new CountDownLatch(1);
    private final AtomicBoolean closing = new AtomicBoolean();
    private final AtomicLong dropped = new AtomicLong();
    private volatile EventLoopGroup group;
    private volatile Channel channel;
    private volatile EventLoop loop;
    private volatile ScheduledFuture<?> tickTask;

    NettyTransport(RakNetServer server, int port) {
        this.server = server;
        this.port = port;
    }

    @Override public String name() { return "netty"; }

    @Override public void open() throws SocketException {

        EventLoopGroup g = new MultiThreadIoEventLoopGroup(1, new DefaultThreadFactory("RakNet-red", true), NioIoHandler.newFactory());
        group = g;
        try {
            ChannelFuture f = new Bootstrap()
                .group(g)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(RECV_BUF))
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(WM_LOW, WM_HIGH))
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override protected void initChannel(DatagramChannel ch) {
                        ch.pipeline().addLast("raknet", new Datagrams());
                    }
                })
                .bind(new InetSocketAddress(port))
                .awaitUninterruptibly();
            if (!f.isSuccess()) {
                Throwable cause = f.cause();
                if (cause instanceof SocketException) throw (SocketException) cause;
                throw new SocketException("No pude abrir el puerto UDP " + port + ": " + cause);
            }
            DatagramChannel ch = (DatagramChannel) f.channel();
            try {
                ch.config().setReceiveBufferSize(SOCKET_BUF);
                ch.config().setSendBufferSize(SOCKET_BUF);
            } catch (RuntimeException ignored) { }
            channel = ch;
            loop = ch.eventLoop();
            tickTask = loop.scheduleWithFixedDelay(this::onTick, TICK_MS, TICK_MS, TimeUnit.MILLISECONDS);
            ch.closeFuture().addListener(done -> onClosed());
        } catch (SocketException | RuntimeException e) {
            Channel c = channel;
            if (c != null) c.close();
            g.shutdownGracefully(0, 1, TimeUnit.SECONDS);
            throw e;
        }
    }

    private void onTick() {
        try {
            server.netTick(RakNet.nowMs());
        } catch (Throwable t) {
            server.logError("Error in network tick: " + RakNet.describe(t));
        }
    }

    @Override public void run() {
        boolean interrupted = false;
        while (true) {
            try {
                closed.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    @Override public void stop() {
        if (!closing.compareAndSet(false, true)) return;
        Channel ch = channel;
        EventLoop lp = loop;
        if (ch == null || lp == null) {
            closed.countDown();
            return;
        }
        try {

            lp.execute(() -> {
                try {
                    server.closeAllSessions("Server closed");
                } catch (RuntimeException e) {
                    server.logError("Error closing sessions: " + RakNet.describe(e));
                } finally {
                    ch.close();
                }
            });
        } catch (RejectedExecutionException e) {
            ch.close();
        }
    }

    private void onClosed() {
        try {
            ScheduledFuture<?> t = tickTask;
            if (t != null) t.cancel(false);
            server.closeAllSessions("Server closed");
        } catch (RuntimeException e) {
            server.logError("Error closing sessions: " + RakNet.describe(e));
        } finally {
            EventLoopGroup g = group;
            if (g != null) g.shutdownGracefully(0, 1, TimeUnit.SECONDS);
            closed.countDown();
        }
    }

    @Override public void send(InetSocketAddress to, byte[] data) {
        Channel ch = channel;
        if (ch == null || !ch.isOpen()) return;
        if (!ch.isWritable()) {
            long n = dropped.incrementAndGet();
            if (n == 1 || n % 1000 == 0) server.logLimited("Outbound buffer full, datagrams dropped so far: " + n);
            return;
        }
        ByteBuf buf = ch.alloc().directBuffer(data.length).writeBytes(data);
        ch.writeAndFlush(new DatagramPacket(buf, to), ch.voidPromise());
    }

    @Override public void wakeup() {
        EventLoop lp = loop;
        if (lp == null) return;
        try {
            lp.execute(server::drainTasks);
        } catch (RejectedExecutionException ignored) { }
    }

    private final class Datagrams extends SimpleChannelInboundHandler<DatagramPacket> {
        @Override protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
            ByteBuf in = packet.content();
            int n = in.readableBytes();
            if (n <= 0) return;
            byte[] data = new byte[n];
            in.getBytes(in.readerIndex(), data);
            server.onDatagram(packet.sender(), data);
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            server.logLimited("Netty UDP error: " + RakNet.describe(cause));
        }
    }
}
