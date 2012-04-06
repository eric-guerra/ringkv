package com.ericguerra.ringkv.cluster;

import com.ericguerra.ringkv.protocol.Codec;
import com.ericguerra.ringkv.protocol.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.AttributeKey;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Netty client transport. One pooled connection per peer; since the protocol is strictly
 * request/response in order on a connection, pending futures are matched FIFO.
 */
public final class NettyTransport implements Transport {
    private static final AttributeKey<Queue<CompletableFuture<Message>>> PENDING = AttributeKey.valueOf("pending");

    private final EventLoopGroup group = new NioEventLoopGroup(2);
    private final Bootstrap bootstrap;
    private final Map<NodeId, CompletableFuture<Channel>> channels = new ConcurrentHashMap<>();
    private final long timeoutMs;

    public NettyTransport(long timeoutMs) {
        this.timeoutMs = timeoutMs;
        this.bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) timeoutMs)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.attr(PENDING).set(new ArrayDeque<>());
                        ch.pipeline()
                                .addLast(new LineBasedFrameDecoder(16 * 1024 * 1024))
                                .addLast(new StringDecoder(StandardCharsets.UTF_8))
                                .addLast(new StringEncoder(StandardCharsets.UTF_8))
                                .addLast(new ResponseHandler());
                    }
                });
    }

    @Override
    public CompletableFuture<Message> send(NodeId target, Message request) {
        return channel(target).thenCompose(ch -> {
            CompletableFuture<Message> reply = new CompletableFuture<>();
            // Enqueue and write on the event loop so ordering of queue and wire match.
            ch.eventLoop().execute(() -> {
                ch.attr(PENDING).get().add(reply);
                ch.writeAndFlush(Codec.encode(request) + "\n").addListener(f -> {
                    if (!f.isSuccess()) reply.completeExceptionally(f.cause());
                });
            });
            return reply.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
        });
    }

    private CompletableFuture<Channel> channel(NodeId target) {
        CompletableFuture<Channel> existing = channels.get(target);
        if (existing != null && (!existing.isDone() || (!existing.isCompletedExceptionally() && existing.join().isActive()))) {
            return existing;
        }
        CompletableFuture<Channel> fresh = new CompletableFuture<>();
        channels.put(target, fresh);
        ChannelFuture cf = bootstrap.connect(target.host(), target.port());
        cf.addListener(f -> {
            if (f.isSuccess()) {
                Channel ch = cf.channel();
                ch.closeFuture().addListener(x -> {
                    channels.remove(target, fresh);
                    failAll(ch, new IOException("connection to " + target + " closed"));
                });
                fresh.complete(ch);
            } else {
                channels.remove(target, fresh);
                fresh.completeExceptionally(f.cause());
            }
        });
        return fresh;
    }

    private static void failAll(Channel ch, Throwable t) {
        Queue<CompletableFuture<Message>> q = ch.attr(PENDING).get();
        CompletableFuture<Message> f;
        while ((f = q.poll()) != null) f.completeExceptionally(t);
    }

    private static final class ResponseHandler extends SimpleChannelInboundHandler<String> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String line) {
            CompletableFuture<Message> f = ctx.channel().attr(PENDING).get().poll();
            if (f == null) return; // unsolicited line; ignore
            try {
                f.complete(Codec.decode(line));
            } catch (RuntimeException e) {
                f.completeExceptionally(e);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            failAll(ctx.channel(), cause);
            ctx.close();
        }
    }

    @Override
    public void close() {
        channels.values().forEach(f -> f.thenAccept(Channel::close));
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
}
