package com.ericguerra.ringkv.server;

import com.ericguerra.ringkv.node.Node;
import com.ericguerra.ringkv.protocol.Codec;
import com.ericguerra.ringkv.protocol.Message;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * TCP server speaking the line protocol. Responses on a connection are written in request
 * order even though requests complete asynchronously, by chaining each reply on the previous one.
 */
public final class KvServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KvServer.class);

    private final Node node;
    private final EventLoopGroup boss = new NioEventLoopGroup(1);
    private final EventLoopGroup workers = new NioEventLoopGroup();
    private Channel serverChannel;

    public KvServer(Node node) {
        this.node = node;
    }

    public void start(int port) throws InterruptedException {
        serverChannel = new ServerBootstrap()
                .group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new LineBasedFrameDecoder(16 * 1024 * 1024))
                                .addLast(new StringDecoder(StandardCharsets.UTF_8))
                                .addLast(new StringEncoder(StandardCharsets.UTF_8))
                                .addLast(new RequestHandler());
                    }
                })
                .bind(port).sync().channel();
        log.info("{} listening on :{}", node.self(), port);
    }

    private final class RequestHandler extends SimpleChannelInboundHandler<String> {
        private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String line) {
            if (line.isBlank()) return;
            CompletableFuture<Message> reply;
            try {
                reply = node.handle(Codec.decode(line));
            } catch (Codec.ProtocolException e) {
                reply = CompletableFuture.completedFuture(new Message.Error("PROTOCOL", e.getMessage()));
            }
            CompletableFuture<Message> safe = reply.exceptionally(t -> new Message.Error("INTERNAL", String.valueOf(t.getMessage())));
            tail = tail.thenCombine(safe, (ignored, msg) -> msg)
                    .thenAccept(msg -> ctx.writeAndFlush(Codec.encode(msg) + "\n"));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause instanceof TooLongFrameException) {
                ctx.writeAndFlush("ERR PROTOCOL line too long\n");
            } else {
                log.warn("connection error: {}", cause.toString());
            }
            ctx.close();
        }
    }

    @Override
    public void close() {
        if (serverChannel != null) serverChannel.close().syncUninterruptibly();
        boss.shutdownGracefully();
        workers.shutdownGracefully();
    }
}
