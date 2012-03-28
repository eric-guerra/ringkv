package com.ericguerra.ringkv.cluster;

import com.ericguerra.ringkv.protocol.Message;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * In-process transport for tests and simulations. Nodes register a handler; links can be
 * partitioned to simulate failures. Messages pass through the text codec, so the protocol
 * is exercised end to end.
 */
public final class LocalTransport implements Transport {
    private final Map<NodeId, Function<Message, Message>> handlers = new ConcurrentHashMap<>();
    private final Set<NodeId> down = ConcurrentHashMap.newKeySet();

    public void register(NodeId node, Function<Message, Message> handler) {
        handlers.put(node, handler);
    }

    public void setDown(NodeId node, boolean isDown) {
        if (isDown) down.add(node); else down.remove(node);
    }

    @Override
    public CompletableFuture<Message> send(NodeId target, Message request) {
        if (down.contains(target)) {
            return CompletableFuture.failedFuture(new IOException(target + " unreachable"));
        }
        Function<Message, Message> handler = handlers.get(target);
        if (handler == null) return CompletableFuture.failedFuture(new IOException("no such node " + target));
        return CompletableFuture.supplyAsync(() -> {
            var wire = com.ericguerra.ringkv.protocol.Codec.decode(com.ericguerra.ringkv.protocol.Codec.encode(request));
            var reply = handler.apply(wire);
            return com.ericguerra.ringkv.protocol.Codec.decode(com.ericguerra.ringkv.protocol.Codec.encode(reply));
        });
    }
}
