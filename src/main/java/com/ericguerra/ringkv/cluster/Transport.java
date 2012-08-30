package com.ericguerra.ringkv.cluster;

import com.ericguerra.ringkv.protocol.Message;

import java.util.concurrent.CompletableFuture;

/** Sends a request to a peer and completes with its response (or exceptionally on failure/timeout). */
public interface Transport extends AutoCloseable {
    CompletableFuture<Message> send(NodeId target, Message request);

    @Override
    default void close() {}
}
