package com.ericguerra.ringkv.cluster;

import com.ericguerra.ringkv.protocol.Message;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Heartbeat failure detector: pings every peer periodically and marks a peer suspect when
 * no successful reply has been seen within {@code suspectAfterMs}. The coordinator skips
 * suspect nodes when choosing replicas, falling back to the next healthy node on the ring.
 */
public final class FailureDetector implements AutoCloseable {
    private final NodeId self;
    private final Set<NodeId> peers;
    private final Transport transport;
    private final long suspectAfterMs;
    private final LongSupplier clock;
    private final Map<NodeId, Long> lastSeen = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler;

    public FailureDetector(NodeId self, Set<NodeId> peers, Transport transport, long suspectAfterMs, LongSupplier clock) {
        this.self = self;
        this.peers = peers.stream().filter(p -> !p.equals(self)).collect(Collectors.toUnmodifiableSet());
        this.transport = transport;
        this.suspectAfterMs = suspectAfterMs;
        this.clock = clock;
        long now = clock.getAsLong();
        this.peers.forEach(p -> lastSeen.put(p, now)); // optimistic start
    }

    public void start(long intervalMs) {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "failure-detector");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::probeAll, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void probeAll() {
        for (NodeId peer : peers) {
            transport.send(peer, new Message.Ping(self.id())).thenAccept(reply -> {
                if (reply instanceof Message.Pong) heard(peer);
            });
        }
    }

    /** Any successful exchange counts as a heartbeat. */
    public void heard(NodeId peer) {
        lastSeen.put(peer, clock.getAsLong());
    }

    public boolean isAlive(NodeId node) {
        if (node.equals(self)) return true;
        Long seen = lastSeen.get(node);
        return seen != null && clock.getAsLong() - seen <= suspectAfterMs;
    }

    public Set<NodeId> suspects() {
        return peers.stream().filter(p -> !isAlive(p)).collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public void close() {
        if (scheduler != null) scheduler.shutdownNow();
    }
}
