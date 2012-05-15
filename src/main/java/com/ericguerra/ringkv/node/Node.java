package com.ericguerra.ringkv.node;

import com.ericguerra.ringkv.cluster.FailureDetector;
import com.ericguerra.ringkv.cluster.NodeId;
import com.ericguerra.ringkv.cluster.Transport;
import com.ericguerra.ringkv.protocol.Message;
import com.ericguerra.ringkv.ring.ConsistentHashRing;
import com.ericguerra.ringkv.storage.Storage;
import com.ericguerra.ringkv.storage.Versioned;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A cluster member. It serves replica requests against local storage and coordinates
 * client requests: it picks the key's healthy replicas from the ring, fans out, and
 * completes once a quorum agrees. Reads repair stale replicas in the background.
 */
public final class Node {
    private final NodeId self;
    private final ConsistentHashRing ring;
    private final Storage storage;
    private final Transport transport;
    private final FailureDetector detector;
    private final QuorumConfig quorum;
    private final HybridClock clock;
    private final NodeMetrics metrics = new NodeMetrics();

    public Node(NodeId self, ConsistentHashRing ring, Storage storage, Transport transport,
                FailureDetector detector, QuorumConfig quorum, HybridClock clock) {
        this.self = self;
        this.ring = ring;
        this.storage = storage;
        this.transport = transport;
        this.detector = detector;
        this.quorum = quorum;
        this.clock = clock;
    }

    /** Entry point for everything that arrives on the wire. */
    public CompletableFuture<Message> handle(Message request) {
        return switch (request) {
            case Message.Get g -> get(g.key());
            case Message.Put p -> write(p.key(), p.value());
            case Message.Delete d -> write(d.key(), null);
            case Message.ReplicaGet g -> CompletableFuture.completedFuture(localRead(g.key()));
            case Message.ReplicaPut p -> CompletableFuture.completedFuture(localWrite(p));
            case Message.Ping ping -> CompletableFuture.completedFuture(new Message.Pong(self.id()));
            default -> CompletableFuture.completedFuture(new Message.Error("BAD_REQUEST", "unexpected " + request.getClass().getSimpleName()));
        };
    }

    // ---------------- replica side ----------------

    Message localRead(String key) {
        return storage.get(key)
                .<Message>map(v -> new Message.Value(true, v.value(), v.timestamp(), v.writer()))
                .orElse(new Message.Value(false, null, 0, ""));
    }

    Message localWrite(Message.ReplicaPut p) {
        clock.observe(p.timestamp());
        Versioned v = p.value() == null ? Versioned.tombstone(p.timestamp(), p.writer()) : Versioned.of(p.value(), p.timestamp(), p.writer());
        storage.putIfNewer(p.key(), v);
        return new Message.Ok(); // stale writes are acknowledged: the replica already holds something newer
    }

    // ---------------- coordinator side ----------------

    /** Healthy replicas for a key: walk the ring past suspect nodes (sloppy quorum). */
    List<NodeId> replicasFor(String key) {
        List<NodeId> candidates = ring.preferenceList(key, ring.members().size());
        List<NodeId> healthy = new ArrayList<>(quorum.n());
        for (NodeId n : candidates) {
            if (detector.isAlive(n)) healthy.add(n);
            if (healthy.size() == quorum.n()) break;
        }
        return healthy;
    }

    private CompletableFuture<Message> send(NodeId target, Message m) {
        if (target.equals(self)) return handle(m);
        return transport.send(target, m).whenComplete((r, err) -> {
            if (err == null) detector.heard(target);
        });
    }

    CompletableFuture<Message> write(String key, byte[] value) {
        long ts = clock.now();
        Message.ReplicaPut put = new Message.ReplicaPut(key, value, ts, self.id());
        List<NodeId> replicas = replicasFor(key);
        if (replicas.size() < quorum.w()) {
            metrics.unavailable.incrementAndGet();
            return CompletableFuture.completedFuture(new Message.Error("UNAVAILABLE",
                    "only " + replicas.size() + " healthy replicas, need W=" + quorum.w()));
        }

        CompletableFuture<Message> result = new CompletableFuture<>();
        AtomicInteger acks = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        for (NodeId replica : replicas) {
            send(replica, put).whenComplete((reply, err) -> {
                if (err == null && reply instanceof Message.Ok) {
                    if (acks.incrementAndGet() == quorum.w()) {
                        metrics.writes.incrementAndGet();
                        result.complete(new Message.Ok());
                    }
                } else if (failures.incrementAndGet() > replicas.size() - quorum.w()) {
                    metrics.unavailable.incrementAndGet();
                    result.complete(new Message.Error("QUORUM_FAILED", "write reached " + acks.get() + "/" + quorum.w() + " replicas"));
                }
            });
        }
        return result;
    }

    CompletableFuture<Message> get(String key) {
        List<NodeId> replicas = replicasFor(key);
        if (replicas.size() < quorum.r()) {
            metrics.unavailable.incrementAndGet();
            return CompletableFuture.completedFuture(new Message.Error("UNAVAILABLE",
                    "only " + replicas.size() + " healthy replicas, need R=" + quorum.r()));
        }

        CompletableFuture<Message> result = new CompletableFuture<>();
        ConcurrentLinkedQueue<Response> responses = new ConcurrentLinkedQueue<>();
        AtomicInteger replies = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        AtomicInteger settled = new AtomicInteger();

        for (NodeId replica : replicas) {
            send(replica, new Message.ReplicaGet(key)).whenComplete((reply, err) -> {
                if (err == null && reply instanceof Message.Value v) {
                    responses.add(new Response(replica, toVersioned(v)));
                    if (replies.incrementAndGet() == quorum.r()) {
                        metrics.reads.incrementAndGet();
                        result.complete(toClient(newest(responses)));
                    }
                } else if (failures.incrementAndGet() > replicas.size() - quorum.r()) {
                    metrics.unavailable.incrementAndGet();
                    result.complete(new Message.Error("QUORUM_FAILED", "read reached " + replies.get() + "/" + quorum.r() + " replicas"));
                }
                // Once every replica has answered (or failed), repair the stale ones.
                if (settled.incrementAndGet() == replicas.size()) readRepair(key, responses);
            });
        }
        return result;
    }

    private void readRepair(String key, Iterable<Response> responses) {
        Optional<Versioned> newest = newest(responses);
        if (newest.isEmpty()) return;
        Versioned winner = newest.get();
        for (Response r : responses) {
            if (r.value().map(v -> winner.isNewerThan(v)).orElse(true)) {
                metrics.readRepairs.incrementAndGet();
                send(r.node(), new Message.ReplicaPut(key, winner.value(), winner.timestamp(), winner.writer()));
            }
        }
    }

    private static Optional<Versioned> newest(Iterable<Response> responses) {
        Versioned best = null;
        for (Response r : responses) {
            if (r.value().isPresent() && r.value().get().isNewerThan(best)) best = r.value().get();
        }
        return Optional.ofNullable(best);
    }

    private static Optional<Versioned> toVersioned(Message.Value v) {
        if (!v.found()) return Optional.empty();
        return Optional.of(v.value() == null ? Versioned.tombstone(v.timestamp(), v.writer()) : Versioned.of(v.value(), v.timestamp(), v.writer()));
    }

    private static Message toClient(Optional<Versioned> v) {
        if (v.isEmpty() || v.get().isTombstone()) return new Message.Value(false, null, 0, "");
        Versioned x = v.get();
        return new Message.Value(true, x.value(), x.timestamp(), x.writer());
    }

    public NodeId self() {
        return self;
    }

    public NodeMetrics metrics() {
        return metrics;
    }

    public Storage storage() {
        return storage;
    }

    private record Response(NodeId node, Optional<Versioned> value) {}

    public static final class NodeMetrics {
        public final AtomicInteger reads = new AtomicInteger();
        public final AtomicInteger writes = new AtomicInteger();
        public final AtomicInteger readRepairs = new AtomicInteger();
        public final AtomicInteger unavailable = new AtomicInteger();
    }
}
