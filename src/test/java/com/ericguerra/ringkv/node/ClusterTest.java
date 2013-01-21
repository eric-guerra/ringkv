package com.ericguerra.ringkv.node;

import com.ericguerra.ringkv.cluster.FailureDetector;
import com.ericguerra.ringkv.cluster.LocalTransport;
import com.ericguerra.ringkv.cluster.NodeId;
import com.ericguerra.ringkv.protocol.Message;
import com.ericguerra.ringkv.ring.ConsistentHashRing;
import com.ericguerra.ringkv.storage.MemoryStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/** Five in-process nodes over LocalTransport (which round-trips through the wire codec). */
class ClusterTest {
    private final LocalTransport transport = new LocalTransport();
    private final AtomicLong wall = new AtomicLong(1_000_000);
    private final Map<NodeId, Node> nodes = new LinkedHashMap<>();
    private final Map<NodeId, FailureDetector> detectors = new LinkedHashMap<>();
    private ConsistentHashRing ring;

    @BeforeEach
    void setUp() {
        List<NodeId> ids = IntStream.range(0, 5).mapToObj(i -> new NodeId("n" + i, "localhost", 7000 + i)).toList();
        Set<NodeId> all = Set.copyOf(ids);
        ring = new ConsistentHashRing(64, ids);
        for (NodeId id : ids) {
            var detector = new FailureDetector(id, all, transport, 3000, wall::get);
            var node = new Node(id, ring, new MemoryStorage(), transport, detector, new QuorumConfig(3, 2, 2), new HybridClock(wall::get));
            nodes.put(id, node);
            detectors.put(id, detector);
            transport.register(id, m -> node.handle(m).join());
        }
    }

    private Node any() {
        return nodes.values().iterator().next();
    }

    private static Message await(java.util.concurrent.CompletableFuture<Message> f) throws Exception {
        return f.get(5, TimeUnit.SECONDS);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(Message m) {
        assertInstanceOf(Message.Value.class, m, String.valueOf(m));
        var v = (Message.Value) m;
        return v.found() ? new String(v.value(), StandardCharsets.UTF_8) : null;
    }

    @Test
    void writeThroughOneNodeReadThroughAnother() throws Exception {
        var nodeList = List.copyOf(nodes.values());
        assertInstanceOf(Message.Ok.class, await(nodeList.get(0).handle(new Message.Put("user:1", bytes("ada")))));
        assertEquals("ada", text(await(nodeList.get(3).handle(new Message.Get("user:1")))));
        assertNull(text(await(nodeList.get(2).handle(new Message.Get("missing")))));
    }

    @Test
    void dataLivesOnExactlyNReplicas() throws Exception {
        await(any().handle(new Message.Put("k", bytes("v"))));
        Thread.sleep(50);
        long holders = nodes.values().stream().filter(n -> n.storage().get("k").isPresent()).count();
        assertEquals(3, holders);
        Set<NodeId> expected = Set.copyOf(ring.preferenceList("k", 3));
        Set<NodeId> actual = nodes.values().stream().filter(n -> n.storage().get("k").isPresent()).map(Node::self).collect(Collectors.toSet());
        assertEquals(expected, actual);
    }

    @Test
    void deletesAreVisibleAndBeatOlderValues() throws Exception {
        await(any().handle(new Message.Put("k", bytes("v1"))));
        wall.addAndGet(10);
        await(any().handle(new Message.Delete("k")));
        assertNull(text(await(any().handle(new Message.Get("k")))));
    }

    @Test
    void toleratesOneReplicaDown() throws Exception {
        NodeId victim = ring.preferenceList("k", 3).get(0);
        Node coordinator = nodes.values().stream().filter(n -> !n.self().equals(victim)).findFirst().orElseThrow();
        transport.setDown(victim, true);
        assertInstanceOf(Message.Ok.class, await(coordinator.handle(new Message.Put("k", bytes("v")))));
        assertEquals("v", text(await(coordinator.handle(new Message.Get("k")))));
    }

    @Test
    void failsWhenQuorumUnreachableButDetectorHasNotNoticed() throws Exception {
        List<NodeId> replicas = ring.preferenceList("k", 3);
        Node coordinator = nodes.values().stream().filter(n -> !replicas.contains(n.self())).findFirst().orElseThrow();
        transport.setDown(replicas.get(0), true);
        transport.setDown(replicas.get(1), true);
        var reply = await(coordinator.handle(new Message.Put("k", bytes("v"))));
        assertInstanceOf(Message.Error.class, reply);
        assertEquals("QUORUM_FAILED", ((Message.Error) reply).code());
    }

    @Test
    void sloppyQuorumRoutesAroundSuspectNodes() throws Exception {
        List<NodeId> replicas = ring.preferenceList("k", 3);
        Node coordinator = nodes.values().stream().filter(n -> !replicas.contains(n.self())).findFirst().orElseThrow();
        transport.setDown(replicas.get(0), true);
        transport.setDown(replicas.get(1), true);

        // Time passes; healthy peers keep answering pings, the downed ones do not.
        wall.addAndGet(5000);
        detectors.get(coordinator.self()).probeAll();
        Thread.sleep(100);
        assertEquals(Set.of(replicas.get(0), replicas.get(1)), detectors.get(coordinator.self()).suspects());

        assertInstanceOf(Message.Ok.class, await(coordinator.handle(new Message.Put("k", bytes("v")))));
        assertEquals("v", text(await(coordinator.handle(new Message.Get("k")))));
    }

    @Test
    void readRepairFixesStaleReplica() throws Exception {
        List<NodeId> replicas = ring.preferenceList("k", 3);
        NodeId lagger = replicas.get(2);
        Node coordinator = nodes.get(replicas.get(0));

        await(coordinator.handle(new Message.Put("k", bytes("v1"))));
        Thread.sleep(50);
        transport.setDown(lagger, true);
        wall.addAndGet(10);
        await(coordinator.handle(new Message.Put("k", bytes("v2"))));
        transport.setDown(lagger, false);
        assertEquals("v1", new String(nodes.get(lagger).storage().get("k").orElseThrow().value(), StandardCharsets.UTF_8));

        assertEquals("v2", text(await(coordinator.handle(new Message.Get("k")))));
        Thread.sleep(200);
        assertEquals("v2", new String(nodes.get(lagger).storage().get("k").orElseThrow().value(), StandardCharsets.UTF_8));
        assertTrue(coordinator.metrics().readRepairs.get() >= 1);
    }

    @Test
    void unavailableWhenTooFewHealthyNodes() throws Exception {
        Node coordinator = any();
        nodes.keySet().stream().filter(id -> !id.equals(coordinator.self())).forEach(id -> transport.setDown(id, true));
        wall.addAndGet(5000);
        var reply = await(coordinator.handle(new Message.Get("k")));
        assertEquals("UNAVAILABLE", ((Message.Error) reply).code());
    }
}
