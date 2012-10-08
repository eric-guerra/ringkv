package com.ericguerra.ringkv.ring;

import com.ericguerra.ringkv.cluster.NodeId;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashRingTest {
    private static List<NodeId> nodes(int n) {
        return IntStream.range(0, n).mapToObj(i -> new NodeId("n" + i, "localhost", 7000 + i)).toList();
    }

    @Test
    void preferenceListHasDistinctNodes() {
        var ring = new ConsistentHashRing(64, nodes(5));
        for (int i = 0; i < 1000; i++) {
            List<NodeId> list = ring.preferenceList("key-" + i, 3);
            assertEquals(3, list.size());
            assertEquals(3, list.stream().distinct().count());
        }
    }

    @Test
    void preferenceListIsCappedByClusterSize() {
        var ring = new ConsistentHashRing(16, nodes(2));
        assertEquals(2, ring.preferenceList("k", 3).size());
        assertTrue(new ConsistentHashRing(16).preferenceList("k", 3).isEmpty());
    }

    @Test
    void loadIsReasonablyBalancedWithVirtualNodes() {
        var ring = new ConsistentHashRing(128, nodes(5));
        Map<NodeId, Integer> counts = new HashMap<>();
        int keys = 50_000;
        for (int i = 0; i < keys; i++) counts.merge(ring.primary("user:" + i), 1, Integer::sum);
        double ideal = keys / 5.0;
        for (int c : counts.values()) {
            assertTrue(Math.abs(c - ideal) / ideal < 0.25, "imbalanced: " + counts);
        }
    }

    @Test
    void addingANodeMovesOnlyAboutOneNthOfKeys() {
        var ring = new ConsistentHashRing(128, nodes(4));
        int keys = 20_000;
        NodeId[] before = new NodeId[keys];
        for (int i = 0; i < keys; i++) before[i] = ring.primary("k" + i);

        NodeId added = new NodeId("n4", "localhost", 7004);
        ring.add(added);
        int moved = 0;
        for (int i = 0; i < keys; i++) {
            NodeId now = ring.primary("k" + i);
            if (!now.equals(before[i])) {
                moved++;
                assertEquals(added, now, "keys may only move to the new node");
            }
        }
        double fraction = moved / (double) keys;
        assertTrue(fraction > 0.12 && fraction < 0.28, "moved " + fraction);
    }

    @Test
    void removeIsInverseOfAdd() {
        var ring = new ConsistentHashRing(32, nodes(3));
        NodeId extra = new NodeId("x", "localhost", 9000);
        String key = "stable";
        List<NodeId> original = ring.preferenceList(key, 3);
        ring.add(extra);
        ring.remove(extra);
        assertEquals(original, ring.preferenceList(key, 3));
        assertFalse(ring.members().contains(extra));
    }
}
