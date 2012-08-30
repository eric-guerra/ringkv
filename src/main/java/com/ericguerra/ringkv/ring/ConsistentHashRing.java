package com.ericguerra.ringkv.ring;

import com.ericguerra.ringkv.cluster.NodeId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Consistent hash ring with virtual nodes. Each physical node owns {@code vnodes}
 * tokens; a key's preference list is the first N distinct physical nodes found
 * walking clockwise from the key's hash.
 */
public final class ConsistentHashRing {
    private final int vnodes;
    private final NavigableMap<Long, NodeId> tokens = new TreeMap<>();
    private final Set<NodeId> members = new LinkedHashSet<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsistentHashRing(int vnodes) {
        if (vnodes < 1) throw new IllegalArgumentException("vnodes must be >= 1");
        this.vnodes = vnodes;
    }

    public ConsistentHashRing(int vnodes, Collection<NodeId> nodes) {
        this(vnodes);
        nodes.forEach(this::add);
    }

    public void add(NodeId node) {
        lock.writeLock().lock();
        try {
            if (!members.add(node)) return;
            for (int i = 0; i < vnodes; i++) {
                long token = Hashing.hash(node.id() + "#" + i);
                // Token collisions are astronomically rare with 64-bit hashes; resolve deterministically anyway.
                while (tokens.containsKey(token)) token++;
                tokens.put(token, node);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void remove(NodeId node) {
        lock.writeLock().lock();
        try {
            if (members.remove(node)) tokens.values().removeIf(node::equals);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** First {@code n} distinct nodes clockwise from the key's position (fewer if the cluster is smaller). */
    public List<NodeId> preferenceList(String key, int n) {
        lock.readLock().lock();
        try {
            List<NodeId> result = new ArrayList<>(n);
            if (tokens.isEmpty()) return result;
            long h = Hashing.hash(key);
            int wanted = Math.min(n, members.size());
            for (NodeId node : walk(h)) {
                if (!result.contains(node)) {
                    result.add(node);
                    if (result.size() == wanted) break;
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    public NodeId primary(String key) {
        List<NodeId> list = preferenceList(key, 1);
        if (list.isEmpty()) throw new IllegalStateException("ring is empty");
        return list.get(0);
    }

    public Set<NodeId> members() {
        lock.readLock().lock();
        try {
            return Set.copyOf(members);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterable<NodeId> walk(long start) {
        List<NodeId> order = new ArrayList<>(tokens.size());
        for (Map.Entry<Long, NodeId> e : tokens.tailMap(start, true).entrySet()) order.add(e.getValue());
        for (Map.Entry<Long, NodeId> e : tokens.headMap(start, false).entrySet()) order.add(e.getValue());
        return order;
    }
}
