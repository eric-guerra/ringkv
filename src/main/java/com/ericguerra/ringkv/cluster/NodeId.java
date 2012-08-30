package com.ericguerra.ringkv.cluster;

import java.util.Objects;

/** Cluster member identity. `id` is stable across restarts; host/port are where it listens. */
public record NodeId(String id, String host, int port) {
    public NodeId {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(host, "host");
        if (port <= 0 || port > 65535) throw new IllegalArgumentException("bad port " + port);
    }

    /** Parses {@code id@host:port}. */
    public static NodeId parse(String spec) {
        int at = spec.indexOf('@');
        int colon = spec.lastIndexOf(':');
        if (at <= 0 || colon <= at + 1) throw new IllegalArgumentException("expected id@host:port, got " + spec);
        return new NodeId(spec.substring(0, at), spec.substring(at + 1, colon), Integer.parseInt(spec.substring(colon + 1)));
    }

    @Override
    public String toString() {
        return id + "@" + host + ":" + port;
    }
}
