package com.ericguerra.ringkv;

import com.ericguerra.ringkv.cluster.NodeId;
import com.ericguerra.ringkv.node.QuorumConfig;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Node configuration from environment variables:
 * <pre>
 *   RINGKV_SELF=n1@node1:7000
 *   RINGKV_PEERS=n1@node1:7000,n2@node2:7000,n3@node3:7000
 *   RINGKV_N=3 RINGKV_W=2 RINGKV_R=2 RINGKV_VNODES=128
 *   RINGKV_DATA=/data   RINGKV_FSYNC=false
 * </pre>
 */
public record Config(NodeId self, Set<NodeId> peers, QuorumConfig quorum, int vnodes, Path dataDir, boolean fsync,
                     long rpcTimeoutMs, long suspectAfterMs) {

    public static Config fromEnv(Map<String, String> env) {
        NodeId self = NodeId.parse(require(env, "RINGKV_SELF"));
        Set<NodeId> peers = new LinkedHashSet<>();
        peers.add(self);
        String peerSpec = env.getOrDefault("RINGKV_PEERS", "");
        Arrays.stream(peerSpec.split(",")).map(String::trim).filter(s -> !s.isEmpty()).map(NodeId::parse).forEach(peers::add);

        int n = Integer.parseInt(env.getOrDefault("RINGKV_N", String.valueOf(Math.min(3, peers.size()))));
        int w = Integer.parseInt(env.getOrDefault("RINGKV_W", String.valueOf(n / 2 + 1)));
        int r = Integer.parseInt(env.getOrDefault("RINGKV_R", String.valueOf(n / 2 + 1)));
        if (n > peers.size()) throw new IllegalArgumentException("N=" + n + " exceeds cluster size " + peers.size());

        return new Config(
                self,
                Set.copyOf(peers),
                new QuorumConfig(n, w, r),
                Integer.parseInt(env.getOrDefault("RINGKV_VNODES", "128")),
                Path.of(env.getOrDefault("RINGKV_DATA", "data/" + self.id())),
                Boolean.parseBoolean(env.getOrDefault("RINGKV_FSYNC", "false")),
                Long.parseLong(env.getOrDefault("RINGKV_RPC_TIMEOUT_MS", "500")),
                Long.parseLong(env.getOrDefault("RINGKV_SUSPECT_AFTER_MS", "3000")));
    }

    private static String require(Map<String, String> env, String key) {
        String v = env.get(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException(key + " is required");
        return v;
    }
}
