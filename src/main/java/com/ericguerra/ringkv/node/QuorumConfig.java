package com.ericguerra.ringkv.node;

/**
 * N replicas, W acks required for a write, R responses for a read.
 * R + W > N gives read-your-writes for successful quorum operations.
 */
public record QuorumConfig(int n, int w, int r) {
    public QuorumConfig {
        if (n < 1 || w < 1 || r < 1 || w > n || r > n) {
            throw new IllegalArgumentException("require 1 <= W,R <= N, got N=" + n + " W=" + w + " R=" + r);
        }
    }

    public boolean isStrong() {
        return r + w > n;
    }
}
