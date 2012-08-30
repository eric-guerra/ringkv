package com.ericguerra.ringkv.ring;

import java.nio.charset.StandardCharsets;

/** 64-bit FNV-1a followed by a murmur-style finalizer for good avalanche on short keys. */
public final class Hashing {
    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    private Hashing() {}

    public static long hash(String key) {
        return hash(key.getBytes(StandardCharsets.UTF_8));
    }

    public static long hash(byte[] data) {
        long h = FNV_OFFSET;
        for (byte b : data) {
            h ^= (b & 0xff);
            h *= FNV_PRIME;
        }
        return mix(h);
    }

    static long mix(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }
}
