package com.ericguerra.ringkv.node;

import java.util.function.LongSupplier;

/**
 * Hybrid logical clock packed into a long: upper 48 bits wall-clock millis, lower 16 bits a counter.
 * Timestamps are monotonic per node and advance past any timestamp observed from peers,
 * so causally-later writes always win last-writer-wins even with modest clock skew.
 */
public final class HybridClock {
    private static final int COUNTER_BITS = 16;
    private static final long COUNTER_MASK = (1L << COUNTER_BITS) - 1;

    private final LongSupplier wallMillis;
    private long last;

    public HybridClock() {
        this(System::currentTimeMillis);
    }

    public HybridClock(LongSupplier wallMillis) {
        this.wallMillis = wallMillis;
    }

    public synchronized long now() {
        long physical = wallMillis.getAsLong() << COUNTER_BITS;
        last = physical > last ? physical : next(last);
        return last;
    }

    /** Merge a timestamp received from another node. */
    public synchronized long observe(long remote) {
        long physical = wallMillis.getAsLong() << COUNTER_BITS;
        long max = Math.max(physical, Math.max(last, remote));
        last = max == physical && physical > last && physical > remote ? physical : next(max);
        return last;
    }

    public static long millis(long hlc) {
        return hlc >>> COUNTER_BITS;
    }

    public static long counter(long hlc) {
        return hlc & COUNTER_MASK;
    }

    private static long next(long ts) {
        if ((ts & COUNTER_MASK) == COUNTER_MASK) {
            // Counter overflow: borrow from the millisecond component.
            return ((ts >>> COUNTER_BITS) + 1) << COUNTER_BITS;
        }
        return ts + 1;
    }
}
