package com.ericguerra.ringkv.node;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class HybridClockTest {
    @Test
    void monotonicWhenWallClockStallsOrGoesBackwards() {
        AtomicLong wall = new AtomicLong(1000);
        HybridClock clock = new HybridClock(wall::get);
        long a = clock.now();
        long b = clock.now();
        wall.set(900); // NTP step backwards
        long c = clock.now();
        assertTrue(a < b && b < c);
        assertEquals(1000, HybridClock.millis(c));
        assertEquals(2, HybridClock.counter(c));
    }

    @Test
    void advancesPastRemoteTimestamps() {
        AtomicLong wall = new AtomicLong(1000);
        HybridClock local = new HybridClock(wall::get);
        long remote = new HybridClock(() -> 5000).now();
        long merged = local.observe(remote);
        assertTrue(merged > remote);
        assertTrue(local.now() > merged);
    }

    @Test
    void physicalTimeResetsCounter() {
        AtomicLong wall = new AtomicLong(1000);
        HybridClock clock = new HybridClock(wall::get);
        clock.now();
        clock.now();
        wall.set(2000);
        long t = clock.now();
        assertEquals(2000, HybridClock.millis(t));
        assertEquals(0, HybridClock.counter(t));
    }
}
