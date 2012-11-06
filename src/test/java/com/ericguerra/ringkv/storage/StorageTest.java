package com.ericguerra.ringkv.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StorageTest {
    private static Versioned v(String s, long ts, String w) {
        return Versioned.of(s.getBytes(StandardCharsets.UTF_8), ts, w);
    }

    @Test
    void lastWriterWinsWithWriterTieBreak() {
        var s = new MemoryStorage();
        assertTrue(s.putIfNewer("k", v("a", 10, "n1")));
        assertFalse(s.putIfNewer("k", v("old", 5, "n9")));
        assertTrue(s.putIfNewer("k", v("b", 10, "n2")), "same ts: higher writer id wins");
        assertFalse(s.putIfNewer("k", v("c", 10, "n1")));
        assertEquals("b", new String(s.get("k").orElseThrow().value(), StandardCharsets.UTF_8));
    }

    @Test
    void tombstonesWinOverOlderWrites() {
        var s = new MemoryStorage();
        s.putIfNewer("k", v("a", 10, "n1"));
        assertTrue(s.putIfNewer("k", Versioned.tombstone(11, "n1")));
        assertTrue(s.get("k").orElseThrow().isTombstone());
        assertFalse(s.putIfNewer("k", v("late", 9, "n2")), "a delayed older write must not resurrect the key");
    }

    @Test
    void walReplayRestoresState(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("wal.log");
        try (var s = new DurableStorage(wal, true, 3.0)) {
            s.putIfNewer("a", v("1", 1, "n1"));
            s.putIfNewer("b", v("2", 2, "n1"));
            s.putIfNewer("a", Versioned.tombstone(3, "n1"));
            s.putIfNewer("a", v("stale", 0, "n1")); // rejected, so not logged
        }
        try (var reopened = new DurableStorage(wal, false, 3.0)) {
            assertEquals(2, reopened.size());
            assertTrue(reopened.get("a").orElseThrow().isTombstone());
            assertEquals("2", new String(reopened.get("b").orElseThrow().value(), StandardCharsets.UTF_8));
        }
        Map<String, Versioned> replayed = new HashMap<>();
        assertEquals(3, WriteAheadLog.replay(wal, replayed::put));
    }

    @Test
    void replayStopsAtTornWrite(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("wal.log");
        try (var log = new WriteAheadLog(wal, false)) {
            log.append("a", v("1", 1, "n1"));
            log.append("b", v("2", 2, "n1"));
        }
        long size = Files.size(wal);
        try (var raf = new RandomAccessFile(wal.toFile(), "rw")) {
            raf.setLength(size - 3); // simulate crash mid-record
        }
        Map<String, Versioned> seen = new HashMap<>();
        assertEquals(1, WriteAheadLog.replay(wal, seen::put));
        assertTrue(seen.containsKey("a"));
    }

    @Test
    void replayStopsAtCorruptChecksum(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("wal.log");
        try (var log = new WriteAheadLog(wal, false)) {
            log.append("a", v("1", 1, "n1"));
        }
        try (var raf = new RandomAccessFile(wal.toFile(), "rw")) {
            raf.seek(raf.length() - 1);
            raf.write(0x42);
        }
        assertEquals(0, WriteAheadLog.replay(wal, (k, x) -> {}));
    }

    @Test
    void compactionKeepsOnlyLiveEntries(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("wal.log");
        try (var s = new DurableStorage(wal, false, 3.0)) {
            for (int i = 1; i <= 5000; i++) s.putIfNewer("hot", v("v" + i, i, "n1"));
        }
        Map<String, Versioned> seen = new HashMap<>();
        long records = WriteAheadLog.replay(wal, seen::put);
        assertTrue(records < 2000, "log should have been compacted, has " + records);
        assertEquals("v5000", new String(seen.get("hot").value(), StandardCharsets.UTF_8));
    }
}
