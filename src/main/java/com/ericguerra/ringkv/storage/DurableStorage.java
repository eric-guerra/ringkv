package com.ericguerra.ringkv.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

/**
 * Memory storage backed by a write-ahead log. On startup the log is replayed;
 * when the log grows past {@code compactionRatio} times the live entry count it is compacted.
 */
public final class DurableStorage extends MemoryStorage {
    private final WriteAheadLog wal;
    private final double compactionRatio;

    public DurableStorage(Path walPath, boolean fsync, double compactionRatio) throws IOException {
        this.compactionRatio = compactionRatio;
        WriteAheadLog.replay(walPath, super::putIfNewer);
        this.wal = new WriteAheadLog(walPath, fsync);
    }

    @Override
    public synchronized boolean putIfNewer(String key, Versioned value) {
        if (!super.putIfNewer(key, value)) return false;
        wal.append(key, value);
        maybeCompact();
        return true;
    }

    private void maybeCompact() {
        int live = Math.max(1, size());
        if (wal.records() > 1000 && wal.records() > live * compactionRatio) {
            try {
                wal.compact(snapshot());
            } catch (IOException e) {
                throw new UncheckedIOException("WAL compaction failed", e);
            }
        }
    }

    @Override
    public void close() {
        try {
            wal.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
