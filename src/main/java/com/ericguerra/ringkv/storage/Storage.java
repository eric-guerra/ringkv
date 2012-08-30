package com.ericguerra.ringkv.storage;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;

public interface Storage extends Closeable {
    Optional<Versioned> get(String key);

    /** Applies the write only if it is newer than what is stored. Returns true if applied. */
    boolean putIfNewer(String key, Versioned value);

    /** Snapshot of all entries, including tombstones (used for anti-entropy and compaction). */
    Map<String, Versioned> snapshot();

    int size();

    @Override
    default void close() {}
}
