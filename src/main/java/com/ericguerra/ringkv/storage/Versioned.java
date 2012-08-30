package com.ericguerra.ringkv.storage;

import java.util.Arrays;
import java.util.Objects;

/**
 * A value with a hybrid-logical-clock timestamp. Conflicts resolve last-writer-wins on
 * (timestamp, writer); deletes are tombstones so they replicate and win over older writes.
 */
public final class Versioned implements Comparable<Versioned> {
    private final byte[] value;
    private final long timestamp;
    private final String writer;

    private Versioned(byte[] value, long timestamp, String writer) {
        this.value = value;
        this.timestamp = timestamp;
        this.writer = Objects.requireNonNull(writer);
    }

    public static Versioned of(byte[] value, long timestamp, String writer) {
        return new Versioned(Objects.requireNonNull(value).clone(), timestamp, writer);
    }

    public static Versioned tombstone(long timestamp, String writer) {
        return new Versioned(null, timestamp, writer);
    }

    public boolean isTombstone() {
        return value == null;
    }

    public byte[] value() {
        return value == null ? null : value.clone();
    }

    public long timestamp() {
        return timestamp;
    }

    public String writer() {
        return writer;
    }

    public boolean isNewerThan(Versioned other) {
        return other == null || compareTo(other) > 0;
    }

    @Override
    public int compareTo(Versioned o) {
        int c = Long.compare(timestamp, o.timestamp);
        return c != 0 ? c : writer.compareTo(o.writer);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Versioned v && timestamp == v.timestamp && writer.equals(v.writer) && Arrays.equals(value, v.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, writer, Arrays.hashCode(value));
    }

    @Override
    public String toString() {
        return (isTombstone() ? "tombstone" : value.length + "B") + "@" + timestamp + "/" + writer;
    }
}
