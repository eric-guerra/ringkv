package com.ericguerra.ringkv.storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.zip.CRC32;

/**
 * Append-only log of writes. Record layout:
 * <pre>
 *   int    length of payload
 *   long   CRC32 of payload
 *   payload: utf key | long ts | utf writer | int valueLen (-1 = tombstone) | bytes
 * </pre>
 * Replay stops at the first truncated or corrupt record (a torn write from a crash).
 */
public final class WriteAheadLog implements AutoCloseable {
    private final Path path;
    private final boolean fsync;
    private FileOutputStream fileOut;
    private DataOutputStream out;
    private long records;

    public WriteAheadLog(Path path, boolean fsync) throws IOException {
        this.path = path;
        this.fsync = fsync;
        Files.createDirectories(path.toAbsolutePath().getParent());
        open();
    }

    private void open() throws IOException {
        fileOut = new FileOutputStream(path.toFile(), true);
        out = new DataOutputStream(new BufferedOutputStream(fileOut));
    }

    public synchronized void append(String key, Versioned v) {
        try {
            byte[] payload = encode(key, v);
            CRC32 crc = new CRC32();
            crc.update(payload);
            out.writeInt(payload.length);
            out.writeLong(crc.getValue());
            out.write(payload);
            out.flush();
            if (fsync) fileOut.getFD().sync();
            records++;
        } catch (IOException e) {
            throw new UncheckedIOException("WAL append failed", e);
        }
    }

    /** Replays valid records in order. Returns the number replayed. */
    public static long replay(Path path, BiConsumer<String, Versioned> consumer) throws IOException {
        if (!Files.exists(path)) return 0;
        long n = 0;
        try (InputStream raw = Files.newInputStream(path);
             DataInputStream in = new DataInputStream(new BufferedInputStream(raw))) {
            while (true) {
                int len;
                long expectedCrc;
                byte[] payload;
                try {
                    len = in.readInt();
                    if (len <= 0 || len > 64 * 1024 * 1024) break;
                    expectedCrc = in.readLong();
                    payload = in.readNBytes(len);
                } catch (EOFException eof) {
                    break;
                }
                if (payload.length != len) break;
                CRC32 crc = new CRC32();
                crc.update(payload);
                if (crc.getValue() != expectedCrc) break;
                decode(payload, consumer);
                n++;
            }
        }
        return n;
    }

    /**
     * Rewrites the log to contain exactly the given live entries, atomically replacing the old file.
     */
    public synchronized void compact(Map<String, Versioned> live) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".compact");
        try (WriteAheadLog fresh = new WriteAheadLog(tmp, false)) {
            live.forEach(fresh::append);
            fresh.fileOut.getFD().sync();
        }
        out.close();
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        open();
        records = live.size();
    }

    public synchronized long records() {
        return records;
    }

    @Override
    public synchronized void close() throws IOException {
        out.close();
    }

    private static byte[] encode(String key, Versioned v) throws IOException {
        var buf = new java.io.ByteArrayOutputStream(64);
        try (DataOutputStream d = new DataOutputStream(buf)) {
            d.writeUTF(key);
            d.writeLong(v.timestamp());
            d.writeUTF(v.writer());
            byte[] value = v.value();
            if (value == null) {
                d.writeInt(-1);
            } else {
                d.writeInt(value.length);
                d.write(value);
            }
        }
        return buf.toByteArray();
    }

    private static void decode(byte[] payload, BiConsumer<String, Versioned> consumer) throws IOException {
        try (DataInputStream d = new DataInputStream(new java.io.ByteArrayInputStream(payload))) {
            String key = d.readUTF();
            long ts = d.readLong();
            String writer = d.readUTF();
            int len = d.readInt();
            Versioned v = len < 0 ? Versioned.tombstone(ts, writer) : Versioned.of(d.readNBytes(len), ts, writer);
            consumer.accept(key, v);
        }
    }
}
