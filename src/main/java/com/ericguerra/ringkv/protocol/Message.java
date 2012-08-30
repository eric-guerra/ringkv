package com.ericguerra.ringkv.protocol;

/**
 * Wire messages. Client commands (GET/PUT/DEL) go to any node, which coordinates;
 * replica commands (RGET/RPUT) are node-to-node; responses carry a status.
 */
public sealed interface Message {
    // ---- client requests ----
    record Get(String key) implements Message {}

    record Put(String key, byte[] value) implements Message {}

    record Delete(String key) implements Message {}

    // ---- replica requests ----
    record ReplicaGet(String key) implements Message {}

    /** value == null means tombstone. */
    record ReplicaPut(String key, byte[] value, long timestamp, String writer) implements Message {}

    record Ping(String from) implements Message {}

    // ---- responses ----
    record Ok() implements Message {}

    /** found=false for missing keys. value == null with found=true means tombstone (replica reads only). */
    record Value(boolean found, byte[] value, long timestamp, String writer) implements Message {}

    record Error(String code, String message) implements Message {}

    record Pong(String from) implements Message {}
}
