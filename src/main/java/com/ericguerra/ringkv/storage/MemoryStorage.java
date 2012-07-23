package com.ericguerra.ringkv.storage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemoryStorage implements Storage {
    private final ConcurrentHashMap<String, Versioned> data = new ConcurrentHashMap<>();

    @Override
    public Optional<Versioned> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    @Override
    public boolean putIfNewer(String key, Versioned value) {
        AtomicBoolean applied = new AtomicBoolean(false);
        data.compute(key, (k, current) -> {
            if (value.isNewerThan(current)) {
                applied.set(true);
                return value;
            }
            return current;
        });
        return applied.get();
    }

    @Override
    public Map<String, Versioned> snapshot() {
        return Map.copyOf(data);
    }

    @Override
    public int size() {
        return data.size();
    }
}
