package com.finapp.mini_redis;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache {

    private final int maxCapacity;

    /*
     * accessOrder = true means:
     *
     * The map keeps entries in order of recent usage.
     *
     * Example:
     *
     * SET A
     * SET B
     * SET C
     * GET A
     *
     * Order becomes:
     * B -> C -> A
     *
     * If capacity is full and a new key is inserted,
     * B will be removed because B is the least recently used.
     */
    private final LinkedHashMap<String, String> cache;

    public LRUCache(int maxCapacity) {

        if (maxCapacity <= 0) {
            throw new IllegalArgumentException(
                    "Cache capacity must be greater than 0"
            );
        }

        this.maxCapacity = maxCapacity;

        cache = new LinkedHashMap<>(
                16,
                0.75f,
                true
        );
    }

    /*
     * Adds or updates a key.
     *
     * Returns the key that was evicted.
     * Returns null if nothing was evicted.
     *
     * We return the evicted key because MiniRedisDatabase
     * also needs to remove that key's TTL information.
     */
    public synchronized String put(String key, String value) {

        cache.put(key, value);

        if (cache.size() > maxCapacity) {

            Map.Entry<String, String> eldest =
                    cache.entrySet().iterator().next();

            String evictedKey = eldest.getKey();

            cache.remove(evictedKey);

            System.out.println(
                    "LRU eviction: " + evictedKey
                    + " = " + eldest.getValue()
            );

            return evictedKey;
        }

        return null;
    }

    /*
     * GET is also an LRU operation.
     *
     * Because accessOrder=true, calling get()
     * moves the key to the most recently used position.
     */
    public synchronized String get(String key) {
        return cache.get(key);
    }

    public synchronized boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    public synchronized String remove(String key) {
        return cache.remove(key);
    }

    public synchronized int size() {
        return cache.size();
    }
}