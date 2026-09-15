package com.finapp.mini_redis;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MiniRedisDatabase {

    private final Map<String, String> database = new ConcurrentHashMap<>();

    public void set(String key, String value) {

        database.put(key, value);
    }

    public String get(String key) {

        return database.get(key);
    } 

    public boolean exists(String key) {
        return database.containsKey(key);
    }

    public boolean delete(String key) {
        return database.remove(key) != null;
    }
}