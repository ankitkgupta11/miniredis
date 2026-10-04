package com.finapp.mini_redis;

import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class MiniRedisDatabase {

	private static final int MAX_CACHE_ENTRIES = 3;

	private final LRUCache cache;
	
	private final Map<String, Long> expiryDatabase = new ConcurrentHashMap<>();

	private final ScheduledExecutorService expiryScheduler = Executors.newSingleThreadScheduledExecutor();

	private final PriorityQueue<ExpiryEntry> expiryQueue = new PriorityQueue<>();

	private final Thread expiryWorker;

	private final AtomicLong cacheHits = new AtomicLong(0);
	private final AtomicLong cacheMisses = new AtomicLong(0);
	
	private final AOFManager aofManager;

	public MiniRedisDatabase() {

	    cache = new LRUCache(MAX_CACHE_ENTRIES);

		expiryWorker = new Thread(this::expirationLoop, "redis-expiry-worker");

		expiryWorker.setDaemon(true);
		expiryWorker.start();
		

	    /*
	     * AOF file will be created in the project
	     * working directory.
	     *
	     * Example:
	     *
	     * mini-redis/
	     * ├── mini-redis.aof
	     * └── src/
	     */
	    aofManager = new AOFManager("mini-redis.aof");


		System.out.println("Expiry worker started");
	}

	public synchronized void set(String key, String value) {

	    /*
	     * SET replaces the existing value.
	     *
	     * A new SET also removes the old TTL because:
	     *
	     * SET name Ankit
	     * EXPIRE name 60
	     *
	     * SET name Rahul
	     *
	     * The new "Rahul" value should not inherit
	     * the old expiration time.
	     */
	    expiryDatabase.remove(key);

	    String evictedKey = cache.put(key, value);

	    /*
	     * LRU and TTL are two different mechanisms.
	     *
	     * If LRU removes a key, its TTL information
	     * must also be removed.
	     */
	    if (evictedKey != null) {
	        expiryDatabase.remove(evictedKey);
	    }
	    
	    /*
	     * Persist the command AFTER successfully
	     * modifying the in-memory database.
	     */
	    aofManager.appendCommand(
	            "SET " + key + " " + value
	    );
	}

	public synchronized String get(String key) {

	    /*
	     * First check TTL.
	     *
	     * If the key expired, remove it before returning.
	     *      * An expired key should behave like a cache miss.

	     */
	    if (isExpired(key)) {
	        cache.remove(key);
	        expiryDatabase.remove(key);
	        cacheMisses.incrementAndGet();
	        return null;
	    }

	    /*
	     * cache.get() also updates LRU order.
	     *
	     * Therefore GET is considered a "recent use".
	     */
	    /*
         * Value was found in cache.
         */
        cacheHits.incrementAndGet();
	    return cache.get(key);
	}

	public synchronized boolean exists(String key) {

	    if (isExpired(key)) {
	        cache.remove(key);
	        expiryDatabase.remove(key);
	        return false;
	    }

	    return cache.containsKey(key);
	}

	public synchronized boolean delete(String key) {

	    expiryDatabase.remove(key);

	    boolean deleted =
	            cache.remove(key) != null;

	    if (deleted) {

	        /*
	         * Persist the mutation so that during
	         * recovery the key is deleted again.
	         */
	        aofManager.appendCommand(
	                "DEL " + key
	        );
	    }

	    return deleted;
	}

	public long ttl(String key) {

		if (isExpired(key)) {
			delete(key);
			return -2;
		}

		if (!cache.containsKey(key)) {
			return -2;
		}

		Long expiryTime = expiryDatabase.get(key);

		if (expiryTime == null) {
			return -1;
		}

		long remainingMillis = expiryTime - System.currentTimeMillis();

		return remainingMillis / 1000;
	}

	private boolean isExpired(String key) {

		Long expiryTime = expiryDatabase.get(key);

		if (expiryTime == null) {
			return false;
		}

		return System.currentTimeMillis() >= expiryTime;
	}


	private void expirationLoop() {

		while (true) {

			synchronized (expiryQueue) {

				try {

					while (expiryQueue.isEmpty()) {

						expiryQueue.wait();
					}

					ExpiryEntry entry = expiryQueue.peek();

					long currentTime = System.currentTimeMillis();

					long waitTime = entry.getExpiryTime() - currentTime;

					if (waitTime > 0) {

						expiryQueue.wait(waitTime);

						continue;
					}

					expiryQueue.poll();

					String key = entry.getKey();

					Long actualExpiry = expiryDatabase.get(key);

					if (actualExpiry != null && actualExpiry == entry.getExpiryTime()) {

						cache.remove(key);
						expiryDatabase.remove(key);

						System.out.println("Expired key removed: " + key);
					}

				} catch (InterruptedException e) {

					Thread.currentThread().interrupt();

					System.out.println("Expiry worker interrupted");

					break;
				}
			}
		}
	}

	public boolean expire(String key, long seconds) {

		if (!cache.containsKey(key)) {
			return false;
		}

		long expiryTime = System.currentTimeMillis() + (seconds * 1000);

		expiryDatabase.put(key, expiryTime);

		synchronized (expiryQueue) {

			expiryQueue.offer(new ExpiryEntry(key, expiryTime));

			expiryQueue.notifyAll();
		}

		
		 /*
	     * Persist the ABSOLUTE expiration time.
	     *
	     * Do not write:
	     *
	     * EXPIRE key 60
	     *
	     * because recovery might happen much later.
	     */
	    aofManager.appendCommand(
	            "EXPIREAT "
	            + key
	            + " "
	            + expiryTime
	    );

		return true;
	}

	public synchronized long increment(String key) {

	    /*
	     * If the key does not exist, Redis-style INCR creates it
	     * with value 1.
	     *
	     * Example:
	     *
	     * INCR counter
	     * counter = 1
	     *
	     * INCR counter
	     * counter = 2
	     */
	    String currentValue = cache.get(key);

	    if (currentValue == null) {
	        cache.put(key, "1");
	        return 1;
	    }

	    try {

	        long value = Long.parseLong(currentValue);

	        value++;

	        /*
	         * Putting the value again also makes this key
	         * the most recently used entry in the LRU cache.
	         */
	        String evictedKey = cache.put(key, String.valueOf(value));

	        if (evictedKey != null) {
	            expiryDatabase.remove(evictedKey);
	        }
	        aofManager.appendCommand(
	                "INCR " + key
	        );
	        return value;

	    } catch (NumberFormatException e) {

	        /*
	         * INCR only works with integer values.
	         *
	         * Example:
	         *
	         * SET name Ankit
	         * INCR name
	         *
	         * This is invalid because "Ankit" is not a number.
	         */
	        throw new IllegalArgumentException(
	                "Value is not an integer"
	        );
	    }
	}
	public synchronized long decrement(String key) {

	    /*
	     * If the key does not exist, DECR creates it
	     * with value -1.
	     *
	     * Example:
	     *
	     * DECR counter
	     * counter = -1
	     *
	     * DECR counter
	     * counter = -2
	     */
	    String currentValue = cache.get(key);

	    if (currentValue == null) {
	        cache.put(key, "-1");
	        return -1;
	    }

	    try {

	        long value = Long.parseLong(currentValue);

	        value--;

	        /*
	         * GET/put keeps the key's LRU position updated.
	         */
	        String evictedKey = cache.put(key, String.valueOf(value));

	        if (evictedKey != null) {
	            expiryDatabase.remove(evictedKey);
	        }
	        aofManager.appendCommand(
	                "DECR " + key
	        );
	        return value;

	    } catch (NumberFormatException e) {

	        throw new IllegalArgumentException(
	                "Value is not an integer"
	        );
	    }
	}
	
	public long getCacheHits() {
	    return cacheHits.get();
	}

	public long getCacheMisses() {
	    return cacheMisses.get();
	}
	
	public double getCacheHitRate() {

	    long hits = cacheHits.get();
	    long misses = cacheMisses.get();

	    long totalRequests = hits + misses;

	    if (totalRequests == 0) {
	        return 0.0;
	    }

	    return (hits * 100.0) / totalRequests;
	}
	
	
	public synchronized void recoverFromAOF() {

	    List<String> commands =
	            aofManager.readCommands();

	    System.out.println(
	            "[AOF] Found "
	            + commands.size()
	            + " commands"
	    );

	    for (String command : commands) {

	        System.out.println(
	                "[AOF] Replaying: " + command
	        );

	        replayCommand(command);
	    }

	    System.out.println(
	            "[AOF] Recovery completed"
	    );
	}
	
	private void replayCommand(String command) {

	    String[] parts = command.split(" ", 3);

	    String operation = parts[0];

	    switch (operation) {

	        case "SET":

	            String key = parts[1];
	            String value = parts[2];

	            /*
	             * IMPORTANT:
	             *
	             * Do NOT call normal set().
	             *
	             * Normal set() writes to AOF again.
	             *
	             * During recovery we only rebuild memory.
	             */
	            cache.put(key, value);

	            break;

	        case "DEL":

	            cache.remove(parts[1]);

	            break;

	        case "INCR":

	            replayIncrement(parts[1]);

	            break;

	        case "DECR":

	            replayDecrement(parts[1]);

	            break;
	            
	        case "EXPIREAT":

	            String key1 = parts[1];

	            long expiryTime =
	                    Long.parseLong(parts[2]);

	            /*
	             * Check whether the key has already expired
	             * while the server was offline.
	             */
	            if (expiryTime <= System.currentTimeMillis()) {

	                cache.remove(key1);
	                expiryDatabase.remove(key1);

	                break;
	            }

	            /*
	             * Restore the TTL metadata.
	             */
	            expiryDatabase.put(
	                    key1,
	                    expiryTime
	            );

	            /*
	             * Restore the expiration entry in the heap.
	             */
	            expiryQueue.offer(
	                    new ExpiryEntry(
	                            key1,
	                            expiryTime
	                    )
	            );

	            break;

	        default:

	            System.out.println(
	                    "[AOF] Unknown command: "
	                    + command
	            );
	    }
	}
	
	private void replayIncrement(String key) {

	    String value = cache.get(key);

	    if (value == null) {

	        cache.put(key, "1");

	        return;
	    }

	    long number =
	            Long.parseLong(value);

	    cache.put(
	            key,
	            String.valueOf(number + 1)
	    );
	}
	
	private void replayDecrement(String key) {

	    String value = cache.get(key);

	    if (value == null) {

	        cache.put(key, "-1");

	        return;
	    }

	    long number =
	            Long.parseLong(value);

	    cache.put(
	            key,
	            String.valueOf(number - 1)
	    );
	}
	
	public void closePersistence() {

	    aofManager.close();
	}
	
}