package com.finapp.mini_redis;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MiniRedisApplication {

	public static void main(String[] args) {
		SpringApplication.run(MiniRedisApplication.class, args);
		MiniRedisServer server = new MiniRedisServer();
		MiniRedisDatabase database = new MiniRedisDatabase();

		 /*
	     * Rebuild the in-memory database before
	     * accepting clients.
	     */
	    database.recoverFromAOF();

		/*
         * JVM shutdown hook.
         *
         * When the application receives a shutdown signal,
         * Java executes this thread.
         *
         * For example:
         *
         * Ctrl + C
         *     ↓
         * JVM shutdown
         *     ↓
         * shutdown hook
         *     ↓
         * server.shutdownServer()
         */
        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        server::shutdownServer,
                        "redis-shutdown-hook"
                )
        );

        server.start();
	}

}


/*
 * 1–5 Core Redis engine TCP, commands, persistent connections, thread pool 6–9
 * TTL & expiration TTL, EXPIRE, min-heap, wait/notify 10–13 Concurrency &
 * reliability bounded thread pool, connection limits, graceful shutdown 14–17
 *   18–21 Persistence AOF +
 * snapshot + recovery 22–25 Advanced caching cache stampede, request
 * coalescing, negative caching 26–29 Your unique feature adaptive TTL +
 * stale-while-revalidate 30–35 Spring Boot integration REST API + PostgreSQL +
 * Mini Redis + cache-aside 36–40 Testing & interview preparation load testing,
 * benchmarks, architecture, failure scenarios
 */
