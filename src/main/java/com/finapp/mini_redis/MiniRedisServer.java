package com.finapp.mini_redis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket; //Imports the Java class responsible for listening for incoming network requests over TCP.
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MiniRedisServer {

	private static final int PORT = 6379;
	private final MiniRedisDatabase database;
	private static final int CORE_POOL_SIZE = 5;
	private static final int MAX_POOL_SIZE = 8;
	private static final int QUEUE_CAPACITY = 10;

	private final ThreadPoolExecutor threadPool;
	
	private static final int MAX_CONNECTIONS = 20;
	// Create a thread-safe master list (Set) to track all open client sockets currently connected.
	private final Set<Socket> activeSockets =
	        ConcurrentHashMap.newKeySet();
	/*
	 * AtomicInteger: A utility class from the java.util.concurrent.atomic package.
	 * It provides integer operations (like incrementing and decrementing) that are
	 * guaranteed to be atomic—meaning they happen as a single, uninterrupted
	 * operation. This prevents race conditions when multiple threads try to update
	 * the counter at the exact same millisecond. initialize to zero
	 */	private final AtomicInteger activeConnections =
	        new AtomicInteger(0);

	public MiniRedisServer() {

		
		/*
         * ThreadPoolExecutor gives us more control than
         * Executors.newFixedThreadPool().
         *
         * CORE_POOL_SIZE = 5
         * --------------------------------
         * Number of worker threads normally available.
         *
         * MAX_POOL_SIZE = 8
         * --------------------------------
         * Core Pool First: When you send tasks 1 through 5, the executor creates
		 * threads until it hits the CORE_POOL_SIZE (5).
		 * The Queue Second: When task 6
		 * arrives, the executor does not create a 6th thread. Instead, it holds the
		 * task in the ArrayBlockingQueue (up to 10 tasks).Max Pool Last (The Backup):
		 * Only when the queue is 100% full (holding 10 tasks) and task 16 arrives, the
		 * executor finally looks at MAX_POOL_SIZE. It says,
		 * "The queue is full, let me spin up temporary backup threads" up to the limit
		 * of 8.
         *
         *
         * QUEUE_CAPACITY = 10
         * --------------------------------
         * If all 5 workers are busy, up to 10 additional
         * client-handling tasks can wait in the queue.
         *
         * After both workers and queue are full, new tasks
         * are rejected.
         *
         * Real-life example:
         *
         * Restaurant
         * ┌──────────────────────────────┐
         * │ 5 chefs = 5 worker threads
         *   │
         * │ 10 waiting orders = queue
				but we have 8 max pool size so new 3 cheif will come to help │
         * │ Restaurant full = reject     │
         * └──────────────────────────────┘
         *
         * This is called BACKPRESSURE.
         *
         * Instead of allowing unlimited requests to accumulate
         * and potentially consume too much memory, the server
         * has a defined capacity.
         */
		
		database = new MiniRedisDatabase();
		threadPool = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, 60, TimeUnit.SECONDS,
				new ArrayBlockingQueue<>(QUEUE_CAPACITY), new ThreadPoolExecutor.AbortPolicy());

		System.out.println("Thread pool created: workers=" + CORE_POOL_SIZE + ", queue=" + QUEUE_CAPACITY);
	}

	 /**
     * Starts the Mini Redis TCP server.
     *
     * The server continuously waits for new client connections.
     * Each accepted connection is submitted to the thread pool
     * instead of being handled directly by the main server thread.
     */
	public void start() {

		
		/*
         * try-with-resources automatically closes ServerSocket
         * when the try block finishes.
         *
         * This is important because ServerSocket owns the
         * operating-system network resource for port 6379.
         *
         * Without proper closing, resources may remain allocated.
         */
		try (ServerSocket serverSocket = new ServerSocket(PORT)) {

			System.out.println("Mini Redis Server started on port " + PORT);

			
			 /*
             * accept() is a BLOCKING operation.
             *
             * The main server thread waits here until a
             * client connects to port 6379.
             *
             * Important:
             *
             * The main thread only ACCEPTS connections.
             * It does NOT perform the actual client processing.
             *
             * Flow:
             *
             * Client
             *    │
             *    ▼
             * ServerSocket.accept()
             *    │
             *    ▼
             * Socket
             *    │
             *    ▼
             * ThreadPoolExecutor
             *    │
             *    ├── Worker 1
             *    ├── Worker 2
             *    ├── Worker 3
             *    ├── Worker 4
             *    └── Worker 5
             */
			
			while (true) {

				Socket clientSocket = serverSocket.accept();
				
				/*
				 * A TCP connection has been established.
				 *
				 * But establishing a TCP connection does NOT mean we have
				 * enough server capacity to process it.
				 *
				 * We maintain our own application-level connection limit.
				 * 
				 *  1. Increment the atomic counter first to reserve a spot safely across multiple threads
				 */
				int currentConnections =
				        activeConnections.incrementAndGet();
				// 2. Check if this specific thread's increment pushed us over the allowed ceiling
				if (currentConnections > MAX_CONNECTIONS) {

				    /*
				     * We have exceeded our configured connection limit.
				     *
				     * Reject this client and immediately close its socket.
				     */
				    System.out.println(
				            "Maximum connections reached. "
				                    + "Rejecting client."
				    );

					/*
					 * 3. CRITICAL: Roll back the counter by 1 since this connection is being rejected.
					 *  Without this, the counter would stay permanently inflated.
					 */	
				    
				    activeConnections.decrementAndGet();

				    // 4. Sever the connection to the client immediately
				    clientSocket.close();
				    
				    // 5. Skip the rest of the processing loop and wait for the next client
				    continue;
				}

				System.out.println(
				        "Client connected. Active connections: "
				                + currentConnections
				);

				System.out.println("Client connected!");

				/*
                 * submit() does not make the main server thread
                 * execute handleClient().
                 *
                 * Instead, it submits the task to the executor.
                 *
                 * If a worker is available:
                 *
                 *     Task → Worker
                 *
                 * If all workers are busy:
                 *
                 *     Task → Queue
                 *
                 * If workers + queue are full:
                 *
                 *     RejectedExecutionException
                 */

				try {

					threadPool.submit(() -> {
						System.out.println(Thread.currentThread().getName() + " started handling client");
						System.out.println("Active workers: " + threadPool.getActiveCount() + " | Queue: "
								+ threadPool.getQueue().size());
						
						 /*
                         * The worker now owns the client connection
                         * and continuously reads commands from it.
                         *
                         * The main server thread is free to accept
                         * another client.
                         */
						handleClient(clientSocket);
					});

				} catch (RejectedExecutionException e) {

					 /*
                     * This happens when:
                     *
                     *     5 workers are busy
                     *     +
                     *     10 queue positions are occupied
                     *
                     * The server has reached its configured
                     * processing capacity.
                     *
                     * Instead of allowing unlimited tasks to
                     * accumulate, we reject the new connection.
                     *
                     * This is BACKPRESSURE.
                     */
					System.out.println("Server overloaded. Rejecting client.");
				    activeConnections.decrementAndGet();

					try {
						clientSocket.close();
					} catch (IOException ex) {
						ex.printStackTrace();
					}
				}

				  /*
                 * IMPORTANT:
                 *
                 * The main server thread does NOT wait for
                 * handleClient() to finish.
                 *
                 * It immediately returns to:
                 *
                 *     serverSocket.accept()
                 *
                 * Therefore:
                 *
                 * Client 1 → Worker 1
                 * Client 2 → Worker 2
                 * Client 3 → Worker 3
                 * ...
                 *
                 * while the main thread continues accepting
                 * new connections.
                 */
			}

		} catch (IOException e) { 
			 /*
             * Possible examples:
             *
             * - Port 6379 is already being used.
             * - ServerSocket cannot be created.
             * - Network-level I/O failure.
             */
			e.printStackTrace();
		} finally {
			/*
             * Stop accepting new tasks and allow already submitted
             * tasks to finish.
             *
             * Later we will improve this with a proper
             * GRACEFUL SHUTDOWN mechanism.
             */
			threadPool.shutdown();
		}
	}

	/**
	 * @param clientSocket
	 */
	private void handleClient(Socket clientSocket) {

		try (
				// : Sets up a mechanism to read data coming from the client.
				// clientSocket.getInputStream() grabs the raw, binary stream of bytes sent by
				// the client.
				// new InputStreamReader(...) converts those raw bytes into readable text
				// characters.
				// new BufferedReader(...) buffers the characters so the server can efficiently
				// read entire lines of text at a time.
				BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
				// Sets up a mechanism to send data back to the client.
				// clientSocket.getOutputStream() points to the network highway leading back to
				// the client.
				// true Activates auto-flush. This means every time you write something, it is
				// immediately sent across the network instead of waiting in a temporary memory
				// buffer.

				PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {

			// String command = reader.readLine();: This is another blocking line. The
			// server pauses here and waits until the client hits "Enter" (sends a newline
			// character \n). It reads that line of text and stores it in the command
			// variable.
			String command;
			while ((command = reader.readLine()) != null) {

				System.out.println("Received: " + command);

				String[] parts = command.split(" ", 3);

				// if ("PING".equalsIgnoreCase(command)):
				// Checks if the message received is "PING" (ignoring upper or lowercase
				// letters, so ping or Ping work too).writer.println("PONG");:
				// If it is "PING", the server sends "PONG" back to the client—which matches how
				// a real Redis server behaves!writer.println("Unknown command");:
				// If the client typed anything else, the server tells them it doesn't
				// understand.

				if ("PING".equalsIgnoreCase(parts[0])) {

					writer.println("PONG");

				} else if ("SET".equalsIgnoreCase(parts[0])) {

					if (parts.length < 3) {

						writer.println("ERROR: SET requires key and value");

					} else {

						String key = parts[1];

						String value = parts[2];

						database.set(key, value);

						writer.println("OK");
					}

				} else if ("GET".equalsIgnoreCase(parts[0])) {

					if (parts.length < 2) {

						writer.println("ERROR: GET requires a key");

					} else {

						String key = parts[1];

						String value = database.get(key);

						if (value == null) {

							writer.println("(nil)");

						} else {

							writer.println(value);
						}
					}

				} else if ("EXISTS".equalsIgnoreCase(parts[0])) {

					if (parts.length < 2) {
						writer.println("ERROR: EXISTS requires a key");
					} else {
						String key = parts[1];

						if (database.exists(key)) {
							writer.println("1");
						} else {
							writer.println("0");
						}
					}
				}

				else if ("DEL".equalsIgnoreCase(parts[0])) {

					if (parts.length < 2) {
						writer.println("ERROR: DEL requires a key");
					} else {
						String key = parts[1];

						if (database.delete(key)) {
							writer.println("1");
						} else {
							writer.println("0");
						}
					}
				} else if ("EXPIRE".equalsIgnoreCase(parts[0])) {

					if (parts.length < 3) {
						writer.println("ERROR: EXPIRE requires key and seconds");
					} else {

						String key = parts[1];

						try {
							long seconds = Long.parseLong(parts[2]);

							if (seconds <= 0) {
								writer.println("ERROR: seconds must be greater than 0");
							} else {

								boolean result = database.expire(key, seconds);

								writer.println(result ? "1" : "0");
							}

						} catch (NumberFormatException e) {
							writer.println("ERROR: invalid seconds");
						}
					}
				} else if ("TTL".equalsIgnoreCase(parts[0])) {

					if (parts.length < 2) {
						writer.println("ERROR: TTL requires a key");
					} else {

						String key = parts[1];

						long ttl = database.ttl(key);

						writer.println(ttl);
					}
				} else if ("INCR".equalsIgnoreCase(parts[0])) {

					if (parts.length < 2) {
						writer.println("ERROR: INCR requires a key");
					} else {

						try {

							long result = database.increment(parts[1]);

							writer.println(result);

						} catch (IllegalArgumentException e) {

							writer.println("ERROR: " + e.getMessage());
						}
					}
				} else if ("DECR".equalsIgnoreCase(parts[0])) {

					if (parts.length < 2) {
						writer.println("ERROR: DECR requires a key");
					} else {

						try {

							long result = database.decrement(parts[1]);

							writer.println(result);

						} catch (IllegalArgumentException e) {

							writer.println("ERROR: " + e.getMessage());
						}
					}
				
			} else if ("STATS".equalsIgnoreCase(parts[0])) {

				

					try {

						writer.println(
					            "Cache Hits: " + database.getCacheHits()
					    );

					    writer.println(
					            "Cache Misses: " + database.getCacheMisses()
					    );

					    writer.println(
					            "Cache Hit Rate: "
					            + String.format(
					                    "%.2f%%",
					                    database.getCacheHitRate()
					            )
					    );

					} catch (IllegalArgumentException e) {

						writer.println("ERROR: " + e.getMessage());
					}
			}

				else {

					writer.println("Unknown command");
				}
			}

		} catch (IOException e) {

			e.printStackTrace();

		} 
		
		// This block GUARANTEES cleanup code runs, even if the client crashes or throws an error.
		
		finally {
			//[ Connection Ends / Crashes ] ──► [ remove from Set ] ──► [ decrement count ] ──► [ close socket ]
		    // 1. Remove this specific client's socket from our master tracking list.
			activeSockets.remove(clientSocket);

		    // 2. Reduce the active connection counter by 1 to open up a slot for a new client.
			activeConnections.decrementAndGet();

		    // 3. Try to close the network connection to free up system resources.
			try {
				clientSocket.close();
				
			// 4. If the socket fails to close properly, catch the error so it doesn't crash the server.
			} catch (IOException e) {
				e.printStackTrace();
			}

			System.out.println("Client disconnected. Active connections: " + activeConnections.get());
		}
	}
	
	
	public void shutdownServer() {

	    System.out.println();
	    System.out.println("Starting graceful shutdown...");

	    /*
	     * Step 1:
	     * Stop accepting new work.
	     *
	     * The executor will finish tasks that were already
	     * submitted, but won't accept new ones.
	     */
	    threadPool.shutdown();

	    /*
	     * Step 2:
	     * Close all active client sockets.
	     *
	     * This is important because handleClient() may be blocked
	     * waiting for reader.readLine().
	     *
	     * Closing the socket causes the blocking read to return
	     * or throw an IOException, allowing the worker to finish.
	     */
	    for (Socket socket : activeSockets) {

	        try {
	            socket.close();
	        } catch (IOException e) {
	            System.out.println(
	                    "Error closing client socket"
	            );
	        }
	    }
	    database.closePersistence();

	    /*
	     * Step 3:
	     * Give currently running tasks a chance to finish.
	     */
	    try {

	        if (!threadPool.awaitTermination(
	                10,
	                TimeUnit.SECONDS
	        )) {

	            /*
	             * Some tasks did not finish within the
	             * graceful shutdown period.
	             *
	             * Now force interruption.
	             */
	            System.out.println(
	                    "Graceful shutdown timeout. "
	                            + "Forcing shutdown."
	            );

	            threadPool.shutdownNow();
	        }

	    } catch (InterruptedException e) {

	        /*
	         * Current shutdown thread was interrupted.
	         *
	         * Restore the interrupt flag and force shutdown.
	         */
	        Thread.currentThread().interrupt();

	        threadPool.shutdownNow();
	    }

	    System.out.println("Mini Redis Server stopped.");
	}

}