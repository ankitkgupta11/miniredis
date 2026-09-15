package com.finapp.mini_redis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket; //Imports the Java class responsible for listening for incoming network requests over TCP.
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MiniRedisServer {

	private static final int PORT = 6379;
	private final MiniRedisDatabase database;
	private final ExecutorService threadPool; // initialize a pool of exactly any reusable worker threads

	public MiniRedisServer() {

		database = new MiniRedisDatabase();
		int numberOfThreads = Runtime.getRuntime().availableProcessors();
		numberOfThreads = 5;
		threadPool = Executors.newFixedThreadPool(numberOfThreads); // Create a pool of exactly numberOfThreads reusable
																	// worker
																	// threads

		System.out.println("Thread pool created with " + numberOfThreads + " threads");
	}

	/**
	 * 
	 */
	public void start() {

		try (ServerSocket serverSocket = new ServerSocket(PORT)) { // It uses Java's Try-with-Resources statement. This
																	// ensures that when the try block finishes (or
																	// crashes), the serverSocket is automatically
																	// closed, freeing up the network port for other
																	// applications.

			System.out.println("Mini Redis Server started on port " + PORT);

			while (true) { // Creates an infinite loop. Because a database server needs to run continuously
							// to accept requests indefinitely, this loop will keep running until you
							// manually stop the program.

				Socket clientSocket = serverSocket.accept(); // This is a blocking method. The program stops and waits
																// at this exact line until a client (like a terminal or
																// another app) tries to connect to port 6379. Once a
																// client connects, it establishes the connection and
																// execution moves to the next line.

				System.out.println("Client connected!");

				// Instead of hiring a new worker thread for every client and letting them die
				// afterward, you should use a fixed pool of reusable threads. The server
				// creates a set number of workers once, and they wait around to pick up client
				// tasks from a queue.
				threadPool.submit(() -> {
					handleClient(clientSocket); // PASS THE TASK TO THE POOL INSTEAD OF CREATING A NEW THREAD

				});

				// What happens next: Because clientThread.start() is non-blocking, the code
				// immediately loops back up to while (true) and hits serverSocket.accept()
				// again. The server is now ready to receive a second client, even if the first
				// client is still connected and talking to the background thread!
			}

		} catch (IOException e) { // Catches any network errors that happen inside the try block (for example, if
									// port 6379 is already being used by another program).
			e.printStackTrace();
		} finally {

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
				}

				else {

					writer.println("Unknown command");
				}
			}

		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				clientSocket.close();

			} catch (IOException e) {

				e.printStackTrace();
			}
		}
	}

}