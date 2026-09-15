package com.finapp.mini_redis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket; //Imports Java's standard client socket class. Unlike ServerSocket (which waits for incoming calls), this Socket class actively makes an outbound call to a target server.

public class MiniRedisClient {

	public static void main(String[] args) {

        try (
        		Socket socket = new Socket("localhost", 6379); //Instantiates a new client socket and immediately tries to establish a network connection.
        	BufferedReader serverReader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));

            PrintWriter writer = new PrintWriter(
                    socket.getOutputStream(), true);

            BufferedReader consoleReader = new BufferedReader(
                    new InputStreamReader(System.in))
    ) {

        System.out.println("Connected to Mini Redis!");
        System.out.println("Type commands:");
        System.out.println("SET key value");
        System.out.println("GET key");
        System.out.println("PING");
        System.out.println("Type 'exit' to quit");
        System.out.println("EXISTS name");
        System.out.println("DEL name");

        System.out.println();

        while (true) {

            System.out.print("> ");

            String command = consoleReader.readLine();

            if (command == null || "exit".equalsIgnoreCase(command)) {
                break;
            }

            if (command.trim().isEmpty()) {
                continue;
            }

            // Send command to Mini Redis server
            writer.println(command);

            // Read response from server
            String response = serverReader.readLine();

            System.out.println(response);
        }

        System.out.println("Disconnected from Mini Redis!");

    } catch (IOException e) {

        System.out.println("Could not connect to Mini Redis.");
        e.printStackTrace();
    }
}
}