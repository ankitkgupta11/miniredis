package com.finapp.mini_redis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AOFManager {

    private final String filePath;

    private BufferedWriter writer;

    public AOFManager(String filePath) {

        this.filePath = filePath;

        try {

            /*
             * true = append mode.
             *
             * This is important.
             *
             * We don't want every SET to overwrite
             * the existing AOF file.
             *
             * Example:
             *
             * Existing:
             * SET A 10
             *
             * New command:
             * SET B 20
             *
             * Result:
             *
             * SET A 10
             * SET B 20
             */
            writer = new BufferedWriter(
                    new FileWriter(filePath, true)
            );

        } catch (IOException e) {

            throw new RuntimeException(
                    "Unable to open AOF file",
                    e
            );
        }
    }

    public synchronized void appendCommand(
            String command) {

        try {

            writer.write(command);
            writer.newLine();

            /*
             * flush() pushes buffered data toward
             * the operating system.
             *
             * Without flushing, the command may still
             * be sitting inside Java's buffer when the
             * application crashes.
             */
            writer.flush();

        } catch (IOException e) {

            throw new RuntimeException(
                    "Unable to write to AOF",
                    e
            );
        }
    }

    public synchronized void close() {

        try {

            writer.close();

        } catch (IOException e) {

            e.printStackTrace();
        }
    }
    
    public synchronized List<String> readCommands() {

        List<String> commands = new ArrayList<>();

        File file = new File(filePath);

        if (!file.exists()) {
            return commands;
        }

        try (BufferedReader reader =
                     new BufferedReader(
                             new FileReader(file))) {

            String line;

            while ((line = reader.readLine()) != null) {

                if (!line.isBlank()) {
                    commands.add(line);
                }
            }

        } catch (IOException e) {

            throw new RuntimeException(
                    "Unable to read AOF",
                    e
            );
        }

        return commands;
    }
    
    
}