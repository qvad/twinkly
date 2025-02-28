package org.example;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class TwinklyProxyServer {
    private static final int PORT = 5431; // PostgreSQL default port
    private static final String PG_HOST = "localhost";
    private static final int PG_PORT = 5432;
    private static final String YB_HOST = "localhost";
    private static final int YB_PORT = 5433;

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Twinkly Proxy Server started on port " + PORT);
            ExecutorService executor = Executors.newCachedThreadPool();
            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(new ClientHandler(clientSocket));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

