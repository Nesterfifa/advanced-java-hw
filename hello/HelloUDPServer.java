package info.kgeorgiy.ja.nesterenko.hello;

import info.kgeorgiy.java.advanced.hello.HelloServer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class HelloUDPServer implements HelloServer {
    private DatagramSocket socket;
    private ExecutorService senderService;
    private ExecutorService listener;
    private BlockingQueue<DatagramPacket> packetPool;

    @Override
    public void start(int port, int threads) {
        try {
            socket = new DatagramSocket(port);
            final int bufferSize = socket.getReceiveBufferSize();
            packetPool = new ArrayBlockingQueue<>(threads);
            IntStream.range(0, threads).forEach(
                    i -> packetPool.add(new DatagramPacket(new byte[bufferSize], bufferSize)));
            senderService = Executors.newFixedThreadPool(threads);
            listener = Executors.newSingleThreadExecutor();
            listener.submit(this::submitTasks);
        } catch (SocketException e) {
            System.err.println("Failed to start UDP server: " + e.getMessage());
        }
    }

    private void submitTasks() {
        while (!socket.isClosed() && !Thread.interrupted()) {
            try {
                final DatagramPacket packet = packetPool.take();
                socket.receive(packet);
                senderService.submit(() -> {
                    try {
                        String response = "Hello, "
                                + new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);
                        packet.setData(response.getBytes(StandardCharsets.UTF_8));
                        socket.send(packet);
                        packetPool.add(packet);
                    } catch (IOException e) {
                        System.err.println("Failed to send response: " + e.getMessage());
                    }
                });
            } catch (InterruptedException e) {
                System.err.println("Failed to take free packet: " + e.getMessage());
            } catch (IOException e) {
                System.err.println("Failed to receive request: " + e.getMessage());
            }

        }
    }

    @Override
    public void close() {
        socket.close();
        Shutdowns.shutdownAndAwaitTermination(senderService);
        Shutdowns.shutdownAndAwaitTermination(listener);
    }

    public static void main(final String[] args) {
        if (args == null || args.length != 2 || Arrays.stream(args).anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("Usage: HelloUDPServer port threads");
        }
        new HelloUDPServer().start(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
    }
}