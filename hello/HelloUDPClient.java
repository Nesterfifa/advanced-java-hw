package info.kgeorgiy.ja.nesterenko.hello;

import info.kgeorgiy.java.advanced.hello.HelloClient;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class HelloUDPClient implements HelloClient {
    private final Pattern RESPONSE_REGEXP = Pattern.compile("([^0-9]*)([0-9]+)([^0-9]+)([0-9]+)([^0-9]*)");

    @Override
    public void run(String host, int port, String prefix, int threads, int requests) {
        try {
            final SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByName(host), port);
            final ExecutorService senderService = Executors.newFixedThreadPool(threads);
            IntStream.range(0, threads).forEach(threadIndex ->
                    senderService.submit(() -> sendAndReceive(socketAddress, prefix, threadIndex, requests)));
            Shutdowns.shutdownAndAwaitTermination(senderService);
        } catch (UnknownHostException e) {
            System.err.println("Unknown host: " + e.getMessage());
        }
    }

    private void sendAndReceive(final SocketAddress socketAddress,
                                final String prefix,
                                final int threadIndex,
                                final int requests) {
        try (final DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(100);
            final DatagramPacket packet = new DatagramPacket(
                    new byte[0],
                    0,
                    socketAddress);
            for (int request = 0; request < requests; request++) {
                final String requestString = prefix + threadIndex + "_" + request;
                while (!socket.isClosed() && !Thread.interrupted()) {
                    try {
                        packet.setData(requestString.getBytes(StandardCharsets.UTF_8));
                        socket.send(packet);
                    } catch (final IOException e) {
                        System.err.println("Failed to send request: " + e.getMessage());
                        continue;
                    }
                    try {
                        packet.setData(new byte[socket.getReceiveBufferSize()]);
                        socket.receive(packet);
                        final String responseString = new String(
                                packet.getData(),
                                packet.getOffset(),
                                packet.getLength(),
                                StandardCharsets.UTF_8);
                        final Matcher matcher = RESPONSE_REGEXP.matcher(responseString);
                        if (matcher.matches()
                                && matcher.group(2).equals(Integer.toString(threadIndex))
                                && matcher.group(4).equals(Integer.toString(request))) {
                            break;
                        }
                    } catch (final IOException e) {
                        System.err.println("Failed to receive response: " + e.getMessage());
                    }
                }
            }
        } catch (final SocketException e) {
            System.err.println("Bad socket " + threadIndex + ":" + e.getMessage());
        }
    }

    public static void main(final String[] args) {
        if (args == null || args.length != 5 || Arrays.stream(args).anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("Usage: HelloUDPClient host port prefix threads requests");
        }
        final int port = Integer.parseInt(args[1]);
        final int threads = Integer.parseInt(args[3]);
        final int requests = Integer.parseInt(args[4]);
        new HelloUDPClient().run(args[0], port, args[2], threads, requests);
    }
}
