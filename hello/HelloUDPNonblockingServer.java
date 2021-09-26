package info.kgeorgiy.ja.nesterenko.hello;

import info.kgeorgiy.java.advanced.hello.HelloServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class HelloUDPNonblockingServer implements HelloServer {
    private int bufferSize;
    private ExecutorService senderService;
    private ExecutorService listener;
    private Selector selector;
    private DatagramChannel channel;

    private class Context {
        private final List<ByteBuffer> freeBuffers;
        private final List<Packet> packets;

        public Context(final int threads) {
            freeBuffers = new ArrayList<>(threads);
            IntStream.range(0, threads).forEach(i -> freeBuffers.add(ByteBuffer.allocate(bufferSize)));
            packets = new ArrayList<>();
        }

        synchronized void addBuffer(final ByteBuffer buffer) {
            if (freeBuffers.isEmpty()) {
                channel.keyFor(selector).interestOpsOr(SelectionKey.OP_READ);
                selector.wakeup();
            }
            freeBuffers.add(buffer);
        }

        synchronized ByteBuffer getBuffer() {
            final ByteBuffer buffer = freeBuffers.remove(freeBuffers.size() - 1);
            if (freeBuffers.isEmpty()) {
                channel.keyFor(selector).interestOpsAnd(~SelectionKey.OP_READ);
                selector.wakeup();
            }
            return buffer;
        }

        synchronized void addPacket(final ByteBuffer buffer, final SocketAddress address) {
            if (packets.isEmpty()) {
                channel.keyFor(selector).interestOpsOr(SelectionKey.OP_WRITE);
                selector.wakeup();
            }
            packets.add(new Packet(buffer, address));
        }

        synchronized Packet getPacket() {
            final Packet packet = packets.remove(packets.size() - 1);
            if (packets.isEmpty()) {
                channel.keyFor(selector).interestOpsAnd(~SelectionKey.OP_WRITE);
                selector.wakeup();
            }
            return packet;
        }

        private class Packet {
            private final ByteBuffer data;
            private final SocketAddress address;

            private Packet(ByteBuffer data, SocketAddress address) {
                this.data = data;
                this.address = address;
            }
        }
    }

    @Override
    public void start(int port, int threads) {
        try {
            selector = Selector.open();

            channel = DatagramChannel.open();
            channel.configureBlocking(false);
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.bind(new InetSocketAddress(port));
            bufferSize = channel.socket().getReceiveBufferSize();
            channel.register(selector, SelectionKey.OP_READ, new Context(threads));
        } catch (IOException e) {
            System.err.println("Failed to init connection: " + e.getMessage());
            return;
        }

        senderService = Executors.newFixedThreadPool(threads);
        listener = Executors.newSingleThreadExecutor();
        listener.submit(this::listen);
    }

    private void listen() {
        while (!Thread.interrupted() && !selector.keys().isEmpty()) {
            try {
                selector.select();
            } catch (IOException e) {
                System.err.println(e.getMessage());
                break;
            }
            for (Iterator<SelectionKey> it = selector.selectedKeys().iterator(); it.hasNext();) {
                final SelectionKey key = it.next();
                if (key.isReadable()) {
                    receive(key);
                }
                if (key.isWritable()) {
                    send(key);
                }
                it.remove();
            }
        }
    }

    private void receive(SelectionKey key) {
        final Context context = (Context) key.attachment();
        final ByteBuffer buffer = context.getBuffer();

        try {
            final SocketAddress address = channel.receive(buffer);
            senderService.submit(() -> {
                buffer.flip();
                final String request = StandardCharsets.UTF_8.decode(buffer).toString();
                final String response = "Hello, " + request;
                buffer.clear();
                buffer.put(response.getBytes());
                buffer.flip();
                context.addPacket(buffer, address);
            });
        } catch (IOException e) {
            System.err.println("Failed to receive client request: " + e.getMessage());
        }
    }

    private void send(SelectionKey key) {
        final Context context = (Context) key.attachment();
        final Context.Packet packet = context.getPacket();
        try {
            channel.send(packet.data, packet.address);
            context.addBuffer(packet.data.clear());
        } catch (IOException e) {
            System.err.println("Failed to send response: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
            selector.close();
            Shutdowns.shutdownAndAwaitTermination(senderService);
            Shutdowns.shutdownAndAwaitTermination(listener);
        } catch (IOException ignored) {

        }
    }

    public static void main(String[] args) {
        if (args == null || args.length != 2 || Arrays.stream(args).anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("Usage: HelloUDPNonblockingServer port threads");
        }
        new HelloUDPNonblockingServer().start(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
    }
}
