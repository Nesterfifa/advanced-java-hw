package info.kgeorgiy.ja.nesterenko.hello;

import info.kgeorgiy.java.advanced.hello.HelloClient;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HelloUDPNonblockingClient implements HelloClient {
    private final Pattern RESPONSE_REGEXP = Pattern.compile("([^0-9]*)([0-9]+)([^0-9]+)([0-9]+)([^0-9]*)");

    private static class Context {
        final int threadId;
        int requestId;

        Context(int threadId, int requestId) {
            this.requestId = requestId;
            this.threadId = threadId;
        }
    }

    @Override
    public void run(String host, int port, String prefix, int threads, int requests) {
        final Selector selector;
        final SocketAddress address;

        try {
            selector = Selector.open();
            address = new InetSocketAddress(InetAddress.getByName(host), port);
        } catch (UnknownHostException e) {
            System.err.println("Unknown host: " + e.getMessage());
            return;
        } catch (IOException e) {
            System.err.println("Can't open selector: " + e.getMessage());
            return;
        }

        try {
            for (int thread = 0; thread < threads; thread++) {
                DatagramChannel datagramChannel = DatagramChannel.open();
                datagramChannel.configureBlocking(false);
                datagramChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                datagramChannel.connect(address);
                datagramChannel.register(selector, SelectionKey.OP_WRITE, new Context(thread, 0));
            }
        } catch (IOException e) {
            System.err.println("Channel configure error: " + e.getMessage());
            return;
        }

        while (!Thread.interrupted() && !selector.keys().isEmpty()) {
            try {
                selector.select(100);
            } catch (IOException e) {
                System.err.println(e.getMessage());
                break;
            }
            if (selector.selectedKeys().isEmpty()) {
                for (final SelectionKey key : selector.keys()) {
                    send(prefix, address, key);
                }
            } else {
                for (Iterator<SelectionKey> it = selector.selectedKeys().iterator(); it.hasNext();) {
                    final SelectionKey key = it.next();
                    if (key.isWritable()) {
                        key.interestOps(SelectionKey.OP_WRITE);
                        send(prefix, address, key);
                    } else if (key.isReadable()) {
                        receive(key, requests, prefix, address);
                    }
                    it.remove();
                }
            }
        }
    }

    private void receive(final SelectionKey key, final int requests, final String prefix, final SocketAddress address) {
        final DatagramChannel channel = (DatagramChannel) key.channel();
        final ByteBuffer receiveData;
        try {
            receiveData = ByteBuffer.allocate(channel.socket().getReceiveBufferSize());
            receiveData.clear();
            channel.receive(receiveData);   
        } catch (IOException e) {
            System.err.println("Failed to receive response: " + e.getMessage());
            return;
        }
        final String response = new String(receiveData.array(), StandardCharsets.UTF_8);
        final Context responseContext = (Context) key.attachment();
        final Matcher matcher = RESPONSE_REGEXP.matcher(response);
        if (matcher.matches()
                && matcher.group(2).equals(Integer.toString(responseContext.threadId))
                && matcher.group(4).equals(Integer.toString(responseContext.requestId))) {
            if (responseContext.requestId == requests - 1) {
                try {
                    key.channel().close();
                } catch (IOException ignored) {
                }
            } else {
                responseContext.requestId++;
                key.attach(responseContext);
                key.interestOps(SelectionKey.OP_WRITE);
            }
        } else {
            send(prefix, address, key);
        }
    }

    private void send(final String prefix, final SocketAddress address, final SelectionKey key) {
        final Context requestContext = (Context) key.attachment();
        DatagramChannel channel = (DatagramChannel) key.channel();
        try {
            ByteBuffer buffer = ByteBuffer.allocate(channel.socket().getSendBufferSize());
            String request = prefix + requestContext.threadId + "_" + requestContext.requestId;
            buffer.clear();
            buffer.put(request.getBytes());
            buffer.flip();
            channel.send(buffer, address);
        } catch (IOException e) {
            System.err.println("Failed to send request: " + e.getMessage());
        }
        key.interestOps(SelectionKey.OP_READ);
    }

    public static void main(String[] args) {
        if (args == null || args.length != 5 || Arrays.stream(args).anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("Usage: HelloUDPNonblockingClient host port prefix threads requests");
        }
        final int port = Integer.parseInt(args[1]);
        final int threads = Integer.parseInt(args[3]);
        final int requests = Integer.parseInt(args[4]);
        new HelloUDPNonblockingClient().run(args[0], port, args[2], threads, requests);
    }
}
