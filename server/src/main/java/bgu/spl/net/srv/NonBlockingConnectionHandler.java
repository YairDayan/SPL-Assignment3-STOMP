package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles a single client in the reactor server.
 * The reactor thread reads bytes from the channel, and the decoding and
 * processing are done by a worker thread. Messages to the client are queued by
 * send and written when the channel is ready for writing.
 *
 * @param <T> the type of message handled
 */
public class NonBlockingConnectionHandler<T> implements ConnectionHandler<T> {

    private static final int BUFFER_ALLOCATION_SIZE = 1 << 13;
    private static final ConcurrentLinkedQueue<ByteBuffer> BUFFER_POOL = new ConcurrentLinkedQueue<>();

    private final StompMessagingProtocol<T> protocol;
    private final MessageEncoderDecoder<T> encdec;
    private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
    private final SocketChannel chan;
    private final Reactor<T> reactor;
    private final Connections<T> connections;
    private final int connectionId;
    private final AtomicBoolean disconnectedNotified = new AtomicBoolean(false);

    /**
     * Creates a handler for a newly accepted client.
     *
     * @param reader       the encoder-decoder for this client
     * @param protocol     the protocol for this client
     * @param chan         the client's channel
     * @param reactor      the reactor that owns this handler
     * @param connectionId the connection id of this client
     * @param connections  the server's active connections
     */
    public NonBlockingConnectionHandler(
            MessageEncoderDecoder<T> reader,
            StompMessagingProtocol<T> protocol,
            SocketChannel chan,
            Reactor<T> reactor,
            int connectionId,
            Connections<T> connections) {
        this.chan = chan;
        this.encdec = reader;
        this.protocol = protocol;
        this.reactor = reactor;
        this.connectionId = connectionId;
        this.connections = connections;
    }

    /**
     * Reads the available bytes from the channel. Called by the reactor
     * thread.
     *
     * @return a task that decodes and processes the bytes read, or null if the
     *         client disconnected (in which case the handler is closed)
     */
    public Runnable continueRead() {
        ByteBuffer buf = leaseBuffer();
        boolean success = false;
        try {
            success = chan.read(buf) != -1;
        } catch (IOException ignored) {
            close();
        }

        if (success) {
            buf.flip();
            return () -> {
                try {
                    while (buf.hasRemaining()) {
                        T nextMessage = encdec.decodeNextByte(buf.get());
                        if (nextMessage != null) {
                            protocol.process(nextMessage);
                        }
                    }
                } finally {
                    releaseBuffer(buf);
                }
            };
        } else {
            releaseBuffer(buf);
            close();
            return null;
        }
    }

    /**
     * Closes the channel and removes the client from the connections.
     */
    @Override
    public void close() {
        try {
            chan.close();
        } catch (IOException ignored) {
        } finally {
            notifyDisconnectedOnce();
        }
    }

    /**
     * Checks whether the client's channel is closed.
     *
     * @return true if the channel is closed
     */
    public boolean isClosed() {
        return !chan.isOpen();
    }

    /**
     * Writes as much of the queued data as the channel accepts. Called by the
     * reactor thread. Once the queue is empty, closes the connection if the
     * protocol asked to terminate.
     */
    public void continueWrite() {
        while (!writeQueue.isEmpty()) {
            try {
                ByteBuffer top = writeQueue.peek();
                chan.write(top);
                if (top.hasRemaining())
                    return;
                writeQueue.remove();
            } catch (IOException ex) {
                close();
                return;
            }
        }

        if (writeQueue.isEmpty()) {
            if (protocol.shouldTerminate())
                close();
            else
                reactor.updateInterestedOps(chan, SelectionKey.OP_READ);
        }
    }

    /**
     * Queues a message to the client and asks the reactor to write it when the
     * channel is ready. May be called from any thread.
     *
     * @param msg the message to send
     */
    @Override
    public void send(T msg) {
        writeQueue.add(ByteBuffer.wrap(encdec.encode(msg)));
        reactor.updateInterestedOps(chan, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    /**
     * Takes a buffer from the pool, or allocates a new one if the pool is
     * empty.
     *
     * @return a cleared buffer
     */
    private static ByteBuffer leaseBuffer() {
        ByteBuffer buff = BUFFER_POOL.poll();
        if (buff == null)
            return ByteBuffer.allocateDirect(BUFFER_ALLOCATION_SIZE);
        buff.clear();
        return buff;
    }

    /**
     * Returns a buffer to the pool.
     *
     * @param buff the buffer to return
     */
    private static void releaseBuffer(ByteBuffer buff) {
        BUFFER_POOL.add(buff);
    }

    /**
     * Removes the client from the connections, only on the first call.
     */
    private void notifyDisconnectedOnce() {
        if (disconnectedNotified.compareAndSet(false, true)) {
            connections.disconnect(connectionId);
        }
    }
}
