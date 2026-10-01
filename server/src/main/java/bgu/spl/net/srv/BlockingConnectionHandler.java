package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles a single client in the thread-per-client server.
 * Runs on its own thread, reading bytes from the socket and passing complete messages to the
 * protocol. Messages to the client are written by send, which may be called from other threads.
 *
 * @param <T> the type of message handled
 */
public class BlockingConnectionHandler<T> implements Runnable, ConnectionHandler<T> {

    private final StompMessagingProtocol<T> protocol;
    private final MessageEncoderDecoder<T> encdec;
    private final Socket sock;
    private final BufferedInputStream in;
    private final BufferedOutputStream out;
    private final Connections<T> connections;
    private final int connectionId;

    private final AtomicBoolean disconnectedNotified = new AtomicBoolean(false);
    private volatile boolean connected = true;

    /**
     * @param sock         the client's socket
     * @param reader       the encoder-decoder for this client
     * @param protocol     the protocol for this client
     * @param connectionId the connection id of this client
     * @param connections  the server's active connections
     * @throws RuntimeException if the socket's streams cannot be opened
     */
    public BlockingConnectionHandler(
            Socket sock,
            MessageEncoderDecoder<T> reader,
            StompMessagingProtocol<T> protocol,
            int connectionId,
            Connections<T> connections) {
        this.sock = sock;
        this.encdec = reader;
        this.protocol = protocol;
        this.connectionId = connectionId;
        this.connections = connections;
        try {
            this.in = new BufferedInputStream(sock.getInputStream());
            this.out = new BufferedOutputStream(sock.getOutputStream());
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize connection streams", e);
        }
    }

    /**
     * Reads from the socket until the client disconnects or the protocol asks to terminate,
     * then closes the socket and removes the client from the connections.
     */
    @Override
    public void run() {
        try (Socket ignored = this.sock) {
            int read;
            while (!protocol.shouldTerminate() && connected && (read = in.read()) >= 0) {
                T nextMessage = encdec.decodeNextByte((byte) read);
                if (nextMessage != null) {
                    protocol.process(nextMessage);
                }
            }
        } catch (IOException ignored) {
        } finally {
            connected = false;
            notifyDisconnectedOnce();
        }
    }

    /**
     * Closes the socket and removes the client from the connections.
     *
     * @throws IOException if closing the socket fails
     */
    @Override
    public void close() throws IOException {
        connected = false;
        sock.close();
        notifyDisconnectedOnce();
    }

    /**
     * Encodes and writes a message to the client. Does nothing if the client is no longer
     * connected; a write failure closes the connection.
     *
     * @param msg the message to send
     */
    @Override
    public void send(T msg) {
        if (!connected) return;
        synchronized (out) {
            try {
                out.write(encdec.encode(msg));
                out.flush();
            } catch (IOException e) {
                connected = false;
                try {
                    close();
                } catch (IOException ignored) {
                }
            }
        }
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
