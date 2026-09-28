package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.MessagingProtocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingConnectionHandler<T> implements Runnable, ConnectionHandler<T> {

    private final MessagingProtocol<T> protocol;
    private final MessageEncoderDecoder<T> encdec;
    private final Socket sock;
    private final BufferedInputStream in;
    private final BufferedOutputStream out;
    private final Connections<T> connections;
    private final int connectionId;

    private final AtomicBoolean disconnectedNotified = new AtomicBoolean(false);
    private volatile boolean connected = true;

    public BlockingConnectionHandler(
            Socket sock,
            MessageEncoderDecoder<T> reader,
            MessagingProtocol<T> protocol,
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

    @Override
    public void run() {
        try (Socket ignored = this.sock) {
            int read;
            while (!protocol.shouldTerminate() && connected && (read = in.read()) >= 0) {
                T nextMessage = encdec.decodeNextByte((byte) read);
                if (nextMessage != null) {
                    T response = protocol.process(nextMessage);
                    if (response != null) {
                        send(response);
                    }
                }
            }
        } catch (IOException ignored) {
        } finally {
            connected = false;
            notifyDisconnectedOnce();
        }
    }

    @Override
    public void close() throws IOException {
        connected = false;
        sock.close();
        notifyDisconnectedOnce();
    }

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

    private void notifyDisconnectedOnce() {
        if (disconnectedNotified.compareAndSet(false, true)) {
            connections.disconnect(connectionId);
        }
    }
}