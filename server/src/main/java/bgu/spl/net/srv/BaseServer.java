package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A server that accepts clients on a blocking server socket and creates a
 * BlockingConnectionHandler for each one. Subclasses decide how each handler is run.
 *
 * @param <T> the type of message handled
 */
public abstract class BaseServer<T> implements Server<T> {

    private final int port;
    private final Supplier<StompMessagingProtocol<T>> protocolFactory;
    private final Supplier<MessageEncoderDecoder<T>> encdecFactory;
    private final ConnectionsImpl<T> connections = new ConnectionsImpl<>();
    private final AtomicInteger connectionIds = new AtomicInteger(0);
    private ServerSocket sock;

    /**
     * @param port            the port to listen on
     * @param protocolFactory creates a new protocol for each client
     * @param encdecFactory   creates a new encoder-decoder for each client
     */
    public BaseServer(
            int port,
            Supplier<StompMessagingProtocol<T>> protocolFactory,
            Supplier<MessageEncoderDecoder<T>> encdecFactory) {
        this.port = port;
        this.protocolFactory = protocolFactory;
        this.encdecFactory = encdecFactory;
        this.sock = null;
    }

    /**
     * Accepts clients until the thread is interrupted or the server socket is closed.
     * Each client is given a unique connection id and registered in the connections, and its
     * protocol is started before its handler begins running.
     */
    @Override
    public void serve() {
        try (ServerSocket serverSock = new ServerSocket(port)) {
            System.out.println("Server started");
            this.sock = serverSock;

            while (!Thread.currentThread().isInterrupted()) {
                Socket clientSock = serverSock.accept();

                StompMessagingProtocol<T> protocol = protocolFactory.get();
                int connectionId = connectionIds.getAndIncrement();

                BlockingConnectionHandler<T> handler = new BlockingConnectionHandler<>(
                        clientSock,
                        encdecFactory.get(),
                        protocol,
                        connectionId,
                        connections);

                connections.addClient(connectionId, handler);
                protocol.start(connectionId, connections);

                execute(handler);
            }
        } catch (IOException ignored) {
        }

        System.out.println("server closed!!!");
    }

    /**
     * Closes the server socket, which stops serve.
     *
     * @throws IOException if closing the socket fails
     */
    @Override
    public void close() throws IOException {
        if (sock != null) {
            sock.close();
        }
    }

    /**
     * Runs the handler of a newly accepted client.
     *
     * @param handler the handler to run
     */
    protected abstract void execute(BlockingConnectionHandler<T> handler);
}
