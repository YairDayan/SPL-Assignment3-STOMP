package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.MessagingProtocol;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public abstract class BaseServer<T> implements Server<T> {

    private final int port;
    private final Supplier<MessagingProtocol<T>> protocolFactory;
    private final Supplier<MessageEncoderDecoder<T>> encdecFactory;
    private final ConnectionsImpl<T> connections = new ConnectionsImpl<>();
    private final AtomicInteger connectionIds = new AtomicInteger(0);
    private ServerSocket sock;

    public BaseServer(
            int port,
            Supplier<MessagingProtocol<T>> protocolFactory,
            Supplier<MessageEncoderDecoder<T>> encdecFactory) {
        this.port = port;
        this.protocolFactory = protocolFactory;
        this.encdecFactory = encdecFactory;
        this.sock = null;
    }

    @Override
    public void serve() {
        try (ServerSocket serverSock = new ServerSocket(port)) {
            System.out.println("Server started");
            this.sock = serverSock;

            while (!Thread.currentThread().isInterrupted()) {
                Socket clientSock = serverSock.accept();

                MessagingProtocol<T> protocol = protocolFactory.get();
                int connectionId = connectionIds.getAndIncrement();

                BlockingConnectionHandler<T> handler = new BlockingConnectionHandler<>(
                        clientSock,
                        encdecFactory.get(),
                        protocol,
                        connectionId,
                        connections
                );

                connections.addClient(connectionId, handler);
                protocol.start(connectionId, connections);

                execute(handler);
            }
        } catch (IOException ignored) {
        }

        System.out.println("server closed!!!");
    }

    @Override
    public void close() throws IOException {
        if (sock != null) {
            sock.close();
        }
    }

    protected abstract void execute(BlockingConnectionHandler<T> handler);
}