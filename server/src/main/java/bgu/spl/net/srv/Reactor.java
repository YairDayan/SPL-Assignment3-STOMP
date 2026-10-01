package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A server that handles all clients with a single selector thread.
 * The selector thread accepts clients and does the socket I/O, while decoding and processing
 * messages is done by a pool of worker threads.
 *
 * @param <T> the type of message handled
 */
public class Reactor<T> implements Server<T> {

    private final int port;
    private final Supplier<StompMessagingProtocol<T>> protocolFactory;
    private final Supplier<MessageEncoderDecoder<T>> readerFactory;
    private final ActorThreadPool pool;
    private final ConnectionsImpl<T> connections = new ConnectionsImpl<>();
    private final AtomicInteger connectionIds = new AtomicInteger(0);

    private Selector selector;
    private Thread selectorThread;
    private final ConcurrentLinkedQueue<Runnable> selectorTasks = new ConcurrentLinkedQueue<>();

    /**
     * @param numThreads      the number of worker threads
     * @param port            the port to listen on
     * @param protocolFactory creates a new protocol for each client
     * @param readerFactory   creates a new encoder-decoder for each client
     */
    public Reactor(
            int numThreads,
            int port,
            Supplier<StompMessagingProtocol<T>> protocolFactory,
            Supplier<MessageEncoderDecoder<T>> readerFactory) {

        this.pool = new ActorThreadPool(numThreads);
        this.port = port;
        this.protocolFactory = protocolFactory;
        this.readerFactory = readerFactory;
    }

    /**
     * Runs the selector loop until the thread is interrupted or the selector is closed,
     * then shuts down the worker pool.
     */
    @Override
    public void serve() {
        selectorThread = Thread.currentThread();
        try (Selector selector = Selector.open();
                ServerSocketChannel serverSock = ServerSocketChannel.open()) {

            this.selector = selector;
            serverSock.bind(new InetSocketAddress(port));
            serverSock.configureBlocking(false);
            serverSock.register(selector, SelectionKey.OP_ACCEPT);
            System.out.println("Server started");

            while (!Thread.currentThread().isInterrupted()) {
                selector.select();
                runSelectionThreadTasks();

                for (SelectionKey key : selector.selectedKeys()) {
                    if (!key.isValid()) {
                        continue;
                    } else if (key.isAcceptable()) {
                        handleAccept(serverSock, selector);
                    } else {
                        handleReadWrite(key);
                    }
                }
                selector.selectedKeys().clear();
            }

        } catch (ClosedSelectorException ignored) {
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println("server closed!!!");
        pool.shutdown();
    }

    /**
     * Changes the operations the selector waits for on a channel.
     * Selection keys may only be changed by the selector thread, so a call from another thread
     * is queued and the selector is woken up to run it.
     *
     * @param chan the client's channel
     * @param ops  the new interest set
     */
    void updateInterestedOps(SocketChannel chan, int ops) {
        final SelectionKey key = chan.keyFor(selector);
        if (key == null || !key.isValid()) {
            return;
        }

        if (Thread.currentThread() == selectorThread) {
            key.interestOps(ops);
        } else {
            selectorTasks.add(() -> {
                SelectionKey k = chan.keyFor(selector);
                if (k != null && k.isValid()) {
                    k.interestOps(ops);
                }
            });
            selector.wakeup();
        }
    }

    /**
     * Accepts a new client, gives it a unique connection id, registers it in the connections
     * and starts its protocol before registering it for reading, so start completes before
     * any call to process.
     *
     * @param serverChan the server channel
     * @param selector   the selector to register the client with
     * @throws IOException if accepting or configuring the client's channel fails
     */
    private void handleAccept(ServerSocketChannel serverChan, Selector selector) throws IOException {
        SocketChannel clientChan = serverChan.accept();
        clientChan.configureBlocking(false);

        StompMessagingProtocol<T> protocol = protocolFactory.get();
        int connectionId = connectionIds.getAndIncrement();

        NonBlockingConnectionHandler<T> handler = new NonBlockingConnectionHandler<>(
                readerFactory.get(),
                protocol,
                clientChan,
                this,
                connectionId,
                connections);

        connections.addClient(connectionId, handler);
        protocol.start(connectionId, connections);

        clientChan.register(selector, SelectionKey.OP_READ, handler);
    }

    /**
     * Handles a client's channel that is ready for reading and/or writing.
     * Read data is processed by the worker pool; writing is done on this thread.
     *
     * @param key the client's selection key
     */
    private void handleReadWrite(SelectionKey key) {
        @SuppressWarnings("unchecked")
        NonBlockingConnectionHandler<T> handler = (NonBlockingConnectionHandler<T>) key.attachment();

        if (key.isReadable()) {
            Runnable task = handler.continueRead();
            if (task != null) {
                pool.submit(handler, task);
            }
        }

        if (key.isValid() && key.isWritable()) {
            handler.continueWrite();
        }
    }

    /**
     * Runs the tasks queued for the selector thread.
     */
    private void runSelectionThreadTasks() {
        while (!selectorTasks.isEmpty()) {
            selectorTasks.remove().run();
        }
    }

    /**
     * Closes the selector, which stops serve.
     *
     * @throws IOException if closing the selector fails
     */
    @Override
    public void close() throws IOException {
        selector.close();
    }
}
