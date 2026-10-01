package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;
import java.io.Closeable;
import java.util.function.Supplier;

/**
 * A server that accepts clients and handles their messages.
 * Instances are created with the threadPerClient and reactor factory methods.
 *
 * @param <T> the type of message handled
 */
public interface Server<T> extends Closeable {

    /**
     * The main loop of the server. Starts listening and handling new clients.
     */
    void serve();

    /**
     * Creates a server that handles each client on its own thread.
     *
     * @param port                  the port for the server socket
     * @param protocolFactory       creates a new StompMessagingProtocol for each client
     * @param encoderDecoderFactory creates a new MessageEncoderDecoder for each client
     * @param <T>                   the type of message handled
     * @return a new thread-per-client server
     */
    public static <T> Server<T> threadPerClient(
            int port,
            Supplier<StompMessagingProtocol<T>> protocolFactory,
            Supplier<MessageEncoderDecoder<T>> encoderDecoderFactory) {

        return new BaseServer<T>(port, protocolFactory, encoderDecoderFactory) {
            @Override
            protected void execute(BlockingConnectionHandler<T> handler) {
                new Thread(handler).start();
            }
        };

    }

    /**
     * Creates a server that uses the reactor pattern.
     *
     * @param nthreads              the number of threads available for protocol processing
     * @param port                  the port for the server socket
     * @param protocolFactory       creates a new StompMessagingProtocol for each client
     * @param encoderDecoderFactory creates a new MessageEncoderDecoder for each client
     * @param <T>                   the type of message handled
     * @return a new reactor server
     */
    public static <T> Server<T> reactor(
            int nthreads,
            int port,
            Supplier<StompMessagingProtocol<T>> protocolFactory,
            Supplier<MessageEncoderDecoder<T>> encoderDecoderFactory) {
        return new Reactor<T>(nthreads, port, protocolFactory, encoderDecoderFactory);
    }

}
