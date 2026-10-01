package bgu.spl.net.srv;

import java.io.Closeable;

/**
 * Handles the connection of a single client: receives its messages and sends
 * messages to it. Connections uses it to deliver messages to the client.
 *
 * @param <T> the type of message handled
 */
public interface ConnectionHandler<T> extends Closeable {

    /**
     * Sends a message to the client.
     *
     * @param msg the message to send
     */
    void send(T msg);

}
