package bgu.spl.net.api;

import bgu.spl.net.srv.Connections;

/**
 * The protocol that handles the messages of a single client.
 * Unlike a request-response protocol, process does not return a response;
 * every frame (to this client or to others) is sent through the Connections
 * object.
 *
 * @param <T> the type of message this protocol works with
 */
public interface StompMessagingProtocol<T> {

    /**
     * Initializes the protocol with the connection id of its client and the
     * server's active connections. Must complete before the first call to
     * process.
     *
     * @param connectionId the connection id of the client this protocol serves
     * @param connections  the active connections of the server
     */
    void start(int connectionId, Connections<T> connections);

    /**
     * Processes a message received from the client. Any response is sent
     * through the connections object.
     *
     * @param message the received message
     */
    void process(T message);

    /**
     * Checks whether the connection to the client should be closed.
     *
     * @return true if the connection should be terminated
     */
    boolean shouldTerminate();
}
