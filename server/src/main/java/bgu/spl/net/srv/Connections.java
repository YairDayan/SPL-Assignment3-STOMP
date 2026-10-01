package bgu.spl.net.srv;

import java.util.Map;

/**
 * The active client connections of the server, and the channels they are subscribed to.
 * Each connection is identified by a unique connection id.
 *
 * @param <T> the type of message sent to clients
 */
public interface Connections<T> {

    /**
     * Sends a message to a single client.
     *
     * @param connectionId the client's connection id
     * @param msg          the message to send
     * @return true if the client is connected and the message was handed to it, false otherwise
     */
    boolean send(int connectionId, T msg);

    /**
     * Sends the same message to every client subscribed to a channel.
     *
     * @param channel the channel
     * @param msg     the message to send
     */
    void send(String channel, T msg);

    /**
     * Removes a client from the active connections, along with all its subscriptions.
     *
     * @param connectionId the client's connection id
     */
    void disconnect(int connectionId);

    /**
     * @param connectionId the client's connection id
     * @return true if the client is still an active connection
     */
    boolean isConnected(int connectionId);

    /**
     * Subscribes a client to a channel, creating the channel if it does not exist yet.
     *
     * @param channel        the channel
     * @param connectionId   the client's connection id
     * @param subscriptionId the id the client chose for this subscription
     * @return false if the client is already subscribed to the channel or already uses this
     *         subscription id, true otherwise
     */
    boolean subscribe(String channel, int connectionId, String subscriptionId);

    /**
     * Removes one of a client's subscriptions.
     *
     * @param connectionId   the client's connection id
     * @param subscriptionId the id of the subscription to remove
     * @return true if the subscription existed and was removed
     */
    boolean unsubscribe(int connectionId, String subscriptionId);

    /**
     * @param connectionId the client's connection id
     * @param channel      the channel
     * @return true if the client is subscribed to the channel
     */
    boolean isSubscribed(int connectionId, String channel);

    /**
     * @param channel the channel
     * @return a snapshot of the channel's subscribers, mapping each connection id to its
     *         subscription id; empty if the channel has no subscribers
     */
    Map<Integer, String> getSubscribers(String channel);
}
