package bgu.spl.net.srv;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe implementation of Connections.
 * Holds the connection handler of every active client and the subscribers of every channel.
 *
 * @param <T> the type of message sent to clients
 */
public class ConnectionsImpl<T> implements Connections<T> {
    private final ConcurrentHashMap<Integer, ConnectionHandler<T>> clients;
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> channels;

    /**
     * Creates an empty Connections, with no clients and no channels.
     */
    public ConnectionsImpl() {
        this.clients = new ConcurrentHashMap<>();
        this.channels = new ConcurrentHashMap<>();
    }

    /**
     * Registers a newly connected client.
     *
     * @param connectionId the client's connection id
     * @param client       the client's connection handler
     */
    public void addClient(int connectionId, ConnectionHandler<T> client) {
        clients.put(connectionId, client);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean send(int connectionId, T msg) {
        ConnectionHandler<T> handler = clients.get(connectionId);
        if (handler == null) {
            return false;
        }
        handler.send(msg);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void send(String channel, T msg) {
        for (Integer connectionId : getSubscribers(channel).keySet()) {
            send(connectionId, msg);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void disconnect(int connectionId) {
        clients.remove(connectionId);
        for (ConcurrentHashMap<Integer, String> subscribers : channels.values()) {
            subscribers.remove(connectionId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isConnected(int connectionId) {
        return clients.containsKey(connectionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean subscribe(String channel, int connectionId, String subscriptionId) {
        for (ConcurrentHashMap<Integer, String> subscribers : channels.values()) {
            if (subscriptionId.equals(subscribers.get(connectionId))) {
                return false;
            }
        }
        ConcurrentHashMap<Integer, String> subscribers =
                channels.computeIfAbsent(channel, k -> new ConcurrentHashMap<>());
        return subscribers.putIfAbsent(connectionId, subscriptionId) == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean unsubscribe(int connectionId, String subscriptionId) {
        for (ConcurrentHashMap<Integer, String> subscribers : channels.values()) {
            if (subscribers.remove(connectionId, subscriptionId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSubscribed(int connectionId, String channel) {
        ConcurrentHashMap<Integer, String> subscribers = channels.get(channel);
        return subscribers != null && subscribers.containsKey(connectionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Integer, String> getSubscribers(String channel) {
        ConcurrentHashMap<Integer, String> subscribers = channels.get(channel);
        if (subscribers == null) {
            return Collections.emptyMap();
        }
        return new HashMap<>(subscribers);
    }
}
