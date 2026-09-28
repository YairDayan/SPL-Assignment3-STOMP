// server/src/main/java/bgu/spl/net/srv/ConnectionsImpl.java
package bgu.spl.net.srv;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionsImpl<T> implements Connections<T> {

    private final ConcurrentHashMap<Integer, ConnectionHandler<T>> clients;
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> channels;
    private final ConcurrentHashMap<String, User> users;

    public ConnectionsImpl() {
        this.clients = new ConcurrentHashMap<>();
        this.channels = new ConcurrentHashMap<>();
        this.users = new ConcurrentHashMap<>();
    }

    @Override
    public boolean send(int connectionId, T msg) {
        ConnectionHandler<T> handler = clients.get(connectionId);
        if (handler == null) {
            return false;
        }
        handler.send(msg);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void send(String channel, T msg) {
        ConcurrentHashMap<Integer, String> subscribers = channels.get(channel);
        if (subscribers == null) {
            return;
        }

        for (Map.Entry<Integer, String> entry : subscribers.entrySet()) {
            Integer clientId = entry.getKey();
            String subId = entry.getValue();
            if (subId == null) {
                continue;
            }

            T payload = msg;
            if (msg instanceof String) {
                payload = (T) ((String) msg).replace("%%SUB_ID%%", subId);
            }
            send(clientId, payload);
        }
    }

    @Override
    public void disconnect(int connectionId) {
        clients.remove(connectionId);

        for (User user : users.values()) {
            if (user.getConnectionId() == connectionId) {
                user.setConnected(false);
            }
        }

        for (ConcurrentHashMap<Integer, String> subscribers : channels.values()) {
            subscribers.remove(connectionId);
        }
    }

    public void addUser(User user) {
        users.putIfAbsent(user.getUsername(), user);
    }

    public void removeUser(String username) {
        users.remove(username);
    }

    public User getUser(String username) {
        return users.get(username);
    }

    public boolean isUserConnected(String username) {
        User user = users.get(username);
        return user != null && user.isConnected();
    }

    public void setUserConnected(String username, boolean connected) {
        users.computeIfPresent(username, (u, user) -> {
            user.setConnected(connected);
            return user;
        });
    }

    public boolean addSubscription(int connectionId, String channel, String subscriptionId) {
        ConcurrentHashMap<Integer, String> subscribers =
                channels.computeIfAbsent(channel, k -> new ConcurrentHashMap<>());
        return subscribers.putIfAbsent(connectionId, subscriptionId) == null;
    }

    public boolean removeSubscription(int connectionId, String subscriptionId) {
        for (ConcurrentHashMap<Integer, String> subscribers : channels.values()) {
            if (subscribers.remove(connectionId, subscriptionId)) {
                return true;
            }
        }
        return false;
    }

    public boolean isSubscribed(int connectionId, String channel) {
        if (channel == null) {
            return false;
        }
        ConcurrentHashMap<Integer, String> subscribers = channels.get(channel);
        return subscribers != null && subscribers.containsKey(connectionId);
    }

    public void addClient(int connectionId, ConnectionHandler<T> client) {
        clients.put(connectionId, client);
    }
}