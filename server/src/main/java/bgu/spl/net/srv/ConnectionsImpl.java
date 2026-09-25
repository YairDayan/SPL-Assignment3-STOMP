package bgu.spl.net.srv;

import java.util.concurrent.ConcurrentHashMap;

public class ConnectionsImpl implements Connections<String> {

    private ConcurrentHashMap<Integer, ConnectionHandler<String>> clients;
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> channels;
    private ConcurrentHashMap<String, User> users;

    public ConnectionsImpl() {
        this.clients = new ConcurrentHashMap<>();
        this.channels = new ConcurrentHashMap<>();
        this.users = new ConcurrentHashMap<>();
    }

    @Override
    public boolean send(int connectionId, String msg) {
        if (clients.containsKey(connectionId)) {
            clients.get(connectionId).send(msg);
            return true;
        }
        return false;
    }

    @Override
    public void send(String channel, String msg) {
        if (channels.containsKey(channel)) {
            for (Integer clientId : channels.get(channel).keySet()) {
                String subID = channels.get(channel).get(clientId);
                String response = msg.replace("%%SUB_ID%%", subID);
                send(clientId, response);

            }
        }
    }

    @Override
    public void disconnect(int connectionId) {
        if (clients.containsKey(connectionId)) {
            clients.remove(connectionId);
            for (User user : users.values()) {
                if (user.getConnectionId() == connectionId) {
                    user.setConnected(false);
                }
            }
        }
        for (String channel : channels.keySet()) {
            if (channels.get(channel).containsKey(connectionId)) {
                channels.get(channel).remove(connectionId);
            }
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
        return users.get(username).isConnected();
    }

    public void setUserConnected(String username, boolean connected) {
        users.get(username).setConnected(connected);
    }

    public boolean addSubscription(int connectionId, String channel, String subscriptionId) {
        channels.putIfAbsent(channel, new ConcurrentHashMap<>());
        return channels.get(channel).putIfAbsent(connectionId, subscriptionId) == null;
    }

    public boolean removeSubscription(int connectionId, String subscriptionId) {
        for (ConcurrentHashMap<Integer, String> subscribers : channels.values()) {
            String subID = subscribers.get(connectionId);
            if (subID != null && subID.equals(subscriptionId)) {
                subscribers.remove(connectionId);
                return true;
            }
        }
        return false;
    }

    public boolean isSubscribed(int connectionId, String channel) {
        return channel != null && channels.containsKey(channel) && channels.get(channel).containsKey(connectionId);
    }
}
