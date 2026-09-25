package bgu.spl.net.srv;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionsImpl implements Connections<String> {

    private ConcurrentHashMap<Integer, ConnectionHandler<String>> clients;
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> channels;

    public ConnectionsImpl() {
        this.clients = new ConcurrentHashMap<>();
        this.channels = new ConcurrentHashMap<>();
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
        }
        for (ConcurrentHashMap<Integer, String> channel : channels.values()) {
            channel.remove(connectionId);
        }
    }
    
}
