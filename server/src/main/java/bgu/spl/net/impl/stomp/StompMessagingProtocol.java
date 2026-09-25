package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.ConnectionsImpl;
import bgu.spl.net.srv.User;
import bgu.spl.net.api.MessagingProtocol;

public class StompMessagingProtocol implements MessagingProtocol<String> {
    private boolean shouldTerminate = false;
    private int connectionId;
    private ConnectionsImpl connectionsImpl;

    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connectionsImpl = (ConnectionsImpl) connections;
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

    @Override
    public String process(String message) {
        StompMessage stompMessage = new StompMessage(message);
        switch (stompMessage.getCommand()) {
            case "CONNECT":
                return handleConnect(stompMessage);
            case "SUBSCRIBE":
                return handleSubscribe(stompMessage);
            case "UNSUBSCRIBE":
                return handleUnsubscribe(stompMessage);
            case "SEND":
                return handleSend(stompMessage);
            case "DISCONNECT":
                return handleDisconnect(stompMessage);
            default:
                return null;
        }
    }

    

    private String handleDisconnect(StompMessage stompMessage) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handleDisconnect'");
    }

    private String handleSend(StompMessage stompMessage) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handleSend'");
    }

    private String handleUnsubscribe(StompMessage stompMessage) {
        String subscriptionId = stompMessage.getHeader("id");
        String receipt = stompMessage.getHeader("receipt");

        if (subscriptionId == null) {
            shouldTerminate = true;
            connectionsImpl.disconnect(connectionId);
            return buildErrorFrame("Malformed UNSUBSCRIBE frame", receipt);
        }
        
        boolean removed = connectionsImpl.removeSubscription(connectionId, subscriptionId);
        if (!removed) {
            shouldTerminate = true;
            connectionsImpl.disconnect(connectionId);
            return buildErrorFrame("Subscription not found", receipt);
        }

        if (receipt != null) {
            return "RECEIPT\nreceipt-id:" + receipt + "\n\n\u0000";
        }

        return null;
    }

    private String handleSubscribe(StompMessage stompMessage) {
        String destination = stompMessage.getHeader("destination");
        String subscriptionId = stompMessage.getHeader("id");
        String receipt = stompMessage.getHeader("receipt");

        if (destination == null || subscriptionId == null) {
            shouldTerminate = true;
            connectionsImpl.disconnect(connectionId);
            return buildErrorFrame("Malformed SUBSCRIBE frame", receipt);
        }

        if (!connectionsImpl.addSubscription(connectionId, destination, subscriptionId)) {
            shouldTerminate = true;
            connectionsImpl.disconnect(connectionId);
            return buildErrorFrame("Subscription already exists", receipt);
        }

        if (receipt != null) {
            return "RECEIPT\nreceipt-id:" + receipt + "\n\n\u0000";
        }
        return null;
    }

    private String handleConnect(StompMessage stompMessage) {
        String username = stompMessage.getHeader("login");
        String password = stompMessage.getHeader("passcode");
        if (connectionsImpl.getUser(username) == null) {
            User user = new User(username, password, connectionId);
            connectionsImpl.addUser(user);
            connectionsImpl.setUserConnected(username, true);
            return "CONNECTED\nversion:1.2\n\n\u0000";
        }
        if (connectionsImpl.getUser(username).getPassword().equals(password)) {
            if (connectionsImpl.getUser(username).isConnected()) {
                shouldTerminate = true;
                connectionsImpl.disconnect(connectionId);
                return buildErrorFrame("User already logged in", null);
            }
            connectionsImpl.setUserConnected(username, true);
            return "CONNECTED\nversion:1.2\n\n\u0000";
        }
        shouldTerminate = true;
        connectionsImpl.disconnect(connectionId);
        return buildErrorFrame("Wrong password", null);
    }

    private String buildErrorFrame(String message, String receiptId) {
        String frame = "ERROR\n";
        if (receiptId != null) {
            frame += "receipt-id:" + receiptId + "\n";
        }
        frame += "message:" + message + "\n\n\u0000";
        return frame;
    }
}
