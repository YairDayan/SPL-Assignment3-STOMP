package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.MessagingProtocol;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.srv.ConnectionsImpl;
import bgu.spl.net.srv.User;

import java.util.concurrent.atomic.AtomicInteger;

public class StompMessagingProtocol implements MessagingProtocol<String> {
    private boolean shouldTerminate = false;
    private int connectionId;
    private ConnectionsImpl<String> connectionsImpl;
    private static final AtomicInteger MESSAGE_ID_COUNTER = new AtomicInteger(0);

    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connectionsImpl = (ConnectionsImpl<String>) connections;
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
                shouldTerminate = true;
                connectionsImpl.disconnect(connectionId);
                return buildErrorFrame("Unsupported command", stompMessage.getHeader("receipt"));
        }
    }

    private String handleDisconnect(StompMessage stompMessage) {
        String receipt = stompMessage.getHeader("receipt");
        if (receipt == null) {
            shouldTerminate = true;
            connectionsImpl.disconnect(connectionId);
            return buildErrorFrame("Malformed DISCONNECT frame", null);
        }

        shouldTerminate = true;
        connectionsImpl.disconnect(connectionId);
        return "RECEIPT\nreceipt-id:" + receipt + "\n\n\u0000";
    }

    private String handleSend(StompMessage stompMessage) {
        String destination = stompMessage.getHeader("destination");
        String receipt = stompMessage.getHeader("receipt");
        String body = stompMessage.getBody();

        if (destination == null) {
            shouldTerminate = true;
            connectionsImpl.disconnect(connectionId);
            return buildErrorFrame("Malformed SEND frame", receipt);
        }

        if (!connectionsImpl.isSubscribed(connectionId, destination)) {
            shouldTerminate = true;
            connectionsImpl.disconnect(connectionId);
            return buildErrorFrame("Not subscribed to destination", receipt);
        }

        if (body == null) {
            body = "";
        }
        if (!body.isEmpty() && body.charAt(body.length() - 1) == '\n') {
            body = body.substring(0, body.length() - 1);
        }

        String messageFrame =
                "MESSAGE\n" +
                "subscription:%%SUB_ID%%\n" +
                "message-id:" + nextMessageId() + "\n" +
                "destination:" + destination + "\n\n" +
                body +
                "\u0000";

        connectionsImpl.send(destination, messageFrame);

        if (receipt != null) {
            return "RECEIPT\nreceipt-id:" + receipt + "\n\n\u0000";
        }
        return null;
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

        if (username == null || password == null) {
            shouldTerminate = true;
            connectionsImpl.disconnect(connectionId);
            return buildErrorFrame("Malformed CONNECT frame", stompMessage.getHeader("receipt"));
        }

        User existing = connectionsImpl.getUser(username);
        if (existing == null) {
            User user = new User(username, password, connectionId);
            connectionsImpl.addUser(user);
            connectionsImpl.setUserConnected(username, true);
            return "CONNECTED\nversion:1.2\n\n\u0000";
        }

        if (existing.getPassword().equals(password)) {
            if (existing.isConnected()) {
                shouldTerminate = true;
                connectionsImpl.disconnect(connectionId);
                return buildErrorFrame("User already logged in", null);
            }
            existing.setConnectionId(connectionId);
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

    private int nextMessageId() {
        return MESSAGE_ID_COUNTER.incrementAndGet();
    }
}