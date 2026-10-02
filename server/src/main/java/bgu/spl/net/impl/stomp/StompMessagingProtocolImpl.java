package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The server side of the STOMP protocol for a single client.
 * Handles CONNECT, SUBSCRIBE, UNSUBSCRIBE, SEND and DISCONNECT frames and
 * sends every response (CONNECTED, MESSAGE, RECEIPT, ERROR) through the
 * server's connections. The registered users are shared by all clients of the
 * server.
 */
public class StompMessagingProtocolImpl implements StompMessagingProtocol<String> {
    private static final AtomicInteger MESSAGE_ID_COUNTER = new AtomicInteger(0);
    private static final ConcurrentHashMap<String, User> USERS = new ConcurrentHashMap<>();

    /**
     * The possible outcomes of a login attempt.
     */
    private enum LoginResult {
        CONNECTED,
        ALREADY_LOGGED_IN,
        WRONG_PASSWORD
    }

    private volatile boolean shouldTerminate = false;
    private int connectionId;
    private Connections<String> connections;
    private User loggedInUser = null;

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

    /**
     * Parses a frame received from the client and handles it according to its
     * command. A malformed frame, an unknown command, or any command other
     * than CONNECT sent before the client logged in results in an ERROR frame
     * and closing the connection.
     *
     * @param message the raw frame, without the terminating '\0'
     */
    @Override
    public void process(String message) {
        StompMessage stompMessage;
        try {
            stompMessage = new StompMessage(message);
        } catch (IllegalArgumentException e) {
            sendErrorAndClose("Malformed frame", null);
            return;
        }

        if (loggedInUser == null && !"CONNECT".equals(stompMessage.getCommand())) {
            sendErrorAndClose("User is not logged in", stompMessage.getHeader("receipt"));
            return;
        }

        switch (stompMessage.getCommand()) {
            case "CONNECT":
                handleConnect(stompMessage);
                break;
            case "SUBSCRIBE":
                handleSubscribe(stompMessage);
                break;
            case "UNSUBSCRIBE":
                handleUnsubscribe(stompMessage);
                break;
            case "SEND":
                handleSend(stompMessage);
                break;
            case "DISCONNECT":
                handleDisconnect(stompMessage);
                break;
            default:
                sendErrorAndClose("Unsupported command", stompMessage.getHeader("receipt"));
        }
    }

    /**
     * Logs the client in, creating the user if it does not exist yet.
     * Replies with CONNECTED (and a RECEIPT if requested), or with an ERROR if
     * the frame is missing login details, the password is wrong or the user is
     * already logged in.
     *
     * @param stompMessage the CONNECT frame
     */
    private void handleConnect(StompMessage stompMessage) {
        String username = stompMessage.getHeader("login");
        String password = stompMessage.getHeader("passcode");
        String receipt = stompMessage.getHeader("receipt");

        if (username == null || password == null) {
            sendErrorAndClose("Malformed CONNECT frame", receipt);
            return;
        }

        LoginResult result = login(username, password);
        if (result == LoginResult.CONNECTED) {
            sendFrame("CONNECTED\nversion:1.2\n\n\u0000");
            sendReceiptIfRequested(receipt);
        } else if (result == LoginResult.ALREADY_LOGGED_IN) {
            sendErrorAndClose("User already logged in", receipt);
        } else {
            sendErrorAndClose("Wrong password", receipt);
        }
    }

    /**
     * Checks the login details and, if they are valid, logs the user in on
     * this connection. Synchronized on the users map so two clients cannot log
     * in to the same user at once.
     *
     * @param username the login name
     * @param password the password
     * @return the outcome of the login attempt
     */
    private LoginResult login(String username, String password) {
        synchronized (USERS) {
            User user = USERS.get(username);
            if (user == null) {
                user = new User(username, password);
                USERS.put(username, user);
            } else if (!user.getPassword().equals(password)) {
                return LoginResult.WRONG_PASSWORD;
            } else if (isLoggedIn(user)) {
                return LoginResult.ALREADY_LOGGED_IN;
            }
            user.setConnectionId(connectionId);
            loggedInUser = user;
            return LoginResult.CONNECTED;
        }
    }

    /**
     * Checks whether a user is logged in. A user is logged in if its last
     * connection is still active, so a client that closed its socket without
     * sending DISCONNECT does not stay logged in.
     *
     * @param user the user to check
     * @return true if the user is logged in on an active connection
     */
    private boolean isLoggedIn(User user) {
        int userConnectionId = user.getConnectionId();
        return userConnectionId != User.NO_CONNECTION && connections.isConnected(userConnectionId);
    }

    /**
     * Subscribes the client to a channel, creating the channel if it does not
     * exist yet. Replies with an ERROR if a header is missing, the client is
     * already subscribed to the channel, or it already uses this subscription
     * id.
     *
     * @param stompMessage the SUBSCRIBE frame
     */
    private void handleSubscribe(StompMessage stompMessage) {
        String destination = stompMessage.getHeader("destination");
        String subscriptionId = stompMessage.getHeader("id");
        String receipt = stompMessage.getHeader("receipt");

        if (destination == null || subscriptionId == null) {
            sendErrorAndClose("Malformed SUBSCRIBE frame", receipt);
            return;
        }

        if (!connections.subscribe(destination, connectionId, subscriptionId)) {
            sendErrorAndClose("Subscription already exists", receipt);
            return;
        }

        sendReceiptIfRequested(receipt);
    }

    /**
     * Removes one of the client's subscriptions.
     * Replies with an ERROR if the id header is missing or no such
     * subscription exists.
     *
     * @param stompMessage the UNSUBSCRIBE frame
     */
    private void handleUnsubscribe(StompMessage stompMessage) {
        String subscriptionId = stompMessage.getHeader("id");
        String receipt = stompMessage.getHeader("receipt");

        if (subscriptionId == null) {
            sendErrorAndClose("Malformed UNSUBSCRIBE frame", receipt);
            return;
        }

        if (!connections.unsubscribe(connectionId, subscriptionId)) {
            sendErrorAndClose("Subscription not found", receipt);
            return;
        }

        sendReceiptIfRequested(receipt);
    }

    /**
     * Forwards the frame's body as a MESSAGE to every subscriber of the
     * destination, each with its own subscription id. Replies with an ERROR if
     * the destination header is missing or the client is not subscribed to it.
     *
     * @param stompMessage the SEND frame
     */
    private void handleSend(StompMessage stompMessage) {
        String destination = stompMessage.getHeader("destination");
        String receipt = stompMessage.getHeader("receipt");
        String body = stompMessage.getBody();

        if (destination == null) {
            sendErrorAndClose("Malformed SEND frame", receipt);
            return;
        }

        if (!connections.isSubscribed(connectionId, destination)) {
            sendErrorAndClose("Not subscribed to destination", receipt);
            return;
        }

        if (!body.isEmpty() && body.charAt(body.length() - 1) == '\n') {
            body = body.substring(0, body.length() - 1);
        }

        int messageId = nextMessageId();
        for (Map.Entry<Integer, String> subscriber : connections.getSubscribers(destination).entrySet()) {
            String messageFrame = "MESSAGE\n" +
                    "subscription:" + subscriber.getValue() + "\n" +
                    "message-id:" + messageId + "\n" +
                    "destination:" + destination + "\n\n" +
                    body +
                    "\u0000";
            connections.send(subscriber.getKey(), messageFrame);
        }
        sendReceiptIfRequested(receipt);
    }

    /**
     * Logs the client out: replies with a RECEIPT and then closes the
     * connection. Replies with an ERROR if the receipt header is missing.
     *
     * @param stompMessage the DISCONNECT frame
     */
    private void handleDisconnect(StompMessage stompMessage) {
        String receipt = stompMessage.getHeader("receipt");
        if (receipt == null) {
            sendErrorAndClose("Malformed DISCONNECT frame", null);
            return;
        }

        shouldTerminate = true;
        sendReceiptIfRequested(receipt);
        close();
    }

    /**
     * Sends a frame to this protocol's client.
     *
     * @param frame the frame to send
     */
    private void sendFrame(String frame) {
        connections.send(connectionId, frame);
    }

    /**
     * Sends a RECEIPT frame if the client asked for one.
     *
     * @param receipt the value of the frame's receipt header, or null if it
     *                has none
     */
    private void sendReceiptIfRequested(String receipt) {
        if (receipt != null) {
            sendFrame("RECEIPT\nreceipt-id:" + receipt + "\n\n\u0000");
        }
    }

    /**
     * Sends an ERROR frame and then closes the connection.
     *
     * @param message   a short description of the error
     * @param receiptId the receipt header of the frame that caused the error,
     *                  or null if none
     */
    private void sendErrorAndClose(String message, String receiptId) {
        String frame = "ERROR\n";
        if (receiptId != null) {
            frame += "receipt-id:" + receiptId + "\n";
        }
        frame += "message:" + message + "\n\n\u0000";
        shouldTerminate = true;
        sendFrame(frame);
        close();
    }

    /**
     * Logs out the client's user, marks the connection for termination and
     * removes the client from the connections, which also deletes its
     * subscriptions. Must be called after the last frame is sent, since a
     * removed client can no longer be sent frames.
     */
    private void close() {
        if (loggedInUser != null) {
            synchronized (USERS) {
                loggedInUser.setConnectionId(User.NO_CONNECTION);
            }
            loggedInUser = null;
        }
        shouldTerminate = true;
        connections.disconnect(connectionId);
    }

    /**
     * Generates a new message id, unique across the whole server.
     *
     * @return a new message id
     */
    private int nextMessageId() {
        return MESSAGE_ID_COUNTER.incrementAndGet();
    }
}
