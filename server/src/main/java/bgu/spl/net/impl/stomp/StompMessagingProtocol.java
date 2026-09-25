package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.Connections;
import bgu.spl.net.api.MessagingProtocol;

public class StompMessagingProtocol implements MessagingProtocol<String> {
    private boolean shouldTerminate = false;
    private int connectionId;
    private Connections<String> connections;

    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handleUnsubscribe'");
    }

    private String handleSubscribe(StompMessage stompMessage) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handleSubscribe'");
    }

    private String handleConnect(StompMessage stompMessage) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handleConnect'");
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }
}
