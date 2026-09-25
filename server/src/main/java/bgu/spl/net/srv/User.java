package bgu.spl.net.srv;

public class User {
    private String username;
    private String password;
    private String sessionId;
    private boolean connected;

    public User(String username, String password, String sessionId) {
        this.username = username;
        this.password = password;
        this.sessionId = sessionId;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
    
    public String getSessionId() {
        return sessionId;
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }
}
