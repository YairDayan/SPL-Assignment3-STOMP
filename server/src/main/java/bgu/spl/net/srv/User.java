package bgu.spl.net.srv;

public class User {
    private String username;
    private String password;
    private int connectionId;
    private boolean connected;

    public User(String username, String password, int connectionId) {
        this.username = username;
        this.password = password;
        this.connectionId = connectionId;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
    
    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }
}
