package bgu.spl.net.impl.stomp;

/**
 * A registered user of the STOMP server.
 * The username and password are kept after the user logs out, so the user can
 * log in again.
 */
public class User {
    /** The connection id of a user that is not logged in. */
    public static final int NO_CONNECTION = -1;

    private final String username;
    private final String password;
    private volatile int connectionId = NO_CONNECTION;

    /**
     * Creates a user that is not logged in.
     *
     * @param username the user's login name
     * @param password the user's password
     */
    public User(String username, String password) {
        this.username = username;
        this.password = password;
    }

    /**
     * Returns the user's login name.
     *
     * @return the user's login name
     */
    public String getUsername() {
        return username;
    }

    /**
     * Returns the user's password.
     *
     * @return the user's password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Returns the connection id the user last logged in from.
     *
     * @return the connection id, or NO_CONNECTION if the user logged out
     */
    public int getConnectionId() {
        return connectionId;
    }

    /**
     * Sets the connection id the user is logged in from.
     *
     * @param connectionId the connection id the user logged in from, or
     *                     NO_CONNECTION on logout
     */
    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }
}
