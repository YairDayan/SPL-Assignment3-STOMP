package bgu.spl.net.impl.stomp;

import java.util.HashMap;
import java.util.Map;

/**
 * A STOMP frame split into its command, headers and body.
 */
public class StompMessage {
    private String command;
    private Map<String, String> headers;
    private String body;

    /**
     * Parses a raw frame. The first line is the command, the lines up to the
     * first blank line are headers, and everything after the blank line is the
     * body, kept as is.
     *
     * @param message the raw frame, without the terminating '\0'
     * @throws IllegalArgumentException if the frame is empty, has no command or
     *                                  has a malformed header line
     */
    public StompMessage(String message) {
        if (message == null || message.isEmpty()) {
            throw new IllegalArgumentException("Frame is empty");
        }
        message = message.replace("\r", "");

        int headersEnd = message.indexOf("\n\n");
        String head = headersEnd == -1 ? message : message.substring(0, headersEnd);
        this.body = headersEnd == -1 ? "" : message.substring(headersEnd + 2);

        String[] lines = head.split("\n");
        if (lines.length == 0 || lines[0].isEmpty()) {
            throw new IllegalArgumentException("Missing command");
        }
        this.command = lines[0];
        this.headers = new HashMap<>();
        for (int i = 1; i < lines.length; i++) {
            String[] header = lines[i].split(":", 2);
            if (header.length < 2 || header[0].trim().isEmpty()) {
                throw new IllegalArgumentException("Malformed header");
            }
            this.headers.put(header[0].trim(), header[1].trim());
        }
    }

    /**
     * Returns the frame's command.
     *
     * @return the frame's command, e.g. "SEND"
     */
    public String getCommand() {
        return command;
    }

    /**
     * Returns all the frame's headers.
     *
     * @return the frame's headers, mapping each name to its value
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Returns the frame's body.
     *
     * @return the frame's body; empty if it has none
     */
    public String getBody() {
        return body;
    }

    /**
     * Returns the value of one of the frame's headers.
     *
     * @param header the header name
     * @return the header's value, or null if the frame does not have it
     */
    public String getHeader(String header) {
        return headers.get(header);
    }
}
