package bgu.spl.net.impl.stomp;

import java.util.HashMap;
import java.util.Map;

public class StompMessage {
    private String command;
    private Map<String, String> headers;
    private String body;

    public StompMessage(String message) {
        if (message == null || message.isEmpty()) {
            throw new IllegalArgumentException("Frame is empty");
        }
        message = message.replace("\r", "");
        String[] lines = message.split("\n");
        if (lines.length == 0 || lines[0].isEmpty()) {
            throw new IllegalArgumentException("Missing command");
        }
        this.command = lines[0];
        this.headers = new HashMap<>();
        StringBuilder bodyBuilder = new StringBuilder();
        boolean body = false;
        for (int i = 1; i < lines.length; i++) {
            if (lines[i].isEmpty()) {
                body = true;
                continue;
            }
            if (body) {
                bodyBuilder.append(lines[i]).append("\n");
            } else {
                String[] header = lines[i].split(":", 2);
                if (header.length < 2 || header[0].isEmpty()) {
                    throw new IllegalArgumentException("Malformed header");
                }
                this.headers.put(header[0], header[1]);
            }
        }
        this.body = bodyBuilder.toString();
    }

    public String getCommand() {
        return command;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getBody() {
        return body;
    }

    public String getHeader(String header) {
        return headers.get(header);
    }
}
