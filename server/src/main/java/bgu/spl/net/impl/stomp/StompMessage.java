package bgu.spl.net.impl.stomp;

import java.util.HashMap;
import java.util.Map;

public class StompMessage {
    private String command;
    private Map<String, String> headers;
    private String body;

    public StompMessage(String message) {
        message = message.replace("\r", "");
        String[] lines = message.split("\n");
        this.command = lines[0];
        this.headers = new HashMap<>();
        this.body = "";
        boolean body = false;
        for (int i =1; i<lines.length; i++) {
            if (lines[i].isEmpty()) {
                body = true;
                continue;
            }
            if (body) {
                this.body += lines[i] + "\n";
            } else {
                String[] header = lines[i].split(":", 2);
                this.headers.put(header[0], header[1]);
            }
        }
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
}
