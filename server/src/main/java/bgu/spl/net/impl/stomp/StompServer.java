package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.Server;

/**
 * Entry point of the STOMP server.
 * Runs the server in thread-per-client or reactor mode, according to the
 * arguments given at startup.
 */
public class StompServer {

    /**
     * Starts the server in the requested mode.
     *
     * @param args the port to listen on, and the server mode ("tpc" or
     *             "reactor")
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: <port> <tpc|reactor>");
            return;
        }

        int port;
        try {
            port = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid port number: " + args[0]);
            return;
        }

        String mode = args[1];

        if ("tpc".equals(mode)) {
            Server.threadPerClient(
                    port,
                    StompMessagingProtocolImpl::new,
                    StompEncoderDecoder::new).serve();
        } else if ("reactor".equals(mode)) {
            Server.reactor(
                    Runtime.getRuntime().availableProcessors(),
                    port,
                    StompMessagingProtocolImpl::new,
                    StompEncoderDecoder::new).serve();
        } else {
            System.out.println("Invalid mode: " + mode + " (expected tpc/reactor)");
        }
    }
}
