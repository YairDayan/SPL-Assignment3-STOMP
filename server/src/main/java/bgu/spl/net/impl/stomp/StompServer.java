package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.Server;

public class StompServer {

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
                    StompMessagingProtocol::new,
                    StompEncoderDecoder::new
            ).serve();
        } else if ("reactor".equals(mode)) {
            Server.reactor(
                    Runtime.getRuntime().availableProcessors(),
                    port,
                    StompMessagingProtocol::new,
                    StompEncoderDecoder::new
            ).serve();
        } else {
            System.out.println("Invalid mode: " + mode + " (expected tpc/reactor)");
        }
    }
}