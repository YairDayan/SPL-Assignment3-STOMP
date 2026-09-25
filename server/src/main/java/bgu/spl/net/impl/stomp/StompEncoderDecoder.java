package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.MessageEncoderDecoder;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class StompEncoderDecoder implements MessageEncoderDecoder<String> {
    private byte[] bytes = new byte[1 << 10]; //start with 1k
    private int len = 0;

    @Override
    public String decodeNextByte(byte nextByte) {
        if (nextByte == 0) {
            String frame = new String(bytes, 0, len, StandardCharsets.UTF_8);
            len = 0;
            return frame;
        }
        if (len == 0 && nextByte == '\n') {
            return null;
        }
        pushByte(nextByte);
        return null;
    }

    @Override
    public byte[] encode(String message) {
        byte[] utf8 = message.getBytes(StandardCharsets.UTF_8);
        if (utf8.length > 0 && utf8[utf8.length - 1] == 0) {
            return utf8;
        }
        byte[] output = Arrays.copyOf(utf8, utf8.length + 1);
        output[utf8.length] = 0;
        return output;
    }

    private void pushByte(byte nextByte) {
        if (len >= bytes.length) {
            bytes = Arrays.copyOf(bytes, len * 2);
        }
        bytes[len++] = nextByte;
    }
}
