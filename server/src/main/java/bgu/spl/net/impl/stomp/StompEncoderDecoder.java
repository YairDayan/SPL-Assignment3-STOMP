package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.MessageEncoderDecoder;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Encoder-decoder for STOMP frames.
 * A frame ends with the null character ('\0'); the decoded frame is returned
 * without it, and encoded frames always end with it.
 */
public class StompEncoderDecoder implements MessageEncoderDecoder<String> {
    private byte[] bytes = new byte[1 << 10]; // start with 1k
    private int len = 0;

    /**
     * Adds the next byte to the frame being decoded. Newlines that appear
     * before a frame starts (allowed between frames by STOMP) are skipped.
     *
     * @param nextByte the next byte received from the client
     * @return the complete frame, without the terminating '\0', if this byte
     *         is the '\0'; null otherwise
     */
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

    /**
     * Encodes a frame to UTF-8 bytes, adding the terminating '\0' if the frame
     * does not already end with it.
     *
     * @param message the frame to encode
     * @return the encoded frame, ending with '\0'
     */
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

    /**
     * Appends a byte to the buffer, doubling its size when it is full.
     *
     * @param nextByte the byte to append
     */
    private void pushByte(byte nextByte) {
        if (len >= bytes.length) {
            bytes = Arrays.copyOf(bytes, len * 2);
        }
        bytes[len++] = nextByte;
    }
}
