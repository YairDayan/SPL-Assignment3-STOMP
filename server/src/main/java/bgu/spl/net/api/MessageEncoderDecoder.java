package bgu.spl.net.api;

/**
 * Converts between raw bytes received from or sent to a client and messages of
 * type T.
 * Decoding is done one byte at a time, so a message may arrive split across
 * several reads.
 *
 * @param <T> the type of message this encoder-decoder works with
 */
public interface MessageEncoderDecoder<T> {

    /**
     * Adds the next byte to the decoding process.
     *
     * @param nextByte the next byte to consider for the currently decoded message
     * @return a message if this byte completes one, or null if it doesn't
     */
    T decodeNextByte(byte nextByte);

    /**
     * Encodes the given message to a byte array.
     *
     * @param message the message to encode
     * @return the encoded bytes
     */
    byte[] encode(T message);

}
