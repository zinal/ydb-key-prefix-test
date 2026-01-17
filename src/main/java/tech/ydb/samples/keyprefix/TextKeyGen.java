package tech.ydb.samples.keyprefix;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

/**
 * Random UUID generator optimized for range partitioning.
 *
 * Similar to UuidKeyGen, but for keys in textual (base64-encoded) format.
 *
 * @author zinal
 */
public class TextKeyGen {

    private final UuidKeyGen generator;

    public TextKeyGen() {
        this(12);
    }

    public TextKeyGen(int prefixBits) {
        this.generator = new UuidKeyGen(prefixBits);
    }

    /**
     * Generates the new shared prefix to generate a series of related IDs.
     *
     * @return 64-bit random value to be used as a prefix.
     */
    public long nextPrefix() {
        return generator.nextPrefix();
    }

    /**
     * Generates the new ID with the specified prefix value.
     *
     * @param prefix Prefix value
     * @return Random UUID with the embedded prefix, date code and suffix.
     */
    public String nextValue(long prefix) {
        UUID uuid = generator.nextValue(prefix);
        ByteBuffer byteArray = ByteBuffer.allocate(16);
        byteArray.putLong(uuid.getMostSignificantBits());
        byteArray.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder()
                .encodeToString(byteArray.array())
                .substring(0, 22);
    }

    /**
     * Generates the new ID with the random prefix value.
     *
     * @return Random UUID with the embedded prefix, date code and suffix.
     */
    public String nextValue() {
        return nextValue(nextPrefix());
    }

    /**
     * Compute the date code which fits into 14 bits.
     *
     * @return datecode integer between 0 and 14639, inclusive.
     */
    public int getDateCode() {
        return generator.getDateCode();
    }
}
