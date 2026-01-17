package tech.ydb.samples.keyprefix;

import java.nio.ByteBuffer;
import java.time.LocalDate;
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

    /**
     * Constructs the generator instance with the default prefix size of 12
     * bits.
     *
     * Works best for up to 4k table partitions.
     */
    public TextKeyGen() {
        this(12);
    }

    /**
     * Constructs the generator instance with the custom prefix size.
     *
     * @param prefixBits Number of bits for the prefix, 1 to 31 bits.
     */
    public TextKeyGen(int prefixBits) {
        this.generator = new UuidKeyGen(prefixBits);
    }

    /**
     * @return Prefix size used for construction, in bits.
     */
    public int getPrefixBits() {
        return generator.getPrefixBits();
    }

    /**
     * @return The supporting UUID generator instance.
     */
    public UuidKeyGen getGenerator() {
        return generator;
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
     * @param date The date to be used for the generated value
     * @return Random UUID with the embedded prefix, date code and suffix.
     */
    public String nextValue(long prefix, LocalDate date) {
        UUID uuid = generator.nextValue(prefix, date);
        ByteBuffer byteArray = ByteBuffer.allocate(16);
        byteArray.putLong(uuid.getMostSignificantBits());
        byteArray.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder()
                .encodeToString(byteArray.array())
                .substring(0, 22);
    }

    /**
     * Generates the new ID with the specified prefix value.
     *
     * @param prefix Prefix value
     * @return Random UUID with the embedded prefix, date code and suffix.
     */
    public String nextValue(long prefix) {
        return nextValue(prefix, LocalDate.now());
    }

    /**
     * Generates the new ID with the random prefix value.
     *
     * @return Random UUID with the embedded prefix, date code and suffix.
     */
    public String nextValue() {
        return nextValue(nextPrefix(), LocalDate.now());
    }
}
