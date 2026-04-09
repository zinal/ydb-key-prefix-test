package tech.ydb.samples.keyprefix;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Base64;
import java.util.UUID;

/**
 * Random text-format ID generator creates cache friendly identifiers to be used
 * as primary keys for YDB row-organized tables.
 *
 * Similar to UuidKeyGen, but for keys in textual (base64-encoded) format.
 *
 * @author zinal
 */
public class TextKeyGen {

    private final UuidKeyGen generator;

    /**
     * Constructs the generator instance with the default prefix size of 10
     * bits.
     *
     * Works best for up to 1k table partitions.
     */
    public TextKeyGen() {
        this(10);
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
     * Convert a UUID value to a base64 text representation.
     *
     * @param uuid Value to be converted
     * @return Text representation of a UUID value of a fixed length 22 symbols
     */
    public static String convert(UUID uuid) {
        // apply byte swaps to restore the "regular" ordering
        long msb = UuidKeyGen.reorder(uuid.getMostSignificantBits());
        ByteBuffer byteArray = ByteBuffer.allocate(16);
        byteArray.putLong(msb);
        byteArray.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder()
                .encodeToString(byteArray.array())
                .substring(0, 22);
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
     * @param date The date (UTC midnight) used for the embedded timestamp
     * @return Random UUID with the embedded prefix, timestamp code and suffix.
     */
    public String nextValue(long prefix, LocalDate date) {
        UUID uuid = generator.nextValue(prefix, date);
        return convert(uuid);
    }

    /**
     * Generates the new ID with the specified prefix and instant (second
     * precision for the embedded timestamp field).
     *
     * @param prefix Prefix value
     * @param instant The instant whose second is embedded in the ID
     * @return Encoded UUID with the embedded prefix, timestamp code and suffix.
     */
    public String nextValue(long prefix, Instant instant) {
        UUID uuid = generator.nextValue(prefix, instant);
        return convert(uuid);
    }

    /**
     * Generates the new ID with the specified prefix value.
     *
     * @param prefix Prefix value
     * @return Random UUID with the embedded prefix, timestamp code and suffix.
     */
    public String nextValue(long prefix) {
        return nextValue(prefix, Instant.now());
    }

    /**
     * Generates the new ID with the random prefix value and a specified
     * instant.
     *
     * @return Random UUID with the embedded prefix, timestamp code and suffix.
     */
    public String nextValue(Instant instant) {
        return nextValue(-1L, instant);
    }

    /**
     * Generates the new ID with the random prefix value and a specified date.
     *
     * @return Random UUID with the embedded prefix, timestamp code and suffix.
     */
    public String nextValue(LocalDate date) {
        return nextValue(-1L, date);
    }

    /**
     * Generates the new ID with the random prefix value.
     *
     * @return Random UUID with the embedded prefix, timestamp code and suffix.
     */
    public String nextValue() {
        return nextValue(-1L, Instant.now());
    }
}
