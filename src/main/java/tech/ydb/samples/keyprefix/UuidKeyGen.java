package tech.ydb.samples.keyprefix;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Random UUID generator optimized for range partitioning.
 *
 * Each generated value consists of a random prefix of the specified length,
 * date code of 14 bits, followed by the random suffix.
 *
 * In addition, the generator supports the "fixed prefix" schema, in which a
 * common prefix value is used for a series of related ids generated (typically
 * to be written in a single transaction).
 *
 * @author zinal
 */
public class UuidKeyGen {

    private final int maskPos;

    /**
     * Constructs the generator instance with the default prefix size of 12
     * bits.
     *
     * Works best for up to 4k table partitions.
     */
    public UuidKeyGen() {
        this(12);
    }

    /**
     * Constructs the generator instance with the custom prefix size.
     *
     * @param prefixBits Number of bits for the prefix, 1 to 31 bits.
     */
    public UuidKeyGen(int prefixBits) {
        if (prefixBits < 1 || prefixBits > 31) {
            throw new IllegalArgumentException("Unsupported prefix length: " + prefixBits);
        }
        this.maskPos = prefixBits - 1;
    }

    /**
     * @return Prefix size used for construction, in bits.
     */
    public int getPrefixBits() {
        return maskPos + 1;
    }

    /**
     * Generates the new shared prefix to generate a series of related IDs.
     *
     * @return 64-bit random value to be used as a prefix.
     */
    public long nextPrefix() {
        UUID v = UUID.randomUUID();
        return v.getMostSignificantBits();
    }

    /**
     * Generates the new ID with the specified prefix value and date.
     *
     * @param prefix Prefix value
     * @param date The date to be used for the generated value
     * @return Random UUID with the embedded prefix, date code and suffix.
     */
    public UUID nextValue(long prefix, LocalDate date) {
        UUID v = UUID.randomUUID();
        long prefixMask = Holder.prefixMasks[maskPos];
        long dateMask = Holder.dateMasks[maskPos];
        long bits = v.getMostSignificantBits() & ~(prefixMask | dateMask);
        long dateCode = getDateCode(date);
        dateCode = dateCode << (49 - maskPos);
        bits |= (prefix & prefixMask) | (dateCode & dateMask);
        return new UUID(bits, v.getLeastSignificantBits());
    }

    /**
     * Generates the new ID with the specified prefix value.
     *
     * @param prefix Prefix value
     * @return Random UUID with the embedded prefix, date code and suffix.
     */
    public UUID nextValue(long prefix) {
        return nextValue(prefix, LocalDate.now());
    }

    /**
     * Generates the new ID with the random prefix value.
     *
     * @return Random UUID with the embedded prefix, date code and suffix.
     */
    public UUID nextValue() {
        return nextValue(nextPrefix(), LocalDate.now());
    }

    /**
     * Compute the date code which fits into 14 bits, for the specific date.
     *
     * @param date the date for which the code should be computed
     * @return datecode integer between 0 and 14639, inclusive.
     */
    public static int getDateCode(LocalDate date) {
        return date.getDayOfYear() - 1 + (366 * (date.getYear() % 40));
    }

    /**
     * A holder class to defer initialization until needed.
     */
    private static class Holder {

        static final long prefixMasks[];
        static final long dateMasks[];

        static {
            long pf[] = new long[32];
            long dt[] = new long[32];
            pf[0] = 0x8000000000000000L;
            dt[0] = (0xFFFC000000000000L >>> 1);
            for (int i = 1; i < 32; ++i) {
                pf[i] = pf[i - 1] | (1L << (63 - i));
                dt[i] = (dt[i - 1] >>> 1);
            }
            prefixMasks = pf;
            dateMasks = dt;
        }
    }

}
