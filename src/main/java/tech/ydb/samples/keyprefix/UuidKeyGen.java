package tech.ydb.samples.keyprefix;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/**
 * Random UUID generator optimized for range partitioning.
 *
 * Each generated value consists of a random prefix of the specified length,
 * a second-precision timestamp code of 30 bits, followed by the random suffix.
 *
 * The timestamp is epoch seconds since 2020-01-01T00:00:00Z, modulo 2^30
 * (about 34 years), which avoids code reuse for well over 20 years from the
 * epoch start.
 *
 * In addition, the generator supports the "fixed prefix" schema, in which a
 * common prefix value is used for a series of related ids generated (typically
 * to be written in a single transaction).
 *
 * @author zinal
 */
public class UuidKeyGen {

    /** Seconds from Unix epoch to 2020-01-01T00:00:00Z (UTC). */
    static final long TIMESTAMP_EPOCH_SECOND = 1577836800L;

    /** Bit width of the embedded second-precision timestamp field. */
    static final int TIMESTAMP_BITS = 30;

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
     * Generates the new ID with the specified prefix value and calendar date
     * (UTC midnight).
     *
     * @param prefix Prefix value
     * @param date The date whose start-of-day UTC instant is embedded
     * @return Random UUID with the embedded prefix, timestamp code and suffix.
     */
    public UUID nextValue(long prefix, LocalDate date) {
        return nextValue(prefix, date.atStartOfDay(ZoneOffset.UTC).toInstant());
    }

    /**
     * Generates the new ID with the specified prefix value and instant
     * (truncated to whole seconds for the embedded timestamp field).
     *
     * @param prefix Prefix value
     * @param instant The instant whose second is embedded in the UUID
     * @return Random UUID with the embedded prefix, timestamp code and suffix.
     */
    public UUID nextValue(long prefix, Instant instant) {
        UUID v = UUID.randomUUID();
        long prefixMask = Holder.prefixMasks[maskPos];
        long tsMask = Holder.timestampMasks[maskPos];
        long bits = v.getMostSignificantBits() & ~(prefixMask | tsMask);
        long tsCode = getTimestampCode(instant);
        tsCode = tsCode << (Holder.TIMESTAMP_FIELD_LOW_BIT - maskPos);
        bits |= (prefix & prefixMask) | (tsCode & tsMask);
        return new UUID(bits, v.getLeastSignificantBits());
    }

    /**
     * Generates the new ID with the specified prefix value.
     *
     * @param prefix Prefix value
     * @return Random UUID with the embedded prefix, timestamp code and suffix.
     */
    public UUID nextValue(long prefix) {
        return nextValue(prefix, Instant.now());
    }

    /**
     * Generates the new ID with the random prefix value.
     *
     * @return Random UUID with the embedded prefix, timestamp code and suffix.
     */
    public UUID nextValue() {
        return nextValue(nextPrefix(), Instant.now());
    }

    /**
     * Computes the second-precision timestamp code: seconds since
     * 2020-01-01T00:00:00Z, in the range {@code [0, 2^30)}.
     *
     * @param instant the instant (truncated to seconds)
     * @return timestamp code between 0 and 2^30 - 1, inclusive
     */
    public static int getTimestampCode(Instant instant) {
        Instant sec = instant.truncatedTo(ChronoUnit.SECONDS);
        long diff = sec.getEpochSecond() - TIMESTAMP_EPOCH_SECOND;
        if (diff < 0 || diff >= (1L << TIMESTAMP_BITS)) {
            throw new IllegalArgumentException(
                    "Instant out of 30-bit timestamp range: " + instant);
        }
        return (int) diff;
    }

    /**
     * A holder class to defer initialization until needed.
     */
    private static class Holder {

        /**
         * Low bit index (inclusive) of the 30-bit timestamp field when
         * {@code maskPos == 0} (1-bit prefix). Shifts down with larger prefixes.
         */
        static final int TIMESTAMP_FIELD_LOW_BIT = 33;

        static final long prefixMasks[];
        static final long timestampMasks[];

        static {
            long pf[] = new long[32];
            long ts[] = new long[32];
            pf[0] = 0x8000000000000000L;
            ts[0] = (((1L << TIMESTAMP_BITS) - 1) << TIMESTAMP_FIELD_LOW_BIT);
            for (int i = 1; i < 32; ++i) {
                pf[i] = pf[i - 1] | (1L << (63 - i));
                ts[i] = (ts[i - 1] >>> 1);
            }
            prefixMasks = pf;
            timestampMasks = ts;
        }
    }

}
