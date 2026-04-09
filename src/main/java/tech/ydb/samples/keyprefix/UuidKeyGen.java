package tech.ydb.samples.keyprefix;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/**
 * Random UUID generator optimized for range partitioning.
 *
 * Each generated value consists of a random prefix of the specified length, a
 * second-precision timestamp code of 30 bits, followed by the random suffix.
 *
 * The timestamp is epoch seconds since 2020-01-01T00:00:00Z, modulo 2^30 (about
 * 34 years), which avoids code reuse for well over 20 years from the epoch
 * start.
 *
 * In addition, the generator supports the "fixed prefix" schema, in which a
 * common prefix value is used for a series of related ids generated (typically
 * to be written in a single transaction).
 *
 * The generated value is optimized for the actual internal format of UUID
 * storage in YDB, which uses GUID (Microsoft-style) mixed-endian byte ordering.
 * This affects the actual ordering, so it is important to properly put the
 * bytes in the correct order.
 *
 * @author zinal
 */
public class UuidKeyGen {

    /**
     * Bit width of the embedded second-precision timestamp field.
     */
    public static final int TIMESTAMP_BITS = 30;

    /**
     * Number of seconds which fit within the bit width specified.
     */
    public static final long TIMESTAMP_SECONDS = (1L << TIMESTAMP_BITS);

    /**
     * A position within an array of pre-computed bitmasks to be used.
     */
    private final int maskPos;

    /**
     * Constructs the generator instance with the default prefix size of 10
     * bits.
     *
     * Works best for up to 1k table partitions.
     */
    public UuidKeyGen() {
        this(10);
    }

    /**
     * Constructs the generator instance with the custom prefix size.
     *
     * @param prefixBits Number of bits for the prefix, 1 to 18 bits.
     */
    public UuidKeyGen(int prefixBits) {
        if (prefixBits < 1 || prefixBits > 18) {
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
     * @return Prefix mask to be applied
     */
    public long getPrefixMask() {
        return Holder.prefixMasks[maskPos];
    }

    /**
     * Generates the new shared prefix to generate a series of related IDs.
     *
     * @return 64-bit random value to be used as a prefix.
     */
    public long nextPrefix() {
        final SecureRandom ng = Holder.numberGenerator;
        byte[] data = new byte[8];
        ng.nextBytes(data);
        long lsb = 0;
        for (int i = 0; i < 8; i++) {
            lsb = (lsb << 8) | (data[i] & 0xff);
        }
        return lsb;
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
        final SecureRandom ng = Holder.numberGenerator;
        final long prefixMask = Holder.prefixMasks[maskPos];
        final long tsMask = Holder.timestampMasks[maskPos];

        byte[] data = new byte[16];
        ng.nextBytes(data);
        data[6] &= 0x0f;
        data[6] |= 0x80;
        data[8] &= 0x3f;
        data[8] |= (byte) 0x80;

        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (data[i] & 0xff);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (data[i] & 0xff);
        }

        long bits = msb & ~(prefixMask | tsMask);
        long tsCode = getTimestampCode(instant);
        tsCode = tsCode << (Holder.TIMESTAMP_FIELD_LOW_BIT - maskPos);
        bits |= (prefix & prefixMask) | (tsCode & tsMask);
        bits = reorderForYdb(bits);
        return new UUID(bits, lsb);
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
     * Generates the new ID with the random prefix value and a specified
     * instant.
     *
     * @return Random UUID with the embedded prefix, timestamp code and suffix.
     */
    public UUID nextValue(Instant instant) {
        return nextValue(nextPrefix(), instant);
    }

    /**
     * Generates the new ID with the random prefix value and a specified date.
     *
     * @return Random UUID with the embedded prefix, timestamp code and suffix.
     */
    public UUID nextValue(LocalDate date) {
        return nextValue(nextPrefix(), date);
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
     * @param instant the instant to be used
     * @return timestamp code between 0 and 2^30 - 1, inclusive
     */
    public static int getTimestampCode(Instant instant) {
        Instant sec = instant.truncatedTo(ChronoUnit.SECONDS);
        long diff = sec.getEpochSecond() % TIMESTAMP_SECONDS;
        if (diff < 0) {
            throw new IllegalArgumentException(
                    "Instant out of 30-bit timestamp range: " + instant);
        }
        return (int) diff;
    }

    /**
     * YDB uses GUID (Microsoft-style) mixed-endian format.
     *
     * xxxxxxxx 0 1 2 3x 4 5 6 7 <br/>
     * xINPUT: 01020304 05060708 090a0b0c 0d0e0f10 <br/>
     * OUTPUT: 04030201 06050807 090a0b0c 0d0e0f10
     *
     * This function puts the bytes of MSB in the proper order.
     *
     * @param v
     * @return
     */
    public static long reorderForYdb(long v) {
        var bbin = ByteBuffer.allocate(8);
        bbin.putLong(v);
        var arin = bbin.array();
        var arout = new byte[8];
        arout[3] = arin[0];
        arout[2] = arin[1];
        arout[1] = arin[2];
        arout[0] = arin[3];
        arout[5] = arin[4];
        arout[4] = arin[5];
        arout[7] = arin[6];
        arout[6] = arin[7];
        var bbout = ByteBuffer.wrap(arout);
        return bbout.getLong();
    }

    /**
     * A holder class to defer initialization until needed.
     */
    private static class Holder {

        static final SecureRandom numberGenerator = new SecureRandom();

        /**
         * Low bit index (inclusive) of the 30-bit timestamp field when
         * {@code maskPos == 0} (1-bit prefix). Shifts down with larger
         * prefixes.
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
