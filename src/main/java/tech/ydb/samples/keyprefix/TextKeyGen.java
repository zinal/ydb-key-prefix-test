package tech.ydb.samples.keyprefix;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.UUID;
import static tech.ydb.samples.keyprefix.BaseKeyGen.TIMESTAMP_FIELD_LOW_BIT;
import static tech.ydb.samples.keyprefix.BaseKeyGen.getTimestampCode;
import static tech.ydb.samples.keyprefix.BaseKeyGen.reorder;

/**
 * Random text-format ID generator creates cache friendly identifiers to be used
 * as primary keys for YDB row-organized tables.
 *
 * Similar to UuidKeyGen, but for keys in textual (base64-encoded) format.
 *
 * @author zinal
 */
public class TextKeyGen extends BaseKeyGen {

    /**
     * Constructs the generator instance with the default prefix size of 10
     * bits.
     *
     * Works best for up to 1k table partitions.
     */
    public TextKeyGen() {
        super(10);
    }

    /**
     * Constructs the generator instance with the custom prefix size.
     *
     * @param prefixBits Number of bits for the prefix, 1 to 18 bits.
     */
    public TextKeyGen(int prefixBits) {
        super(prefixBits);
    }

    /**
     * Generates the new ID with the specified prefix and instant (second
     * precision for the embedded timestamp field).
     *
     * @param prefix Prefix value
     * @param instant The instant whose second is embedded in the ID
     * @return Base64 encoded ID with the embedded prefix and timestamp.
     */
    public String nextValue(long prefix, Instant instant) {
        SecureRandom ng = Holder.numberGenerator;
        byte[] data = new byte[16];
        ng.nextBytes(data);

        long msb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (data[i] & 0xff);
        }
        msb = update(msb, prefix, instant);
        ByteBuffer.wrap(data).putLong(msb);
        return Base64.getUrlEncoder()
                .encodeToString(data)
                .substring(0, 22);
    }

    /**
     * Generates the new ID with the specified prefix value.
     *
     * @param prefix Prefix value
     * @param date The date (UTC midnight) used for the embedded timestamp
     * @return Base64 encoded ID with the embedded prefix and timestamp for the
     * start of date specified.
     */
    public String nextValue(long prefix, LocalDate date) {
        return nextValue(prefix, date.atStartOfDay(ZoneOffset.UTC).toInstant());
    }

    /**
     * Generates the new ID with the specified prefix value.
     *
     * @param prefix Prefix value
     * @return Base64 encoded ID with the embedded prefix and the current
     * timestamp.
     */
    public String nextValue(long prefix) {
        return nextValue(prefix, Instant.now());
    }

    /**
     * Generates the new ID with the specified instant.
     *
     * @return Base64 encoded ID with the embedded timestamp code.
     */
    public String nextValue(Instant instant) {
        return nextValue(-1L, instant);
    }

    /**
     * Generates the new ID with the random prefix value and a specified date.
     *
     * @param date The date (UTC midnight) used for the embedded timestamp
     * @return Base64 encoded ID with the embedded timestamp for the start of
     * date specified.
     */
    public String nextValue(LocalDate date) {
        return nextValue(-1L, date);
    }

    /**
     * Generates the new ID with the random prefix value.
     *
     * @return Base64 encoded ID with the embedded current timestamp.
     */
    public String nextValue() {
        return nextValue(-1L, Instant.now());
    }
}
