package tech.ydb.samples.keyprefix;

import java.security.SecureRandom;
import static tech.ydb.samples.keyprefix.UuidKeyGen.TIMESTAMP_BITS;

/**
 * Implementation helpers for key generators.
 *
 * @author zinal
 */
class SupportKeyGen {

    /**
     * Low bit index (inclusive) of the 30-bit timestamp field when
     * {@code maskPos == 0} (1-bit prefix). Shifts down with larger prefixes.
     */
    static final int TIMESTAMP_FIELD_LOW_BIT = 33;

    /**
     * A holder class to defer initialization until needed.
     */
    static class Holder {

        static final SecureRandom numberGenerator = new SecureRandom();

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
