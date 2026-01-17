package tech.ydb.samples.keyprefix;

import java.time.LocalDate;
import java.util.UUID;

/**
 *
 * @author zinal
 */
public class UuidKeyGen {

    private final int maskPos;

    public UuidKeyGen() {
        this(12);
    }

    public UuidKeyGen(int prefixBits) {
        if (prefixBits < 1 || prefixBits > 31) {
            throw new IllegalArgumentException("Unsupported prefix length: " + prefixBits);
        }
        this.maskPos = prefixBits - 1;
    }

    public long nextPrefix() {
        UUID v = UUID.randomUUID();
        return v.getMostSignificantBits();

    }

    public UUID nextValue(long prefix) {
        UUID v = UUID.randomUUID();
        long prefixMask = Holder.prefixMasks[maskPos];
        long dateMask = Holder.dateMasks[maskPos];
        long bits = v.getMostSignificantBits() & ~(prefixMask | dateMask);
        long date = getDateCode();
        date = date << (49 - maskPos);
        bits |= (prefix & prefixMask) | (date & dateMask);
        return new UUID(bits, v.getLeastSignificantBits());
    }

    public UUID nextValue() {
        return nextValue(nextPrefix());
    }

    /**
     * Compute the date code which fits into 14 bits.
     *
     * @return datecode integer between 0 and 14639, inclusive.
     */
    public int getDateCode() {
        LocalDate date = LocalDate.now();
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
