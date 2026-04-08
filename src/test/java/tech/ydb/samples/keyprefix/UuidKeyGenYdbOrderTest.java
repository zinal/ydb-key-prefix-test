package tech.ydb.samples.keyprefix;

import java.time.Instant;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

/**
 * Matches YDB Java SDK UUID ordering ({@code PrimitiveValue.compareUUID}):
 * unsigned compare of byte-swapped low 64 bits, then byte-swapped high 64 bits.
 */
public class UuidKeyGenYdbOrderTest {

    private static int ydbCompare(UUID a, UUID b) {
        long al = Long.reverseBytes(a.getLeastSignificantBits());
        long bl = Long.reverseBytes(b.getLeastSignificantBits());
        if (al != bl) {
            return Long.compareUnsigned(al, bl);
        }
        long ah = Long.reverseBytes(a.getMostSignificantBits());
        long bh = Long.reverseBytes(b.getMostSignificantBits());
        return Long.compareUnsigned(ah, bh);
    }

    @Test
    public void fixedPrefixTimeOrderMatchesYdbCompare() {
        var gen = new UuidKeyGen();
        long prefix = 0x1234567890abcdefL;
        Instant t0 = Instant.parse("2024-06-01T12:00:00Z");
        Instant t1 = Instant.parse("2024-06-01T12:00:01Z");
        UUID u0 = gen.nextValue(prefix, t0);
        UUID u1 = gen.nextValue(prefix, t1);
        Assert.assertTrue(ydbCompare(u0, u1) < 0);
    }

    @Test
    public void fixedPrefixAndTimeLexicographicOnLsbPrimary() {
        var gen = new UuidKeyGen();
        Instant t = Instant.parse("2024-06-01T12:00:00Z");
        long lowPfx = 1L << 61;
        long highPfx = 3L << 61;
        UUID a = gen.nextValue(lowPfx, t);
        UUID b = gen.nextValue(highPfx, t);
        Assert.assertTrue(ydbCompare(a, b) < 0);
    }
}
