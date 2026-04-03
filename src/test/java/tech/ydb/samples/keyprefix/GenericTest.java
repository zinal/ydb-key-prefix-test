package tech.ydb.samples.keyprefix;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author zinal
 */
public class GenericTest {

    static final long prefixMasks[];
    static final long timestampMasks[];

    static {
        long pf[] = new long[32];
        long ts[] = new long[32];
        int tsBits = UuidKeyGen.TIMESTAMP_BITS;
        int tsLowBit = 33;
        pf[0] = 0x8000000000000000L;
        ts[0] = (((1L << tsBits) - 1) << tsLowBit);
        for (int i = 1; i < 32; ++i) {
            pf[i] = pf[i - 1] | (1L << (63 - i));
            ts[i] = (ts[i - 1] >>> 1);
        }
        prefixMasks = pf;
        timestampMasks = ts;
    }

    @Test
    public void test1() {
        System.out.printf("** test1 begin\n");
        for (int i = 0; i < prefixMasks.length; ++i) {
            System.out.printf("%016x %016x\n", prefixMasks[i], 0xFFFFFFFFFFFFFFFFL & prefixMasks[i]);
        }
        System.out.println();
        for (int i = 0; i < timestampMasks.length; ++i) {
            System.out.printf("%016x %016x\n", timestampMasks[i], 0xFFFFFFFFFFFFFFFFL & timestampMasks[i]);
        }
        System.out.printf("** test1 end\n");
    }

    @Test
    public void test2() {
        System.out.printf("** test2 begin\n");
        long maxCode = (1L << UuidKeyGen.TIMESTAMP_BITS) - 1;
        long[] sampleCodes = new long[] {
            0L, 1L, maxCode, maxCode - 1, 1L << 29, (1L << 29) - 1
        };
        for (long code : sampleCodes) {
            for (int maskPos = 0; maskPos < timestampMasks.length; ++maskPos) {
                long date = code << (33 - maskPos);
                long verify = date & timestampMasks[maskPos];
                Assert.assertEquals(date, verify);
            }
        }
        System.out.printf("** test2 end\n");
    }

}
