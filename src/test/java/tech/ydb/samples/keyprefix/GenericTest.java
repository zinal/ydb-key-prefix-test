package tech.ydb.samples.keyprefix;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author zinal
 */
public class GenericTest {

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

    @Test
    public void test1() {
        System.out.printf("** test1 begin\n");
        for (int i = 0; i < prefixMasks.length; ++i) {
            System.out.printf("%016x %016x\n", prefixMasks[i], 0xFFFFFFFFFFFFFFFFL & prefixMasks[i]);
        }
        System.out.println();
        for (int i = 0; i < dateMasks.length; ++i) {
            System.out.printf("%016x %016x\n", dateMasks[i], 0xFFFFFFFFFFFFFFFFL & dateMasks[i]);
        }
        System.out.printf("** test1 end\n");
    }

    @Test
    public void test2() {
        System.out.printf("** test2 begin\n");
        for (int day = 1; day < 367; ++day) {
            for (int year = 0; year < 40; ++year) {
                //System.out.printf("** day %d, year %d\n", day, year);
                int code = day - 1 + (366 * year);
                for (int maskPos = 0; maskPos < dateMasks.length; ++maskPos) {
                    long date = code;
                    date = date << (49 - maskPos);
                    long verify = date & dateMasks[maskPos];
                    //System.out.printf("%016x %016x %016x (%d, %016x)\n", (long) code, date, verify, maskPos, dateMasks[maskPos]);
                    Assert.assertEquals(date, verify);
                }
            }
        }
        System.out.printf("** test2 end\n");
    }

}
