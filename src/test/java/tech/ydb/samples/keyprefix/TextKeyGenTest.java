package tech.ydb.samples.keyprefix;

import org.junit.Test;

/**
 *
 * @author zinal
 */
public class TextKeyGenTest {

    @Test
    public void testTextKeyGen() {
        var gen = new TextKeyGen();
        for (int i = 0; i < 10; ++i) {
            long pfx = gen.nextPrefix();
            for (int j = 0; j < 10; ++j) {
                System.out.printf("%d %d %s\n", i, j, gen.nextValue(pfx));
            }
        }
    }

}
