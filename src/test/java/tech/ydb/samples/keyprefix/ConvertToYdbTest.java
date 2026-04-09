package tech.ydb.samples.keyprefix;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author zinal
 */
public class ConvertToYdbTest {

    @Test
    public void convertToYdbTest() {
        var input = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        long v0 = ByteBuffer.wrap(input).getLong();
        long v1 = UuidKeyGen.reorderForYdb(v0);
        long v2 = UuidKeyGen.reorderForYdb(v1);
        Assert.assertEquals(v0, v2);
        Assert.assertNotEquals(v0, v1);
        var bb = ByteBuffer.allocate(8);
        bb.putLong(v1);
        var output = bb.array();
        System.out.println(HexFormat.of().formatHex(output));

    }

}
