package tech.ydb.samples.keyprefix;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import org.junit.Test;

/**
 *
 * @author zinal
 */
public class ConvertToYdbTest {

    @Test
    public void convertToYdbTest() {
        var input = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        long v = UuidKeyGen.reorderForYdb(ByteBuffer.wrap(input).getLong());
        var bb = ByteBuffer.allocate(8);
        bb.putLong(v);
        var output = bb.array();
        System.out.println(HexFormat.of().formatHex(output));
    }

}
