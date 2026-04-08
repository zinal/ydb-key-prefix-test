package tech.ydb.samples.keyprefix;

import java.util.UUID;
import org.junit.Test;

/**
 *
 * @author mzinal
 */
public class BasicGenTest {

    @Test
    public void test1() {
        var gen = new UuidKeyGen();
        print(gen.nextValue());
        print(gen.nextValue());
        print(gen.nextValue());
        print(gen.nextValue());
    }

    private void print(UUID uuid) {
        System.out.println(uuid.toString() + " " + TextKeyGen.convert(uuid));
    }

}
