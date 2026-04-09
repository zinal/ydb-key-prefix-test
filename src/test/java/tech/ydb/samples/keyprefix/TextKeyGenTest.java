package tech.ydb.samples.keyprefix;

import java.time.Instant;
import java.time.LocalDate;
import org.junit.Test;

/**
 *
 * @author mzinal
 */
public class TextKeyGenTest {

    @Test
    public void test1() {
        TextKeyGen gen = new TextKeyGen();
        System.out.println("Current values:");
        print(gen.nextValue());
        print(gen.nextValue());
        print(gen.nextValue());
        print(gen.nextValue());
        System.out.println("Prefix values:");
        long pfx = gen.nextPrefix();
        print(gen.nextValue(pfx));
        print(gen.nextValue(pfx));
        print(gen.nextValue(pfx));
        print(gen.nextValue(pfx));
        System.out.println("Past values:");
        print(gen.nextValue(Instant.parse("2007-12-03T10:15:30.00Z")));
        print(gen.nextValue(Instant.parse("2007-12-03T10:15:40.00Z")));
        print(gen.nextValue(LocalDate.ofYearDay(1999, 157)));
        print(gen.nextValue(LocalDate.ofYearDay(1999, 158)));
        System.out.println("Future values:");
        print(gen.nextValue(Instant.parse("2057-12-03T10:15:30.00Z")));
        print(gen.nextValue(LocalDate.ofYearDay(2083, 33)));
    }

    private void print(String id) {
        System.out.println(id);
    }

}
