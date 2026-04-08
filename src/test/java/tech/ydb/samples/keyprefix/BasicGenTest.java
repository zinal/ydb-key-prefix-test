package tech.ydb.samples.keyprefix;

import java.time.Instant;
import java.time.LocalDate;
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
        System.out.println("Current values:");
        print(gen.nextValue());
        print(gen.nextValue());
        print(gen.nextValue());
        print(gen.nextValue());
        System.out.println("Past values:");
        print(gen.nextValue(Instant.parse("2007-12-03T10:15:30.00Z")));
        print(gen.nextValue(LocalDate.ofYearDay(1999, 157)));
        System.out.println("Future values:");
        print(gen.nextValue(Instant.parse("2057-12-03T10:15:30.00Z")));
        print(gen.nextValue(LocalDate.ofYearDay(2083, 33)));
    }

    private void print(UUID uuid) {
        System.out.println(uuid.toString() + " " + TextKeyGen.convert(uuid));
    }

}
