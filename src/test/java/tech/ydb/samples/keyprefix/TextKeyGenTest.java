package tech.ydb.samples.keyprefix;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author zinal
 */
public class TextKeyGenTest {

    @Test
    public void testTextKeyGen() {
        var gen = new TextKeyGen();
        var map = new HashMap<String, Entry>();
        LocalDate dates[] = new LocalDate[10];
        dates[0] = LocalDate.of(2023, Month.JANUARY, 1);
        dates[1] = LocalDate.of(2023, Month.APRIL, 1);
        dates[2] = LocalDate.of(2023, Month.JULY, 1);
        dates[3] = LocalDate.of(2023, Month.OCTOBER, 1);
        dates[4] = LocalDate.of(2024, Month.JANUARY, 10);
        dates[5] = LocalDate.of(2024, Month.APRIL, 10);
        dates[6] = LocalDate.of(2024, Month.JULY, 10);
        dates[7] = LocalDate.of(2024, Month.OCTOBER, 10);
        dates[8] = LocalDate.of(2025, Month.JANUARY, 20);
        dates[9] = LocalDate.of(2025, Month.APRIL, 20);
        // Generate and collect the number of values per prefix
        var startedAt = Instant.now();
        for (var now : dates) {
            for (int i = 0; i < 100000; ++i) {
                long pfx = gen.nextPrefix();
                for (int j = 0; j < 100; ++j) {
                    String id = gen.nextValue(pfx, now);
                    String part1 = id.substring(0, 2);
                    // String part2 = id.substring(2, 4);
                    Entry e = map.get(part1);
                    if (e == null) {
                        e = new Entry();
                        map.put(part1, e);
                    }
                    e.count += 1;
                    /*
                    var sub = e.sub.get(part2);
                    e.sub.put(part2, (sub == null) ? 1L : sub + 1L);
                     */
                }
            }
        }
        var genMillis = startedAt.until(Instant.now(), ChronoUnit.MILLIS);
        System.out.printf("Generated in %d milliseconds\n", genMillis);
        // Check that statistics are reasonable
        var stats = new Stats();
        map.values().stream().forEach(v -> stats.next(v.count));
        long avg = stats.getAverage();
        System.out.printf("Statistics: count=%d, total=%d, min=%d, max=%d, avg=%d\n",
                stats.count, stats.sum, stats.min, stats.max, avg);
        Assert.assertTrue("Reasonable min", stats.min >= 85L * avg / 100L);
        Assert.assertTrue("Reasonable max", stats.max <= 115L * avg / 100L);
    }

    static class Entry {

        long count = 0L;
        // final HashMap<String, Long> sub = new HashMap<>();
    }

    static class Stats {

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long sum = 0L;
        int count = 0;

        long getAverage() {
            return sum / count;
        }

        void next(long v) {
            if (min > v) {
                min = v;
            }
            if (max < v) {
                max = v;
            }
            sum += v;
            count += 1;
        }
    }

}
