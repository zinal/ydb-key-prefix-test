package tech.ydb.samples.keyprefix;

import java.time.Instant;
import java.time.LocalDate;
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
        var map = new HashMap<String, Long>();
        LocalDate now = LocalDate.now();
        // Generate and collect the number of values per prefix
        var startedAt = Instant.now();
        for (int i = 0; i < 10000000; ++i) {
            long pfx = gen.nextPrefix();
            for (int j = 0; j < 10; ++j) {
                String id = gen.nextValue(pfx, now);
                String x = id.substring(0, 2);
                Long count = map.get(x);
                map.put(x, (count == null) ? 1 : count + 1L);
            }
        }
        var genMillis = startedAt.until(Instant.now(), ChronoUnit.MILLIS);
        System.out.printf("Generated in %d milliseconds\n", genMillis);
        // Check that statistics are reasonable
        var stats = new Stats();
        map.values().stream().forEach(v -> stats.next(v));
        long avg = stats.getAverage();
        System.out.printf("Statistics: count=%d, total=%d, min=%d, max=%d, avg=%d\n",
                stats.count, stats.sum, stats.min, stats.max, avg);
        Assert.assertTrue("Reasonable min", stats.min >= 85L * avg / 100L);
        Assert.assertTrue("Reasonable max", stats.max <= 115L * avg / 100L);
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
