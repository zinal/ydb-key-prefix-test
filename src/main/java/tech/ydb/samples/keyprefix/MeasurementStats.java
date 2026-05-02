package tech.ydb.samples.keyprefix;

import org.HdrHistogram.Histogram;

/**
 * Aggregates latency and retry statistics for transactional retry invocations
 * (success and failure). Durations are recorded in an {@link Histogram} so
 * percentiles are derived without storing every sample.
 */
final class MeasurementStats {

    /**
     * Upper bound for a single observed duration (10 minutes in ns); values are
     * clamped when recording.
     */
    private static final long MAX_TRACKABLE_DURATION_NS = 10L * 60L * 1_000_000_000L;

    private static final int SIGNIFICANT_VALUE_DIGITS = 3;

    private final Object lock = new Object();

    private long count;
    private long sumNanos;
    private long minNanos = Long.MAX_VALUE;
    private long maxNanos = Long.MIN_VALUE;
    private long sumRetries;
    private long invocationsWithRetries;
    private long errors;
    private Histogram durationHistogram;

    MeasurementStats() {
        durationHistogram = newEmptyHistogram();
    }

    private static Histogram newEmptyHistogram() {
        return new Histogram(MAX_TRACKABLE_DURATION_NS, SIGNIFICANT_VALUE_DIGITS);
    }

    void reset() {
        synchronized (lock) {
            count = 0;
            sumNanos = 0;
            minNanos = Long.MAX_VALUE;
            maxNanos = Long.MIN_VALUE;
            sumRetries = 0;
            invocationsWithRetries = 0;
            errors = 0;
            durationHistogram = newEmptyHistogram();
        }
    }

    /**
     * @param retryCount retries attributed to this invocation (success case:
     * value returned by {@code runWithRetry})
     */
    void record(long durationNanos, int retryCount, boolean success) {
        synchronized (lock) {
            count++;
            sumNanos += durationNanos;
            minNanos = Math.min(minNanos, durationNanos);
            maxNanos = Math.max(maxNanos, durationNanos);
            sumRetries += retryCount;
            if (!success) {
                errors++;
            }
            if (retryCount > 0) {
                invocationsWithRetries++;
            }
            long v = durationNanos;
            if (v < 0L) {
                v = 0L;
            } else if (v > MAX_TRACKABLE_DURATION_NS) {
                v = MAX_TRACKABLE_DURATION_NS;
            }
            durationHistogram.recordValue(v);
        }
    }

    void print() {
        synchronized (lock) {
            if (count == 0) {
                System.out.println("MeasurementStats: no samples.");
                return;
            }
            double avgNanos = ((double) sumNanos) / count;
            double avgRetries = ((double) sumRetries) / count;

            long p50 = (long) durationHistogram.getValueAtPercentile(50.0);
            long p90 = (long) durationHistogram.getValueAtPercentile(90.0);
            long p99 = (long) durationHistogram.getValueAtPercentile(99.0);

            System.out.println("MeasurementStats:");
            System.out.println("  samples:               " + count);
            System.out.println("  errors:                " + errors);
            System.out.println("  time total (ms):       " + formatMillis(sumNanos));
            System.out.println("  time min (ms):         " + formatMillis(minNanos));
            System.out.println("  time max (ms):         " + formatMillis(maxNanos));
            System.out.println("  time avg (ms):         " + formatMillisDouble(avgNanos));
            System.out.println("  time p50 (ms):         " + formatMillis(p50));
            System.out.println("  time p90 (ms):         " + formatMillis(p90));
            System.out.println("  time p99 (ms):         " + formatMillis(p99));
            System.out.println("  avg retries:           " + String.format("%.4f", avgRetries));
            System.out.println("  invocations w/ retries:" + invocationsWithRetries);
        }
    }

    private static String formatMillis(long nanos) {
        return String.format("%.6f", nanos / 1_000_000.0);
    }

    private static String formatMillisDouble(double nanos) {
        return String.format("%.6f", nanos / 1_000_000.0);
    }
}
