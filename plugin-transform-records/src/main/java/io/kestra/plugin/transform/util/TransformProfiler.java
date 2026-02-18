package io.kestra.plugin.transform.util;

public final class TransformProfiler {
    private static final boolean ENABLED = "true".equalsIgnoreCase(System.getProperty("bench.profile"));
    private static final ThreadLocal<Counters> COUNTERS = ThreadLocal.withInitial(Counters::new);

    private TransformProfiler() {
    }

    public static boolean isEnabled() {
        return ENABLED;
    }

    public static void reset() {
        if (!ENABLED) {
            return;
        }
        COUNTERS.get().reset();
    }

    public static void addTransformNs(long nanos) {
        if (!ENABLED) {
            return;
        }
        COUNTERS.get().transformNs += nanos;
    }

    public static void addWriteNs(long nanos) {
        if (!ENABLED) {
            return;
        }
        COUNTERS.get().writeNs += nanos;
    }

    public static Snapshot snapshot() {
        if (!ENABLED) {
            return new Snapshot(0L, 0L);
        }
        Counters counters = COUNTERS.get();
        return new Snapshot(counters.transformNs, counters.writeNs);
    }

    public record Snapshot(long transformNs, long writeNs) {
    }

    private static final class Counters {
        private long transformNs;
        private long writeNs;

        private void reset() {
            transformNs = 0L;
            writeNs = 0L;
        }
    }
}
